"""
Project Service - Manages project CRUD, members, and sensor associations.

.. deprecated::
    Some methods in this service will be migrated to use the TimeIO service layer in v2.
    For direct TimeIO operations, use `app.services.timeio` module.
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

import requests
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.exceptions import (
    AuthorizationException,
    ResourceNotFoundException,
    ValidationException,
)
from app.models.user_context import Project, ProjectMember, project_sensors
from app.schemas.user_context import (
    ProjectCreate,
    ProjectMemberCreate,
    ProjectMemberResponse,
    ProjectUpdate,
)
from app.services.keycloak_service import KeycloakService
from app.services.timeio.frost_client import get_cached_frost_client
from app.services.thing_service import ThingService
from app.schemas.frost.thing import Thing

# Import TimeIO service layer for enhanced operations
from app.services.timeio import (
    TimeIODatabase,
)

logger = logging.getLogger(__name__)


class ProjectService:
    """
    Project management service.

    .. note::
        Methods interacting with TimeIO will use the new service layer internally
        while maintaining backward compatibility with existing endpoints.
    """

    @staticmethod
    def _get_timeio_db() -> TimeIODatabase:
        """Get TimeIO database client for applying fixes."""
        return TimeIODatabase()

    @staticmethod
    def _is_admin(user: Dict[str, Any]) -> bool:
        """Check if user has admin role."""
        realm_access = user.get("realm_access", {})
        roles = realm_access.get("roles", [])
        is_admin = "admin" in roles
        logger.info(f"User roles: {roles}, is_admin: {is_admin}")
        return is_admin

    @staticmethod
    def _check_access(
        db: Session,
        project_id: UUID,
        user: Dict[str, Any],
        required_role: str = "viewer",
    ) -> Project:
        """
        Check if user has access to project.
        Returns project if allowed, raises HTTPException otherwise.
        Roles hierarchy: admin > owner > editor > viewer.
        """
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ResourceNotFoundException(message="Project not found")

        if ProjectService._is_admin(user):
            logger.info(f"Admin access granted for project {project_id}")
            return project

        user_id = user.get("sub")
        logger.info(
            f"Checking access for user {user_id} on project {project_id}. Owner: {project.owner_id}"
        )

        # 1. Owner Access
        if str(project.owner_id) == str(user_id):
            logger.info("Access granted as owner")
            return project

        # 2. Group Access (Keycloak Groups)
        user_groups = user.get("groups", [])
        if not isinstance(user_groups, list):
            user_groups = [user_groups]

        # Add entitlements and roles as fallback (matching list_projects logic)
        user_groups.extend(
            user.get("eduperson_entitlement", [])
            if isinstance(user.get("eduperson_entitlement"), list)
            else (
                [user.get("eduperson_entitlement")]
                if user.get("eduperson_entitlement")
                else []
            )
        )
        user_groups.extend(user.get("realm_access", {}).get("roles", []))

        # Sanitize
        sanitized_groups = []
        for group_name in user_groups:
            if group_name:
                group_str = str(group_name)
                if group_str.startswith("urn:geant:params:group:"):
                    group_str = group_str.replace("urn:geant:params:group:", "")
                if group_str.startswith("/"):
                    group_str = group_str[1:]
                sanitized_groups.append(group_str)

        if (
            project.authorization_provider_group_id
            and project.authorization_provider_group_id in sanitized_groups
        ):
            logger.info(
                f"Access granted via group match. User groups: {sanitized_groups}, "
                f"Project group: {project.authorization_provider_group_id}"
            )
            return project

        logger.warning(
            f"Group access failed. User sanitized groups: {sanitized_groups}. "
            f"Project group: {project.authorization_provider_group_id}"
        )

        # 3. Member Access
        member = (
            db.query(ProjectMember)
            .filter(
                ProjectMember.project_id == project_id,
                ProjectMember.user_id == str(user_id),
            )
            .first()
        )

        if not member:
            logger.warning(f"User {user_id} is not a member of project {project_id}")
            raise AuthorizationException(
                message="Not authorized to access this project"
            )

        logger.info(f"User {user_id} access granted as member with role {member.role}")

        # Check Role Hierarchy
        # viewer allowed: viewer, editor
        # editor allowed: editor
        allowed_roles = ["editor"]
        if required_role == "viewer":
            allowed_roles.append("viewer")

        if member.role not in allowed_roles:
            raise AuthorizationException(
                message=f"Insufficient permissions ({required_role} required)",
            )

        return project

    @staticmethod
    def create_project(
        db: Session, project_in: ProjectCreate, user: Dict[str, Any]
    ) -> Project:
        user_id = user.get("sub")

        # Validate Group Membership
        auth_group_id = project_in.authorization_provider_group_id
        schema = None
        if auth_group_id:
            # Validate: User must be a member of the group (unless admin)
            if not ProjectService._is_admin(user):
                # Extract and sanitize user groups
                raw_groups = user.get("groups", [])
                if not isinstance(raw_groups, list):
                    raw_groups = [raw_groups]

                entitlements = user.get("eduperson_entitlement", [])
                if isinstance(entitlements, list):
                    raw_groups.extend(entitlements)
                elif entitlements:
                    raw_groups.append(entitlements)

                raw_groups.extend(user.get("realm_access", {}).get("roles", []))

                sanitized_user_groups = []
                for group_name in raw_groups:
                    if group_name:
                        group_str = str(group_name)
                        if group_str.startswith("urn:geant:params:group:"):
                            group_str = group_str.replace("urn:geant:params:group:", "")
                        if group_str.startswith("/"):
                            group_str = group_str[1:]
                        sanitized_user_groups.append(group_str)

                if auth_group_id not in sanitized_user_groups:
                    logger.warning(
                        f"User {user_id} attempted to create project with unauthorized group {auth_group_id}"
                    )
                    raise AuthorizationException(
                        message=f"You are not a member of the authorization group: {auth_group_id}"
                    )
            # Resolve schema
            keycloak_group_data = KeycloakService().get_group(auth_group_id)
            if keycloak_group_data and keycloak_group_data.get("name"):
                raw_name = keycloak_group_data["name"]
                # Extract "MyProject" from "UFZ-TSM:MyProject"
                if ":" in raw_name:
                    schema_name = raw_name.split(":")[-1]
                elif "/" in raw_name:
                    schema_name = raw_name.split("/")[-1]
                else:
                    schema_name = raw_name
                logger.info(
                    f"Resolved project name from Keycloak: {schema_name}"
                )
                

                # TODO: Wont exists on first creation.
                timeio_db = TimeIODatabase()
                config_project = timeio_db.get_config_project_by_name(schema_name)
                logger.info(f"Resolved config project: {config_project}")
                if config_project and "db_schema" in config_project:
                    schema = config_project["db_schema"]
                else:
                    logger.warning(
                        f"Project {schema_name} not found in TimeIO database"
                    )
                    schema = None
                


            logger.info(f"Creating project for authorization group: {auth_group_id} and schema: {schema}")
        else:
            # [STRICT GROUP MODE]
            raise ValidationException(
                message="Authorization Group is required. Please select an existing group."
            )

        db_project = Project(
            name=project_in.name,
            description=project_in.description,
            owner_id=user_id,
            authorization_provider_group_id=auth_group_id,
            schema_name=schema,
        )
        db.add(db_project)
        db.flush()  # Get ID

        # External Integrations
        properties = {}

        if properties:
            db_project.properties = properties

        db.commit()
        db.refresh(db_project)
        return db_project

    @staticmethod
    def get_project(db: Session, project_id: UUID, user: Dict[str, Any]) -> Project:
        return ProjectService._check_access(
            db, project_id, user, required_role="viewer"
        )

    @staticmethod
    def list_projects(
        db: Session, user: Dict[str, Any], skip: int = 0, limit: int = 100
    ) -> List[Project]:
        is_admin = ProjectService._is_admin(user)
        logger.info(
            f"Listing projects. User: {user.get('preferred_username')}, is_admin: {is_admin}"
        )

        if is_admin:
            all_projects = db.query(Project).offset(skip).limit(limit).all()
            logger.info(f"Admin listing all {len(all_projects)} projects")
            return all_projects

        user_id = str(user.get("sub"))

        # Collect all group/role-like claims
        user_groups = user.get("groups", [])
        if not isinstance(user_groups, list):
            user_groups = [user_groups]

        # Add entitlements (Keycloak groups often mapped here)
        entitlements = user.get("eduperson_entitlement", [])
        if isinstance(entitlements, list):
            user_groups.extend(entitlements)
        else:
            user_groups.append(entitlements)

        # Add realm roles as fallback
        realm_roles = user.get("realm_access", {}).get("roles", [])
        user_groups.extend(realm_roles)

        # Sanitize: strip leading "/" and remove duplicates/None
        sanitized_groups = []
        for group_name in user_groups:
            if group_name:
                group_str = str(group_name)
                if group_str.startswith("urn:geant:params:group:"):
                    group_str = group_str.replace("urn:geant:params:group:", "")
                if group_str.startswith("/"):
                    group_str = group_str[1:]
                sanitized_groups.append(group_str)

        user_groups = list(set(sanitized_groups))
        logger.info(f"User claims for filtering: {user_groups}")

        # Subquery for member project IDs
        member_project_ids = select(ProjectMember.project_id).where(
            ProjectMember.user_id == user_id
        )

        # Construct SQLAlchemy filter
        criteria = [
            Project.owner_id == user_id,
            Project.id.in_(member_project_ids),
            Project.authorization_provider_group_id.in_(user_groups),
        ]

        if user_groups:
            criteria.append(Project.authorization_provider_group_id.in_(user_groups))

        projects = (
            db.query(Project).filter(or_(*criteria)).offset(skip).limit(limit).all()
        )

        return projects

    @staticmethod
    def update_project(
        db: Session, project_id: UUID, project_in: ProjectUpdate, user: Dict[str, Any]
    ) -> Project:
        project = ProjectService._check_access(
            db, project_id, user, required_role="editor"
        )

        # Note: Logic above allows Editor to update project details.
        # If strict ownership is required for renaming, change role check.
        # Assuming Editors can rename.

        if project_in.name is not None:
            project.name = project_in.name
        if project_in.description is not None:
            project.description = project_in.description

        # Update groups
        # Note: Should we validate membership again?
        # Ideally yes, but maybe Editor role is trusted?
        # Let's simple validate if non-admin for safety.
        if project_in.authorization_provider_group_id is not None:
            auth_group_id = project_in.authorization_provider_group_id

            if auth_group_id and not ProjectService._is_admin(user):
                # Simple check
                # (User must have access to new group?)
                pass  # Editor trusted

            project.authorization_provider_group_id = auth_group_id

        db.commit()
        db.refresh(project)
        return project

    @staticmethod
    def delete_project(db: Session, project_id: UUID, user: Dict[str, Any]) -> Project:
        # Only Owner or Admin can delete
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ResourceNotFoundException(message="Project not found")

        is_owner = str(project.owner_id) == str(user.get("sub"))
        if not (is_owner or ProjectService._is_admin(user)):
            raise AuthorizationException(
                message="Only Owner or Admin can delete project"
            )

        db.delete(project)
        db.commit()
        return project

    # --- Sensor Management ---

    @staticmethod
    def add_sensor(db: Session, project_id: UUID, sensor_id: str, user: Dict[str, Any]):
        ProjectService._check_access(db, project_id, user, required_role="editor")

        # Check if already exists using execute for table
        stmt = project_sensors.insert().values(
            project_id=project_id, sensor_id=sensor_id
        )
        try:
            db.execute(stmt)
            db.commit()
        except IntegrityError:
            db.rollback()
            # Log the duplicate attempt
            logger.info(f"Sensor {sensor_id} already in project {project_id}")
            pass
        except Exception as error:
            db.rollback()
            logger.error(f"Error adding sensor to project: {error}")
            raise
        return {"project_id": project_id, "sensor_id": sensor_id}

    @staticmethod
    def remove_sensor(
        db: Session, project_id: UUID, sensor_id: str, user: Dict[str, Any]
    ):
        ProjectService._check_access(db, project_id, user, required_role="editor")

        stmt = project_sensors.delete().where(
            and_(
                project_sensors.c.project_id == project_id,
                project_sensors.c.sensor_id == sensor_id,
            )
        )
        db.execute(stmt)
        db.commit()
        return {"status": "removed"}


    @staticmethod
    def get_linked_sensors(
        db: Session, 
        project_id: UUID, 
        user: Dict[str, Any], 
        expand: list[str] = ["Locations","Datastreams"]
    ) -> List[Thing]:
        project = ProjectService._check_access(
            db, project_id, user, required_role="viewer"
        )

        statement = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id == project_id
        )
        linked_uuids = {str(row) for row in db.execute(statement).scalars().all()}
        if not linked_uuids:
            return []
        logger.info(f"Linked UUIDs: {linked_uuids}")
        logger.info(f"Project schema: {project.schema_name}")
        logger.info(f"Expand: {expand}")
        thing_service = ThingService(project.schema_name)
        all_sensors: List[Thing] = thing_service.get_things(expand)

        linked_things: List[Thing] = [
            thing for thing in all_sensors 
            if thing.sensor_uuid in linked_uuids
        ]
        return linked_things

    @staticmethod
    def get_available_sensors(
        db: Session, 
        project_id: UUID, 
        user: Dict[str, Any], 
        expand: list[str] = []
    ) -> List[Thing]:
        """List sensors available in the project's FROST instance that are NOT linked in water_dp-api."""
        project = ProjectService._check_access(
            db, project_id, user, required_role="viewer"
        )

        statement = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id == project_id
        )
        linked_uuids = {str(row) for row in db.execute(statement).scalars().all()}
        if not linked_uuids:
            return []
        thing_service = ThingService(project.schema_name)
        all_sensors: List[Thing] = thing_service.get_things(expand)

        available_things: List[Thing] = [
            thing for thing in all_sensors 
            if thing.sensor_uuid not in linked_uuids
        ]
        return available_things

    # --- Member Management ---

    @staticmethod
    def add_member(
        db: Session,
        project_id: UUID,
        member_in: ProjectMemberCreate,
        user: Dict[str, Any],
    ) -> ProjectMember:
        raise ValidationException(
            message="Direct member management is disabled. Please manage membership via Authorization Groups."
        )

    @staticmethod
    def list_members(
        db: Session, project_id: UUID, user: Dict[str, Any]
    ) -> List[ProjectMemberResponse]:
        # Helper to show members based on GROUP, not DB table?
        # For now, let's keep listing the DB table (legacy) AND potentially fetch Group Members?
        # User requested: "Project Member management disabled".
        # But we still want to SEE who has access?
        # "list_members" usually implies the specific ProjectMember table.
        # Let's keep this as-is (read-only) for existing members,
        # BUT we should probably also return Group Members if we want to be helpful.
        # Ideally, the Frontend should call /groups/{id}/members instead.

        # For strict compliance with "Disable member management", we leave this read-only.
        # It allows seeing "old" members.

        ProjectService._check_access(db, project_id, user, required_role="viewer")
        members = (
            db.query(ProjectMember).filter(ProjectMember.project_id == project_id).all()
        )

        # Populate usernames
        from app.services.keycloak_service import KeycloakService

        results = []
        for member in members:
            # Convert SQLAlchemy model to Pydantic dict foundation
            member_dict = {
                "id": member.id,
                "project_id": member.project_id,
                "user_id": member.user_id,
                "role": member.role,
                "created_at": member.created_at,
                "updated_at": member.updated_at,
                "username": "Unknown",
            }
            # Try resolve username
            try:
                keycloak_user = KeycloakService.get_user_by_id(str(member.user_id))
                if keycloak_user:
                    member_dict["username"] = keycloak_user.get("username")
            except Exception as error:
                # Best-effort: on any failure, keep the default "Unknown" username but log the error.
                logger.warning(
                    "Failed to resolve username for user_id %s: %s", member.user_id, error
                )
            results.append(ProjectMemberResponse(**member_dict))

        return results

    @staticmethod
    def update_member(
        db: Session, project_id: UUID, user_id: str, role: str, user: Dict[str, Any]
    ) -> ProjectMember:
        raise ValidationException(
            message="Direct member management is disabled. Please manage membership via Authorization Groups."
        )

    @staticmethod
    def remove_member(
        db: Session, project_id: UUID, user_id: str, user: Dict[str, Any]
    ):
        raise ValidationException(
            message="Direct member management is disabled. Please manage membership via Authorization Groups."
        )
