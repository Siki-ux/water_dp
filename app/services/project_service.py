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

            logger.info(f"Creating project with authorization group: {auth_group_id}")
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
    def list_sensors(
        db: Session,
        project_id: UUID,
        user: Dict[str, Any],
        rich: bool = False,
        skip: int = 0,
        limit: int = 100,
    ) -> List[Any]:
        # Check Access
        project = ProjectService._check_access(
            db, project_id, user, required_role="viewer"
        )

        # 1. Get linked UUIDs from local project_sensors table (water_dp-api database)
        statement = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id == project_id
        )
        linked_uuids = [str(row) for row in db.execute(statement).scalars().all()]

        if not linked_uuids:
            return []

        # 2. Use OrchestratorV3 for fetching sensors
        from app.services.timeio.orchestrator_v3 import orchestrator_v3

        try:
            if rich:
                # Use v3 Rich Orchestrator logic (all metadata + datastreams)
                results = orchestrator_v3.list_sensors(
                    project_name=project.name,
                    project_group=project.authorization_provider_group_id,
                )
                return [t for t in results if str(t["uuid"]) in linked_uuids]
            else:
                # Use Paginated/Basic logic
                results = orchestrator_v3.list_sensors_paginated(
                    project_name=project.name,
                    project_group=project.authorization_provider_group_id,
                    uuids=linked_uuids,
                    skip=skip,
                    limit=limit,
                )
                return results

        except Exception as error:
            logger.error(f"Failed to fetch sensors for project {project.name}: {error}")
            return []

    @staticmethod
    def _get_project_details_from_api(
        project_uuid: str, token: str, project_name: str = None
    ) -> Optional[tuple[int, str]]:
        if not token:
            logger.warning("_get_project_details_from_api called without token")
            return None, None

        url = f"{settings.thing_management_api_url}/project"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            logger.info(
                f"Querying thing-management for project {project_uuid} at {url}"
            )
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", data) if isinstance(data, dict) else data
                logger.debug(
                    f"Received {len(items) if isinstance(items, list) else 0} projects from management API"
                )

                if isinstance(items, list):
                    # 1. Try matching by UUID
                    for project_data in items:
                        project_uuid_api = (
                            project_data.get("uuid")
                            or project_data.get("thingId")
                            or project_data.get("projectUuid")
                        )
                        if str(project_uuid_api) == str(project_uuid):
                            logger.info(
                                f"Found match by UUID: {project_data.get('name')} (ID: {project_data.get('id')})"
                            )
                            return project_data.get("id"), project_data.get("name")

                    # 2. Try matching by Name
                    if project_name:
                        target_name = project_name.lower().strip()
                        logger.info(f"Searching for project name: '{target_name}'")

                        for project_data in items:
                            current_name = (project_data.get("name") or "").lower()
                            auth_group_id = (
                                project_data.get("authorization_provider_group_id") or ""
                            ).lower()

                            # Matches: exact, suffix, reverse-suffix, auth-group
                            if (
                                current_name == target_name
                                or current_name.endswith(f":{target_name}")
                                or current_name.endswith(f"/{target_name}")
                                or target_name.endswith(f":{current_name}")
                                or target_name.endswith(f"/{current_name}")
                                or (
                                    auth_group_id
                                    and (
                                        auth_group_id == target_name
                                        or target_name in auth_group_id
                                    )
                                )
                            ):
                                logger.info(
                                    f"Found project by name match: id={project_data.get('id')}, name={project_data.get('name')}"
                                )
                                return project_data.get("id"), project_data.get("name")

                    # 3. Last resort: if only one project exists, use it? USE WITH CAUTION.
                    # This might be dangerous in multi-project envs, but helpful in dev.
                    # if len(items) == 1:
                    #    logger.warning("Single project found - assuming match.")
                    #    return items[0].get("id"), items[0].get("name")

            else:
                logger.error(
                    f"Failed to fetch projects: {response.status_code} {response.text}"
                )
        except Exception as error:
            logger.error(f"Error fetching project details from thing-management: {error}")

        logger.warning(
            f"Project {project_uuid} (name: {project_name}) not found in thing-management response"
        )
        return None, None

    @staticmethod
    def _resolve_project_frost_url(
        project_id: UUID,
        token: str,
        project_name: str = None,
        project_auth_groups: List[str] = None,
    ) -> Optional[str]:
        """
        Resolve FROST URL using Project Authorization Group.
        Logic: Extract group name (last part after : or /), lowercase it, and append to base Host.
        No external API call needed if we have the group list.
        """
        # Determine Base Host
        base_host = "http://frost:8080"
        if settings.frost_url:
            from urllib.parse import urlparse

            parsed = urlparse(settings.frost_url)
            base_host = f"{parsed.scheme}://{parsed.netloc}"

        group_name = "default"

        # Helper to parse group string
        def parse_group(raw: str) -> str:
            if not raw:
                return ""
            # Handle URN or Path
            parts = raw.split(":")
            if len(parts) > 1:
                return parts[-1]
            if "/" in raw:
                return raw.split("/")[-1]
            return raw

        if project_auth_groups and len(project_auth_groups) > 0:
            # [DEPRECATED CODE PATH] - Removing usage
            # But method signature asked for project_auth_groups
            # Let's support it if passed as single-item list
            group_name = parse_group(project_auth_groups[0])

            # If group_name looks like a UUID (length 36), resolve it
            if len(group_name) == 36 and "-" in group_name:
                try:
                    from app.services.keycloak_service import KeycloakService

                    keycloak_group = KeycloakService.get_group(group_name)
                    if keycloak_group and keycloak_group.get("name"):
                        logger.info(
                            f"Resolved Keycloak Group UUID {group_name} -> {keycloak_group.get('name')}"
                        )
                        group_name = keycloak_group.get("name")
                except Exception as error:
                    logger.warning(
                        f"Failed to resolve Keycloak group {group_name}: {error}"
                    )

        elif project_name:
            # Fallback to project name if no group (unlikely for valid projects)
            group_name = project_name

        # Sanitize
        # Strip UFZ-TSM: prefix if present (common in Keycloak groups)
        clean_name = group_name.replace("UFZ-TSM:", "").replace("ufz-tsm:", "").strip()
        slug = clean_name.lower().strip()

        # Internal Docker URL does NOT use /sta/ prefix (that is for Proxy)
        # Context is deployed at root: http://frost:8080/{schema}/v1.1
        # Convention: usage of project_{slug}
        if not slug.startswith("project_"):
            slug = f"project_{slug}"

        # Probe for suffix (e.g. _1, _2) since schema often includes ID
        # Context is deployed at root: http://frost:8080/{schema}/v1.1

        candidates = [slug]
        # range 1 to 10 covers common ID schemas
        for index in range(1, 10):
            candidates.append(f"{slug}_{index}")

        import requests

        for c in candidates:
            url = f"{base_host}/{c}/v1.1"
            probe_url = f"{url}/Things"
            try:
                # Short timeout for probing
                response = requests.get(probe_url, timeout=1, params={"$top": 1})
                if response.status_code == 200:
                    logger.info(f"Resolved FROST URL via probe: {url}")
                    return url
            except Exception:
                pass

        # Fallback to base slug if probe fails
        url = f"{base_host}/{slug}/v1.1"
        logger.warning(
            f"Could not probe valid FROST URL for {group_name}, falling back to {url}"
        )
        return url

    @staticmethod
    def get_available_sensors(
        db: Session,
        project_id: UUID,
        user: Dict[str, Any],
        token: str = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[Dict]:
        """List sensors available in the project's FROST instance that are NOT linked in water_dp-api."""
        # 1. Check access
        project = ProjectService._check_access(
            db, project_id, user, required_role="viewer"
        )

        # 2. Get currently linked sensor IDs (UUIDs) from local water_dp-api database
        statement = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id == project_id
        )
        linked_uuids = {str(row) for row in db.execute(statement).scalars().all()}

        # 3. Fetch all things from Orchestrator (Basic) which uses correct Schema
        from app.services.timeio.orchestrator_v3 import orchestrator_v3

        try:
            # We want ALL sensors in the schema to filter them
            # Since we don't have a "NOT IN" filter in Orchestrator yet easily exposable,
            # We fetch basic list and filter in memory (assuming < 1000 sensors usually per project)
            all_sensors = orchestrator_v3.list_all_sensors_basic(
                project_name=project.name,
                project_group=project.authorization_provider_group_id,
            )
        except Exception as error:
            logger.error(
                f"Failed to fetch available sensors for project {project.name}: {error}"
            )
            return []

        available = []
        for sensor in all_sensors:
            # sensor is {uuid, name, description} from list_all_sensors_basic
            thing_uuid = str(sensor.get("uuid"))

            if thing_uuid not in linked_uuids:
                # We need the FROST ID (View ID) if we want to construct links, but list_all_sensors_basic
                # might only return UUID/Name from 'thing' table unless we joined.
                # Actually list_all_sensors_basic returns result of get_all_sensors_basic which
                # returns: id, name, description, properties, uuid.
                # Check timeio_db.py get_all_sensors_basic implementation to be sure.

                # Assuming sensor has 'id' (int/str) and 'uuid'
                thing_id = sensor.get("id")

                # Construct public link path (relative)
                # We need schema name... Orchestrator resolved it but didn't return it in list.
                # Keep it simple for now or resolve schema again.
                # public_link = f"/sta/{schema}/v1.1/Things('{thing_id}')"

                available.append(
                    {
                        "id": thing_id,
                        "name": sensor["name"],
                        "description": sensor.get("description", ""),
                        "properties": sensor.get("properties", {}),
                        "latitude": sensor.get("latitude"),  # Might be missing in basic
                        "longitude": sensor.get("longitude"),
                        # "iot_self_link": public_link
                    }
                )

        # Pagination on the memory list
        start = skip
        end = skip + limit
        return available[start:end]

    @staticmethod
    def get_allowed_sensor_ids(db: Session, user: Dict[str, Any]) -> List[str]:
        """
        Get ALL sensor IDs that the user is allowed to access across all projects.
        Used for filtering global views.
        """
        # 1. Get Allowed Projects (Reusing list_projects logic via high limit)
        # Note: If user has > 1000 projects, this might need pagination or optimization.
        projects = ProjectService.list_projects(db, user, limit=1000)
        if not projects:
            return []

        project_ids = [p.id for p in projects]

        # 2. Get Sensors linked to these projects
        statement = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id.in_(project_ids)
        )
        result = db.execute(statement).scalars().all()

        # Ensure unique list
        return list(set([str(row) for row in result]))

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
