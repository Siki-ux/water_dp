import logging
from typing import Any, Dict, List
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.user_context import Project, ProjectMember, project_sensors
from app.schemas.user_context import (
    ProjectCreate,
    ProjectMemberCreate,
    ProjectMemberResponse,
    ProjectUpdate,
    SensorCreate,
)

logger = logging.getLogger(__name__)


class ProjectService:
    @staticmethod
    def _is_admin(user: Dict[str, Any]) -> bool:
        """Check if user has admin role."""
        realm_access = user.get("realm_access", {})
        roles = realm_access.get("roles", [])
        return (
            "admin" in roles or "admin-siki" in roles
        )  # Handle potential custom roles

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
            raise HTTPException(status_code=404, detail="Project not found")

        # 1. Admin Access
        if ProjectService._is_admin(user):
            return project

        user_id = user.get("sub")

        # 2. Owner Access
        if str(project.owner_id) == str(user_id):
            return project

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
            raise HTTPException(
                status_code=403, detail="Not authorized to access this project"
            )

        # Check Role Hierarchy
        # viewer allowed: viewer, editor
        # editor allowed: editor
        allowed_roles = ["editor"]
        if required_role == "viewer":
            allowed_roles.append("viewer")

        if member.role not in allowed_roles:
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient permissions ({required_role} required)",
            )

        return project

    @staticmethod
    def create_project(
        db: Session, project_in: ProjectCreate, user: Dict[str, Any]
    ) -> Project:
        user_id = user.get("sub")
        db_project = Project(
            name=project_in.name, description=project_in.description, owner_id=user_id
        )
        db.add(db_project)
        db.flush()  # Get ID

        # External Integrations
        props = {}

        # 1. Keycloak Group
        try:
            from app.services.keycloak_service import KeycloakService

            # Sanitize name for group?
            group_name = f"project-{project_in.name}"
            # Check if exists or randomness?
            # For now direct map
            group_id = KeycloakService.create_group(group_name)
            if group_id:
                props["keycloak_group_id"] = group_id
        except Exception as e:
            logger.error(f"Failed to create Keycloak group for project: {e}")

        # 2. TimeIO (FROST) Project Thing
        try:
            from app.services.time_series_service import TimeSeriesService

            ts_service = TimeSeriesService(db)
            thing_id = ts_service.create_project_thing(
                name=project_in.name,
                description=project_in.description or "",
                project_id=str(db_project.id),
            )
            if thing_id:
                props["timeio_thing_id"] = thing_id
        except Exception as e:
            logger.error(f"Failed to create TimeIO entity for project: {e}")

        if props:
            db_project.properties = props

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
        if ProjectService._is_admin(user):
            return db.query(Project).offset(skip).limit(limit).all()

        user_id = str(user.get("sub"))

        # Query projects where user is owner OR member
        # Using union or simple OR condition

        # Subquery for member project IDs
        member_project_ids = select(ProjectMember.project_id).where(
            ProjectMember.user_id == user_id
        )

        projects = (
            db.query(Project)
            .filter(
                or_(Project.owner_id == user_id, Project.id.in_(member_project_ids))
            )
            .offset(skip)
            .limit(limit)
            .all()
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

        db.commit()
        db.refresh(project)
        return project

    @staticmethod
    def delete_project(db: Session, project_id: UUID, user: Dict[str, Any]) -> Project:
        # Only Owner or Admin can delete
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        is_owner = str(project.owner_id) == str(user.get("sub"))
        if not (is_owner or ProjectService._is_admin(user)):
            raise HTTPException(
                status_code=403, detail="Only Owner or Admin can delete project"
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
        except Exception as e:
            db.rollback()
            logger.error(f"Error adding sensor to project: {e}")
            raise
        return {"project_id": project_id, "sensor_id": sensor_id}

    @staticmethod
    def create_and_link_sensor(
        db: Session, project_id: UUID, sensor_data: SensorCreate, user: Dict[str, Any]
    ):
        # Access check handled in add_sensor, but good to check early
        ProjectService._check_access(db, project_id, user, required_role="editor")

        from app.services.time_series_service import TimeSeriesService

        ts_service = TimeSeriesService(db)

        # Create Thing in FROST
        thing_id = ts_service.create_sensor_thing(sensor_data)

        if not thing_id:
            raise HTTPException(
                status_code=500, detail="Failed to create sensor in TimeIO"
            )

        # Link
        return ProjectService.add_sensor(db, project_id, thing_id, user)

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
    def list_sensors(db: Session, project_id: UUID, user: Dict[str, Any]) -> List[str]:
        ProjectService._check_access(db, project_id, user, required_role="viewer")

        stmt = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id == project_id
        )
        result = db.execute(stmt).scalars().all()
        return [str(r) for r in result]

    @staticmethod
    def get_available_sensors(
        db: Session, project_id: UUID, user: Dict[str, Any]
    ) -> List[Dict]:
        """List sensors available in FROST that are NOT linked to this project."""
        ProjectService._check_access(db, project_id, user, required_role="viewer")

        # 1. Get linked sensor IDs
        linked_ids = ProjectService.list_sensors(db, project_id, user)

        # 2. Get all sensors from TS service
        from app.services.time_series_service import TimeSeriesService

        ts_service = TimeSeriesService(db)

        all_stations = ts_service.get_stations(limit=1000)  # Get a large batch

        # 3. Filter
        # Note: ts_service maps thing/@iot.id to 'id' (string).
        # Project link stores the original @iot.id (as a string) in sensor_id.
        available = [
            s
            for s in all_stations
            if str(s.get("id")) not in [str(lid) for lid in linked_ids]
        ]

        return available

    # --- Member Management ---

    @staticmethod
    def add_member(
        db: Session,
        project_id: UUID,
        member_in: ProjectMemberCreate,
        user: Dict[str, Any],
    ) -> ProjectMember:
        # Only Owner/Admin can manage members
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        is_owner = str(project.owner_id) == str(user.get("sub"))
        if not (is_owner or ProjectService._is_admin(user)):
            raise HTTPException(status_code=403, detail="Only Owner can manage members")

        member = ProjectMember(
            project_id=project_id, user_id=member_in.user_id, role=member_in.role
        )
        db.add(member)
        db.commit()
        db.refresh(member)
        return member

    @staticmethod
    def list_members(
        db: Session, project_id: UUID, user: Dict[str, Any]
    ) -> List[ProjectMemberResponse]:
        ProjectService._check_access(db, project_id, user, required_role="viewer")
        members = (
            db.query(ProjectMember).filter(ProjectMember.project_id == project_id).all()
        )

        # Populate usernames
        from app.services.keycloak_service import KeycloakService

        results = []
        for m in members:
            # Convert SQLAlchemy model to Pydantic dict foundation
            m_dict = {
                "id": m.id,
                "project_id": m.project_id,
                "user_id": m.user_id,
                "role": m.role,
                "created_at": m.created_at,
                "updated_at": m.updated_at,
                "username": "Unknown",
            }
            # Try resolve username
            try:
                k_user = KeycloakService.get_user_by_id(str(m.user_id))
                if k_user:
                    m_dict["username"] = k_user.get("username")
            except Exception as exc:
                # Best-effort: on any failure, keep the default "Unknown" username but log the error.
                logger.warning(
                    "Failed to resolve username for user_id %s: %s", m.user_id, exc
                )
            results.append(ProjectMemberResponse(**m_dict))

        return results

    @staticmethod
    def update_member(
        db: Session, project_id: UUID, user_id: str, role: str, user: Dict[str, Any]
    ) -> ProjectMember:
        # Only Owner/Admin can manage members
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        is_owner = str(project.owner_id) == str(user.get("sub"))
        if not (is_owner or ProjectService._is_admin(user)):
            raise HTTPException(status_code=403, detail="Only Owner can manage members")

        member = (
            db.query(ProjectMember)
            .filter(
                ProjectMember.project_id == project_id, ProjectMember.user_id == user_id
            )
            .first()
        )

        if not member:
            raise HTTPException(status_code=404, detail="Member not found")

        member.role = role
        db.commit()
        db.refresh(member)
        return member

    @staticmethod
    def remove_member(
        db: Session, project_id: UUID, user_id: str, user: Dict[str, Any]
    ):
        # Only Owner/Admin can manage members
        project = db.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        is_owner = str(project.owner_id) == str(user.get("sub"))
        if not (is_owner or ProjectService._is_admin(user)):
            raise HTTPException(status_code=403, detail="Only Owner can manage members")

        # Prevent owner from removing themselves? (Optional, but good practice)
        if str(user_id) == str(project.owner_id):
            raise HTTPException(
                status_code=400, detail="Owner cannot be removed from project"
            )

        stmt = ProjectMember.__table__.delete().where(
            and_(
                ProjectMember.project_id == project_id, ProjectMember.user_id == user_id
            )
        )
        db.execute(stmt)
        db.commit()
        return {"status": "removed"}
