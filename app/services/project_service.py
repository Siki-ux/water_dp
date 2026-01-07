from typing import Any, Dict, List
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.user_context import Project, ProjectMember, project_sensors
from app.schemas.user_context import ProjectCreate, ProjectMemberCreate, ProjectUpdate


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
            # Ignore duplicate violation provided it's the same link
            pass
        except Exception:
            db.rollback()
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
    def list_sensors(db: Session, project_id: UUID, user: Dict[str, Any]) -> List[str]:
        ProjectService._check_access(db, project_id, user, required_role="viewer")

        stmt = select(project_sensors.c.sensor_id).where(
            project_sensors.c.project_id == project_id
        )
        result = db.execute(stmt).scalars().all()
        return list(result)

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
    ) -> List[ProjectMember]:
        ProjectService._check_access(db, project_id, user, required_role="viewer")
        return (
            db.query(ProjectMember).filter(ProjectMember.project_id == project_id).all()
        )
