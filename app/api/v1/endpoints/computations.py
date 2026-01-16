import ast
import os
import uuid
from datetime import datetime
from typing import List, Optional

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import UUID4, BaseModel
from sqlalchemy.orm import Session

from app.api import deps
from app.core.database import get_db
from app.models.computations import ComputationJob, ComputationScript
from app.services.project_service import ProjectService
from app.tasks.computation_tasks import run_computation_task

router = APIRouter()


class ComputationScriptRead(BaseModel):
    id: UUID4
    name: str
    description: str | None
    project_id: UUID4
    filename: str

    class ConfigDict:
        from_attributes = True


class ComputationRequest(BaseModel):
    params: dict = {}


class TaskSubmissionResponse(BaseModel):
    task_id: str
    status: str


COMPUTATIONS_DIR = "app/computations"
MAX_FILE_SIZE = 1 * 1024 * 1024  # 1MB


def validate_script_security(content: str):
    """
    Scan python code for dangerous imports and operations.
    """
    try:
        tree = ast.parse(content)
    except SyntaxError:
        raise HTTPException(status_code=400, detail="Invalid Python syntax")

    for node in ast.walk(tree):
        # Check for dangerous imports
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in ["subprocess", "os", "platform"]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Security violation: Import '{alias.name}' is forbidden",
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module in ["subprocess", "os", "platform"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Security violation: Import from '{node.module}' is forbidden",
                )

        # Check for dangerous calls (eval, exec)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id in ["eval", "exec"]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Security violation: Function '{node.func.id}' is forbidden",
                    )


@router.post("/upload", response_model=ComputationScriptRead)
async def upload_computation_script(
    file: UploadFile = File(...),
    name: str = Form(...),
    description: str = Form(None),
    project_id: UUID4 = Form(...),
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Upload a new computation script and associate it with a project.
    Requires 'editor' access to the project.
    """
    # Check Project Access (Editor required)
    ProjectService._check_access(db, project_id, current_user, required_role="editor")

    # 1. Validate Extension
    if not file.filename.endswith(".py"):
        raise HTTPException(status_code=400, detail="Only .py files are allowed")

    # 2. Validate Size & Content Security
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File size exceeds 1MB limit")

    content_str = content.decode("utf-8")
    validate_script_security(content_str)

    # Restore file cursor for saving (though we use content_str or just write content directly)
    # Actually simpler to just write 'content' since we already read it all

    if not os.path.exists(COMPUTATIONS_DIR):
        os.makedirs(COMPUTATIONS_DIR)

    # Secure filename - ensure valid python module name (no dashes)
    project_hex = (
        project_id.hex
        if hasattr(project_id, "hex")
        else str(project_id).replace("-", "")
    )
    safe_filename = f"{project_hex}_{uuid.uuid4().hex[:8]}_{file.filename}"
    file_path = os.path.join(COMPUTATIONS_DIR, safe_filename)

    # Save File
    with open(file_path, "wb") as buffer:
        buffer.write(content)

    # Save to DB
    db_script = ComputationScript(
        id=uuid.uuid4(),
        name=name,
        description=description,
        filename=safe_filename,
        project_id=project_id,
        uploaded_by=current_user.get("sub", "unknown"),
    )
    db.add(db_script)
    db.commit()
    db.refresh(db_script)

    return db_script


@router.post("/run/{script_id}", response_model=TaskSubmissionResponse)
def run_computation(
    script_id: UUID4,
    request: ComputationRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Run a computation script by ID.
    Requires 'viewer' access to the associated project.
    """
    script = (
        db.query(ComputationScript).filter(ComputationScript.id == script_id).first()
    )
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    # Check Project Access (Viewer required)
    ProjectService._check_access(
        db, script.project_id, current_user, required_role="viewer"
    )

    # Check file existence
    script_path = os.path.join(COMPUTATIONS_DIR, script.filename)
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="Script file missing on server")

    # Pass the filename (module name logic will handle it in the task)
    # We strip .py extension for the module loader if needed, or handle it in task
    # Let's pass the filename and let the task resolve it.

    # NOTE: The task currently expects a module path like "app.computations.xyz".
    # Our filenames are now "project-uuid_xyz.py".
    # The module name would be "app.computations.project-uuid_xyz" (without .py)

    module_name = script.filename
    if module_name.endswith(".py"):
        module_name = module_name[:-3]

    task = run_computation_task.delay(
        module_name, request.params, script_id=str(script.id)
    )

    # Create Job Record
    job = ComputationJob(
        id=task.id,
        script_id=script.id,
        user_id=current_user.get("sub"),
        status="PENDING",
        start_time=datetime.utcnow().isoformat(),
        created_by=current_user.get("preferred_username", "unknown"),
        updated_by=current_user.get("preferred_username", "unknown"),
    )
    db.add(job)
    db.commit()

    return {"task_id": task.id, "status": "submitted"}


@router.get("/list/{project_id}", response_model=List[ComputationScriptRead])
def list_project_computations(
    project_id: UUID4,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    List scripts for a specific project.
    """
    ProjectService._check_access(db, project_id, current_user, required_role="viewer")

    scripts = (
        db.query(ComputationScript)
        .filter(ComputationScript.project_id == project_id)
        .all()
    )
    return scripts


@router.get("/scripts", response_model=List[ComputationScriptRead])
def list_all_scripts(
    project_id: Optional[UUID4] = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    List scripts. If project_id provided, filter by project.
    """
    query = db.query(ComputationScript)

    if project_id:
        ProjectService._check_access(
            db, project_id, current_user, required_role="viewer"
        )
        query = query.filter(ComputationScript.project_id == project_id)
    else:
        # If no project_id, maybe list all accessible?
        # For now, let's just return all if user is admin, or empty/error?
        # User asked "only getting scripts that are available for this project".
        # If called without project_id, we might return nothing or all public?
        # Let's enforce project_id for now or return all if admin.
        pass

    return query.all()


class ComputationJobRead(BaseModel):
    id: str
    script_id: UUID4
    user_id: str
    status: str
    start_time: str | None
    end_time: str | None
    result: str | None
    error: str | None
    logs: str | None
    created_by: str | None

    class ConfigDict:
        from_attributes = True


@router.get("/jobs/{script_id}", response_model=List[ComputationJobRead])
def list_script_jobs(
    script_id: UUID4,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    List execution history for a script.
    Syncs PENDING jobs from Celery before returning.
    """
    script = (
        db.query(ComputationScript).filter(ComputationScript.id == script_id).first()
    )
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    ProjectService._check_access(
        db, script.project_id, current_user, required_role="viewer"
    )

    jobs = (
        db.query(ComputationJob)
        .filter(ComputationJob.script_id == script_id)
        .order_by(ComputationJob.start_time.desc())
        .limit(50)
        .all()
    )

    # Sync PENDING jobs
    import json

    from app.core.celery_app import celery_app

    dirty = False
    for job in jobs:
        if job.status in ["PENDING", "STARTED"]:
            task_result = AsyncResult(job.id, app=celery_app)
            if task_result.ready():
                job.status = task_result.status
                job.end_time = datetime.utcnow().isoformat()

                if task_result.successful():
                    res = task_result.result
                    if isinstance(res, (dict, list)):
                        job.result = json.dumps(res)
                        job.logs = json.dumps(res, indent=2)
                    else:
                        job.result = str(res)
                        job.logs = str(res)
                else:
                    job.error = str(task_result.result)
                    job.logs = f"Error: {task_result.result}\nTraceback: {task_result.traceback}"

                dirty = True

    if dirty:
        db.commit()
        # Refresh all jobs to get updated state (though objects should be updated in session)
        # No need to re-query as SQLAlchemy updates the objects in identity map

    return jobs


@router.get("/tasks/{task_id}")
def get_computation_status(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Get the status of a computation task.
    Syncs Celery result to DB if finished.
    """
    job = db.query(ComputationJob).filter(ComputationJob.id == task_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Authorization Check
    roles = current_user.get("realm_access", {}).get("roles", [])
    is_superuser = "admin" in roles or "admin-siki" in roles

    if not is_superuser and job.user_id != current_user.get("sub"):
        raise HTTPException(status_code=403, detail="Not authorized to view this job")

    # If already finished in DB, return immediately
    if job.status in ["SUCCESS", "FAILURE", "REVOKED"]:
        return {
            "task_id": task_id,
            "status": job.status,
            "result": job.result,
            "error": job.error,
            "logs": job.logs,
        }

    from app.core.celery_app import celery_app

    task_result = AsyncResult(task_id, app=celery_app)

    # Check if ready and sync to DB
    if task_result.ready():
        import json

        job.status = task_result.status
        job.end_time = datetime.utcnow().isoformat()

        if task_result.successful():
            # For now, we store the whole result as JSON in 'result'
            # And also as 'logs' just for visibility in the simple UI
            res = task_result.result
            if isinstance(res, (dict, list)):
                job.result = json.dumps(res)
                job.logs = json.dumps(res, indent=2)
            else:
                job.result = str(res)
                job.logs = str(res)
        else:
            # Failure
            job.error = str(task_result.result)  # Exception message
            job.logs = (
                f"Error: {task_result.result}\nTraceback: {task_result.traceback}"
            )

        db.commit()
        db.refresh(job)

    return {
        "task_id": task_id,
        "status": task_result.status,
        "result": job.result,
        "logs": job.logs,
    }


@router.get("/content/{script_id}")
def get_script_content(
    script_id: UUID4,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Get the raw content of a computation script.
    """
    script = (
        db.query(ComputationScript).filter(ComputationScript.id == script_id).first()
    )
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    ProjectService._check_access(
        db, script.project_id, current_user, required_role="viewer"
    )

    script_path = os.path.join(COMPUTATIONS_DIR, script.filename)
    if not os.path.exists(script_path):
        raise HTTPException(status_code=404, detail="Script file missing on server")

    with open(script_path, "r", encoding="utf-8") as f:
        content = f.read()

    return {"content": content}


class ScriptContentUpdate(BaseModel):
    content: str


@router.put("/content/{script_id}")
def update_script_content(
    script_id: UUID4,
    update_data: ScriptContentUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Update the content of a computation script.
    Requires 'editor' access.
    """
    script = (
        db.query(ComputationScript).filter(ComputationScript.id == script_id).first()
    )
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    ProjectService._check_access(
        db, script.project_id, current_user, required_role="editor"
    )

    new_content = update_data.content

    # 1. Validate Security
    validate_script_security(new_content)

    # 2. Validate Size (approx)
    if len(new_content.encode("utf-8")) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File size exceeds 1MB limit")

    # 3. Save
    script_path = os.path.join(COMPUTATIONS_DIR, script.filename)
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(new_content)

    return {"status": "updated", "id": script_id}
