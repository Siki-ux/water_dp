import os
import uuid
from typing import Any, Optional

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel

from app.api.deps import get_current_active_superuser
from app.tasks.import_tasks import import_geojson_task, import_timeseries_task

router = APIRouter()

# 200MB limit for bulk files
MAX_BULK_FILE_SIZE = 200 * 1024 * 1024
TEMP_IMPORT_DIR = "app/temp_imports"

if not os.path.exists(TEMP_IMPORT_DIR):
    os.makedirs(TEMP_IMPORT_DIR)


class TaskSubmissionResponse(BaseModel):
    task_id: str
    status: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[Any] = None


@router.post(
    "/import/geojson",
    response_model=TaskSubmissionResponse,
    dependencies=[Depends(get_current_active_superuser)],
)
async def import_geojson(file: UploadFile = File(...)):
    # Create secure temp filename
    filename = f"{uuid.uuid4()}_{file.filename}"
    file_path = os.path.join(TEMP_IMPORT_DIR, filename)

    try:
        # Stream file to disk to avoid memory exhaustion
        total_size = 0
        with open(file_path, "wb") as buffer:
            while chunk := await file.read(1024 * 1024):  # Read in 1MB chunks
                total_size += len(chunk)
                if total_size > MAX_BULK_FILE_SIZE:
                    raise HTTPException(
                        status_code=400, detail="File exceeds 200MB limit"
                    )
                buffer.write(chunk)

        # Pass file path to task
        task = import_geojson_task.delay(file_path)
        return {"task_id": task.id, "status": "submitted"}

    except HTTPException:
        # Cleanup if we blew the limit
        if os.path.exists(file_path):
            os.remove(file_path)
        raise
    except Exception as error:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=500, detail=str(error))


@router.post(
    "/import/timeseries",
    response_model=TaskSubmissionResponse,
    dependencies=[Depends(get_current_active_superuser)],
)
async def import_timeseries(file: UploadFile = File(...)):
    filename = f"{uuid.uuid4()}_{file.filename}"
    file_path = os.path.join(TEMP_IMPORT_DIR, filename)

    try:
        total_size = 0
        with open(file_path, "wb") as buffer:
            while chunk := await file.read(1024 * 1024):
                total_size += len(chunk)
                if total_size > MAX_BULK_FILE_SIZE:
                    raise HTTPException(
                        status_code=400, detail="File exceeds 200MB limit"
                    )
                buffer.write(chunk)

        task = import_timeseries_task.delay(file_path)
        return {"task_id": task.id, "status": "submitted"}

    except HTTPException:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/tasks/{task_id}",
    response_model=TaskStatusResponse,
    dependencies=[Depends(get_current_active_superuser)],
)
def get_import_status(task_id: str):
    from app.core.celery_app import celery_app

    task_result = AsyncResult(task_id, app=celery_app)
    return {
        "task_id": task_id,
        "status": task_result.status,
        "result": task_result.result if task_result.ready() else None,
    }
