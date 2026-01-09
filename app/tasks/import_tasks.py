import os

from app.core.celery_app import celery_app


@celery_app.task(bind=True)
def import_geojson_task(self, file_path: str):
    try:
        if not os.path.exists(file_path):
            return {"status": "Error", "detail": f"File not found: {file_path}"}

        # Determine strict processing now that we have the full file
        # In real impl, read line by line or use geopandas

        file_size = os.path.getsize(file_path)

        # Clean up file after processing (or if error)
        # For now, simplistic mock
        result = {
            "status": "Mock success",
            "items_processed": 100,
            "file_size": file_size,
        }

        # Cleanup
        os.remove(file_path)
        return result
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        return {"status": "Error", "detail": str(e)}


@celery_app.task(bind=True)
def import_timeseries_task(self, file_path: str):
    try:
        if not os.path.exists(file_path):
            return {"status": "Error", "detail": f"File not found: {file_path}"}

        file_size = os.path.getsize(file_path)

        result = {
            "status": "Mock success",
            "rows_processed": 5000,
            "file_size": file_size,
        }

        os.remove(file_path)
        return result
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        return {"status": "Error", "detail": str(e)}
