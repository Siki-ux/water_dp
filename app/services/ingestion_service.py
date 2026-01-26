import io
import logging

from fastapi import UploadFile
from minio import Minio
from minio.error import S3Error

from app.core.exceptions import AppException, ResourceNotFoundException
from app.services.timeio.timeio_db import TimeIODatabase

logger = logging.getLogger(__name__)


class IngestionService:
    @staticmethod
    async def upload_csv(thing_uuid: str, file: UploadFile):
        """
        Upload a CSV file to the Thing's S3 bucket to trigger ingestion.
        """
        db = TimeIODatabase()

        # 1. Fetch S3 Config for this Thing
        s3_config = db.get_s3_config(thing_uuid)
        if not s3_config:
            raise ResourceNotFoundException(
                message="Thing not found or S3 not configured"
            )

        bucket = s3_config["bucket"]
        access_key = s3_config["user"]
        secret_key = s3_config["password"]

        # 2. Initialize MinIO Client
        # We use the internal MinIO Endpoint
        # Note: settings.OBJECT_STORAGE_HOST might be "localhost:9000" or internal "object-storage:9000"
        # Since we are in the API container, we should use the internal docker-compose name "object-storage" if configured,
        # or use what is in settings.
        # Let's check settings for MINIO/OBJECT_STORAGE config.
        # Assuming settings has keys. If not, we might need to rely on env vars or internal defaults.
        # Docker Compose says: object-storage ports 9000.

        minio_endpoint = "object-storage:9000"  # Internal connection

        try:
            client = Minio(
                minio_endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=False,  # Internal usually HTTP
            )

            # 3. Read File Content
            content = await file.read()
            file_size = len(content)

            # 4. Upload
            object_name = file.filename

            # Use byte stream
            client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=io.BytesIO(content),
                length=file_size,
                content_type="text/csv",
            )

            logger.info(
                f"Uploaded {object_name} to bucket {bucket} for thing {thing_uuid}"
            )
            return {"status": "success", "bucket": bucket, "file": object_name}

        except S3Error as e:
            logger.error(f"MinIO Error: {e}")
            raise AppException(message=f"S3 Upload failed: {e}")
        except Exception as e:
            logger.error(f"Ingestion Error: {e}")
            raise AppException(message=f"Ingestion failed: {e}")
