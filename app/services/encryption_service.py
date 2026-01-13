import logging

from cryptography.fernet import Fernet

from app.core.config import settings

logger = logging.getLogger(__name__)


class EncryptionService:
    def __init__(self):
        self.key = settings.encryption_key
        if not self.key:
            # Generate a temporary key for development if not provided,
            # or raise warning. Ideally should be provided.
            # For now, we'll generate one but warn
            # In production, this must be set.
            self.key = Fernet.generate_key().decode()
            logger.warning(
                "WARNING: ENCRYPTION_KEY not set. Using temporary key. Data will not be recoverable after restart."
            )

        try:
            # Ensure key is bytes
            key_bytes = self.key.encode() if isinstance(self.key, str) else self.key
            self.fernet = Fernet(key_bytes)
        except Exception as e:
            raise ValueError(f"Invalid ENCRYPTION_KEY: {e}")

    def encrypt(self, data: str) -> str:
        if not data:
            return ""
        return self.fernet.encrypt(data.encode()).decode()

    def decrypt(self, token: str) -> str:
        if not token:
            return ""
        return self.fernet.decrypt(token.encode()).decode()


encryption_service = EncryptionService()
