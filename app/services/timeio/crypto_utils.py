from cryptography.fernet import Fernet

from app.core.config import settings


def get_fernet() -> Fernet:
    """Get Fernet instance using the shared secret."""
    key = settings.fernet_encryption_secret
    if not key:
        raise ValueError("FERNET_ENCRYPTION_SECRET is not set in environment")
    return Fernet(key)


def encrypt_password(password: str) -> str:
    """Encrypt password using Fernet (used for ConfigDB)."""
    return get_fernet().encrypt(password.encode()).decode()


def decrypt_password(token: str) -> str:
    """Decrypt password using Fernet."""
    return get_fernet().decrypt(token.encode()).decode()
