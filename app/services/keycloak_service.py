import logging
from typing import Optional

from app.core.config import settings
from keycloak import KeycloakAdmin

logger = logging.getLogger(__name__)


class KeycloakService:
    _admin_client: Optional[KeycloakAdmin] = None

    @classmethod
    def get_admin_client(cls) -> KeycloakAdmin:
        """
        Get or initialize Keycloak Admin client.

        Note: requires KEYCLOAK_ADMIN_CLIENT_SECRET or username/password in settings.
        Also assumes 'admin-cli' client exists or similar.
        """
        if cls._admin_client:
            return cls._admin_client

        try:
            # Determine connection mode: Client Credentials or Password
            # Usually for backend tasks, client_credentials with a service account is best.
            # But 'admin-cli' public client often uses password.
            # We'll try dynamic approach based on config.

            # Note: For keycloak-admin library, you usually connect to Master realm to manage others,
            # OR connect directly to target realm if the client has realm-management roles there.

            # Assuming we use a client in the SAME realm or a dedicated service account

            connection_args = {
                "server_url": settings.keycloak_url,
                "realm_name": settings.keycloak_realm,
                "client_id": settings.keycloak_admin_client_id,
                "verify": True,
            }

            if settings.keycloak_admin_client_secret:
                connection_args["client_secret_key"] = (
                    settings.keycloak_admin_client_secret
                )
                connection_args["user_realm_name"] = settings.keycloak_realm
                # For client_credentials, we usually don't set user_realm_name generally,
                # but python-keycloak might need it.
                # If using valid Service Account with realm-admin role:
                # authentication logic is handled by lib.
            elif settings.keycloak_admin_username and settings.keycloak_admin_password:
                connection_args["username"] = settings.keycloak_admin_username
                connection_args["password"] = settings.keycloak_admin_password
                connection_args["user_realm_name"] = (
                    "master"  # Admin users usually in master
                )
            else:
                # Fallback/Error
                logger.warning(
                    "No Keycloak Admin credentials found. User lookup will fail."
                )
                # Fallback/Error: do not proceed with incomplete authentication configuration
                error_msg = (
                    "No Keycloak Admin credentials configured. "
                    "Set KEYCLOAK_ADMIN_CLIENT_SECRET or "
                    "KEYCLOAK_ADMIN_USERNAME/KEYCLOAK_ADMIN_PASSWORD."
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            cls._admin_client = KeycloakAdmin(**connection_args)
            return cls._admin_client

        except Exception as e:
            logger.error(f"Failed to initialize Keycloak Admin client: {e}")
            raise

    @classmethod
    def get_user_by_username(cls, username: str) -> Optional[dict]:
        """
        Find user by username (exact match).
        Returns user dict (id, username, email, etc.) or None.
        """
        try:
            admin = cls.get_admin_client()
            # method: get_users(query={"username": ...})
            users = admin.get_users(query={"username": username, "exact": True})
            if users:
                return users[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user {username} from Keycloak: {e}")
            # Reset client on failure (token expiry etc)
            cls._admin_client = None
            return None

    @classmethod
    def get_user_by_email(cls, email: str) -> Optional[dict]:
        try:
            admin = cls.get_admin_client()
            users = admin.get_users(query={"email": email, "exact": True})
            if users:
                return users[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user email {email} from Keycloak: {e}")
            cls._admin_client = None
            return None

    @classmethod
    def get_user_by_id(cls, user_id: str) -> Optional[dict]:
        """Fetch user by UUID."""
        try:
            admin = cls.get_admin_client()
            return admin.get_user(user_id)
        except Exception as e:
            logger.error(f"Error fetching user ID {user_id} from Keycloak: {e}")
            cls._admin_client = None
            return None

    @classmethod
    def create_group(cls, group_name: str) -> Optional[str]:
        """Create a new group in Keycloak and return its ID."""
        try:
            admin = cls.get_admin_client()
            payload = {"name": group_name}
            # create_group returns the group ID
            group_id = admin.create_group(payload)
            return group_id
        except Exception as e:
            logger.error(f"Error creating group '{group_name}' in Keycloak: {e}")
            cls._admin_client = None
            return None
