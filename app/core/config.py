"""
Application configuration management using Pydantic Settings.
"""

from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # Application
    app_name: str = Field(default="Water Data Platform", alias="APP_NAME")
    debug: bool = Field(default=False, alias="DEBUG")
    version: str = Field(default="1.0.0", alias="VERSION")
    seeding: bool = Field(default=False, alias="SEEDING")

    # API
    api_prefix: str = Field(default="/api/v1", alias="API_PREFIX")
    # WARNING: CORS_ORIGINS set to "*" is for development only.
    # In production, specify exact allowed origins (e.g., "https://myapp.com").
    cors_origins: str = Field(default="*", alias="CORS_ORIGINS")

    # Database
    database_url: str = Field(alias="DATABASE_URL")
    postgres_db: Optional[str] = Field(default=None, alias="POSTGRES_DB")
    database_pool_size: int = Field(default=10, alias="DATABASE_POOL_SIZE")
    database_max_overflow: int = Field(default=20, alias="DATABASE_MAX_OVERFLOW")

    # Redis
    redis_url: str = Field(default="redis://localhost:6379", alias="REDIS_URL")

    # GeoServer
    geoserver_url: str = Field(
        default="http://localhost:8080/geoserver", alias="GEOSERVER_URL"
    )
    geoserver_username: str = Field(default="admin", alias="GEOSERVER_USERNAME")
    geoserver_password: str = Field(default="geoserver", alias="GEOSERVER_PASSWORD")
    geoserver_workspace: str = Field(default="water_data", alias="GEOSERVER_WORKSPACE")
    geoserver_timeout: int = Field(default=30, alias="GEOSERVER_TIMEOUT")

    # Time Data Processing
    time_zone: str = Field(default="UTC", alias="TIME_ZONE")
    max_time_range_days: int = Field(default=365, alias="MAX_TIME_RANGE_DAYS")
    frost_url: str = Field(
        default="http://frost:8080/FROST-Server/v1.1", alias="FROST_URL"
    )
    frost_timeout: int = Field(default=30, alias="FROST_TIMEOUT")

    # Security
    secret_key: str = Field(alias="SECRET_KEY")
    access_token_expire_minutes: int = Field(
        default=30, alias="ACCESS_TOKEN_EXPIRE_MINUTES"
    )
    keycloak_url: str = Field(default="http://localhost:8081", alias="KEYCLOAK_URL")
    keycloak_external_url: Optional[str] = Field(
        default=None, alias="KEYCLOAK_EXTERNAL_URL"
    )
    keycloak_realm: str = Field(default="timeio", alias="KEYCLOAK_REALM")
    keycloak_client_id: str = Field(default="timeIO-client", alias="KEYCLOAK_CLIENT_ID")
    keycloak_admin_client_id: str = Field(
        default="admin-cli", alias="KEYCLOAK_ADMIN_CLIENT_ID"
    )
    keycloak_admin_client_secret: str = Field(
        default="", alias="KEYCLOAK_ADMIN_CLIENT_SECRET"
    )
    keycloak_admin_username: Optional[str] = Field(
        default=None, alias="KEYCLOAK_ADMIN_USERNAME"
    )
    keycloak_admin_password: Optional[str] = Field(
        default=None, alias="KEYCLOAK_ADMIN_PASSWORD"
    )
    encryption_key: Optional[str] = Field(default=None, alias="ENCRYPTION_KEY")

    # Monitoring
    sentry_dsn: Optional[str] = Field(default=None, alias="SENTRY_DSN")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Celery
    celery_broker_url: str = Field(
        default="redis://localhost:6379/0", alias="CELERY_BROKER_URL"
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/0", alias="CELERY_RESULT_BACKEND"
    )

    model_config = {"env_file": ".env", "case_sensitive": False}

    @property
    def cors_origins_list(self) -> List[str]:
        """Convert CORS origins string to list."""
        if self.cors_origins == "*":
            return ["*"]
        if self.cors_origins.strip().startswith("["):
            import json

            try:
                return json.loads(self.cors_origins)
            except json.JSONDecodeError:
                # Fallback to comma split if json parse fails
                pass
        return [origin.strip() for origin in self.cors_origins.split(",")]

    @property
    def postgres_db_name(self) -> str:
        """Extract database name from DATABASE_URL."""
        from sqlalchemy.engine.url import make_url

        try:
            return make_url(self.database_url).database
        except Exception:
            # Fallback for simple string parsing if sqlalchemy fails or url is invalid
            if "/" in self.database_url:
                with_params = self.database_url.split("/")[-1]
                return with_params.split("?")[0]
            return "water_app"


# Global settings instance
settings = Settings()
