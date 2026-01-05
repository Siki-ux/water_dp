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
    cors_origins: str = Field(default="*", alias="CORS_ORIGINS")

    # Database
    database_url: str = Field(alias="DATABASE_URL")
    database_pool_size: int = Field(default=10, alias="DATABASE_POOL_SIZE")
    database_max_overflow: int = Field(default=20, alias="DATABASE_MAX_OVERFLOW")

    # Redis
    redis_url: str = Field(default="redis://localhost:6379", alias="REDIS_URL")

    # GeoServer
    geoserver_url: str = Field(alias="GEOSERVER_URL")
    geoserver_username: str = Field(alias="GEOSERVER_USERNAME")
    geoserver_password: str = Field(alias="GEOSERVER_PASSWORD")
    geoserver_workspace: str = Field(default="water_data", alias="GEOSERVER_WORKSPACE")

    # Time Data Processing
    time_zone: str = Field(default="UTC", alias="TIME_ZONE")
    max_time_range_days: int = Field(default=365, alias="MAX_TIME_RANGE_DAYS")

    # Security
    secret_key: str = Field(alias="SECRET_KEY")
    access_token_expire_minutes: int = Field(
        default=30, alias="ACCESS_TOKEN_EXPIRE_MINUTES"
    )

    # Monitoring
    sentry_dsn: Optional[str] = Field(default=None, alias="SENTRY_DSN")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    model_config = {"env_file": ".env", "case_sensitive": False}

    @property
    def cors_origins_list(self) -> List[str]:
        """Convert CORS origins string to list."""
        if self.cors_origins == "*":
            return ["*"]
        return [origin.strip() for origin in self.cors_origins.split(",")]


# Global settings instance
settings = Settings()
