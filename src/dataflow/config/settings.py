from enum import Enum
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from dataflow.utils.common import ORM

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"


class ORMType(str, Enum):
    """Supported ORM types"""
    SQLALCHEMY = "sqlalchemy"
    PEEWEE = "peewee"


class DatabaseType(str, Enum):
    """Supported database types"""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    MSSQL = "mssql"

class Settings(BaseSettings):
    """Main configuration settings"""

    # ==============================================================================
    # Application Settings
    # ==============================================================================
    APP_NAME: str = "dataflow"

    # ==============================================================================
    # ORM Configuration
    # ==============================================================================
    ORM_TYPE: ORMType = ORMType.SQLALCHEMY

    # ==============================================================================
    # Validators
    # ==============================================================================
    @field_validator('DATABASE_PORT')
    @classmethod
    def validate_database_port(cls, v: int, info) -> int:
        """Set default port based on database type if not explicitly set"""
        # This validator runs after DATABASE_TYPE is set
        # Default ports are already set in field defaults
        if v <= 0 or v > 65535:
            raise ValueError(f"DATABASE_PORT must be between 1 and 65535, got {v}")
        return v

    # ==============================================================================
    # Custom Settings Sources (Optional - Only .env file)
    # ==============================================================================
    @classmethod
    def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """
        Customize settings sources to read ONLY from .env file.
        System environment variables are ignored.

        To allow system env vars, return:
        return (init_settings, env_settings, dotenv_settings)

        """
        return (init_settings, dotenv_settings)

    # ==============================================================================
    # Helper Methods
    # ==============================================================================
    def get_database_url(self) -> str:
        """Get database URL.
        If DATABASE_URL is set, use it. Otherwise, construct from individual settings.
        """
        if self.DATABASE_URL:
            return self.DATABASE_URL

        # Construct URL based on database type
        if self.DATABASE_TYPE == DatabaseType.POSTGRESQL:
            driver = "postgresql"
        elif self.DATABASE_TYPE == DatabaseType.MYSQL:
            driver = "mysql+pymysql"
        elif self.DATABASE_TYPE == DatabaseType.SQLITE:
            # SQLite doesn't use host/port/user/password
            return f"sqlite:///{self.DATABASE_NAME}"
        elif self.DATABASE_TYPE == DatabaseType.MSSQL:
            driver = "mssql+pyodbc"
        else:
            raise ValueError(f"Unsupported database type: {self.DATABASE_TYPE}")

        # Construct URL
        return (
            f"{driver}://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}@"
            f"{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        )

    def get_default_port(self) -> int:
        """Get default port for the configured database type"""
        default_ports = {
            DatabaseType.POSTGRESQL: 5432,
            DatabaseType.MYSQL: 3306,
            DatabaseType.SQLITE: 0,  # SQLite doesn't use ports
            DatabaseType.MSSQL: 1433,
        }
        return default_ports.get(self.DATABASE_TYPE, 5432)

    def get_orm_config(self) -> dict[str, Any]:
        """Get ORM-specific configuration"""
        if self.ORM_TYPE == ORMType.SQLALCHEMY:
            return {
                'database_url': self.get_database_url(),
                'echo': self.DATABASE_ECHO,
                'pool_size': self.DATABASE_POOL_SIZE,
                'max_overflow': self.DATABASE_MAX_OVERFLOW,
            }
        elif self.ORM_TYPE == ORMType.PEEWEE:
            # Peewee uses individual connection parameters
            if self.DATABASE_TYPE == DatabaseType.SQLITE:
                return {
                    'database': self.DATABASE_NAME,
                }
            else:
                return {
                    'database': self.DATABASE_NAME,
                    'user': self.DATABASE_USER,
                    'password': self.DATABASE_PASSWORD,
                    'host': self.DATABASE_HOST,
                    'port': self.DATABASE_PORT,
                }
        else:
            raise ValueError(f"Unsupported ORM type: {self.ORM_TYPE}")

    def get_database_config(self) -> dict[str, Any]:
        """Get database configuration as a dictionary"""
        return {
            'type': self.DATABASE_TYPE,
            'host': self.DATABASE_HOST,
            'port': self.DATABASE_PORT,
            'database': self.DATABASE_NAME,
            'user': self.DATABASE_USER,
            'password': self.DATABASE_PASSWORD,
            'url': self.get_database_url(),
        }

    def get_redis_config(self) -> dict[str, Any]:
        """Get Redis configuration"""
        return {
            'host': self.REDIS_HOST,
            'port': self.REDIS_PORT,
            'db': self.REDIS_DB,
            'password': self.REDIS_PASSWORD,
            'ssl': self.REDIS_SSL,
        }

    def get_extractor_config(self, extractor_name: str) -> dict[str, Any]:
        """Get configuration for specific extractor"""
        configs = {
            'databento': {
                'api_key': self.DATABENTO_API_KEY
            },
            'polygon': {
                'api_key': self.POLYGON_API_KEY
            },
            'yfinance': {
                'rate_limit': self.YFINANCE_RATE_LIMIT
            }
        }
        return configs.get(extractor_name.lower(), {})

    def get_file_storage_path(self) -> Path:
        """Get file storage path as Path object"""
        return Path(self.FILE_STORAGE_PATH)


# ==============================================================================
# Singleton Instance
# ==============================================================================
settings = Settings()

# ==============================================================================
# Example Usage
# ==============================================================================
if __name__ == "__main__":
    print("Configuration Settings")
    print("=" * 70)

    print(f"\n📁 Application:")
    print(f"  Name: {settings.APP_NAME}")
    print(f"  Environment: {settings.ENV}")
    print(f"  Debug: {settings.DEBUG}")
    print(f"  Log Level: {settings.LOG_LEVEL}")

    print(f"\n🗄️ Database:")
    print(f"  Type: {settings.DATABASE_TYPE}")
    print(f"  Host: {settings.DATABASE_HOST}")
    print(f"  Port: {settings.DATABASE_PORT}")
    print(f"  Database: {settings.DATABASE_NAME}")
    print(f"  User: {settings.DATABASE_USER}")
    print(f"  URL: {settings.get_database_url()}")
    print(f"  ORM Type: {settings.ORM_TYPE}")
    print(f"  Pool Size: {settings.DATABASE_POOL_SIZE}")
    print(f"  Echo: {settings.DATABASE_ECHO}")

    print(f"\nREDIS:")
    print(f"  Host: {settings.REDIS_HOST}")
    print(f"  Port: {settings.REDIS_PORT}")
    print(f"  DB: {settings.REDIS_DB}")

    print(f"\n💾 File Storage:")
    print(f"  Path: {settings.FILE_STORAGE_PATH}")
    print(f"  Format: {settings.FILE_FORMAT}")

    print(f"\n🔑 API Keys:")
    print(f"  Databento: {'✅ Set' if settings.DATABENTO_API_KEY else '❌ Not set'}")
    print(f"  Polygon: {'✅ Set' if settings.POLYGON_API_KEY else '❌ Not set'}")

    print(f"\n⚙️ Helper Methods:")
    print(f"  Database Config: {settings.get_database_config()}")
    print(f"  ORM Config: {settings.get_orm_config()}")
    print(f"  Redis Config: {settings.get_redis_config()}")
    print(f"  Is Production: {settings.is_production()}")
    print(f"  Is Development: {settings.is_development()}")

    print("\n" + "=" * 70)