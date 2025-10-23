import re
import logging
from pathlib import Path
from pydantic import field_validator
from typing import Optional, Any, Union, get_args, get_origin
from pydantic_settings import BaseSettings, SettingsConfigDict

from dataflow.utils.common import ORM
from dataflow.utils.database import DatabaseConnectionBuilder

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseSettings):
    """
    Configuration for database connections.
    """

    id: str
    orm: str  # 'sqlalchemy' or 'peewee'
    db_type: str  # e.g., 'postgres', 'mysql', 'mssql'
    host: str
    port: Optional[int] = None
    username: str
    password: str
    database: str
    driver: str
    trusted_connection: bool = False
    connection_pool_max_size: int = 10
    connection_pool_recycle: int = 3600
    autocommit: bool = True

    def connection_params(self) -> str | dict:
        """
        Build connection string based on ORM type and database type.

        Returns:
            str: Connection string for SQLAlchemy, or dict for Peewee.

        Raises:
            ValueError: If ORM type is not supported.
        """
        if self.orm == ORM.SQLALCHEMY:
            return DatabaseConnectionBuilder.build_sqlalchemy_connection_string(
                db_type=self.db_type,
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                driver=self.driver,
                trusted_connection=self.trusted_connection,
                autocommit=self.autocommit,
            )
        elif self.orm == ORM.PEEWEE:
            return DatabaseConnectionBuilder.build_peewee_connection_params(
                db_type=self.db_type,
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                driver=self.driver,
                trusted_connection=self.trusted_connection,
                autocommit=self.autocommit,
            )
        else:
            raise ValueError(f"Unsupported ORM type: {self.orm}")


class RedisConfig(BaseSettings):
    id: str
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    ssl: bool
    db: int


class FileConfig(BaseSettings):
    id: str
    file_storage_path: Path
    file_format: str


class Settings(BaseSettings):
    # Application
    app_name: str

    # Time Series
    time_series_config_type: str
    time_series_config_path: Path
    time_series_config_db_id: str
    time_series_config_db_table: str

    # ORM
    orm_type: str

    # Databases
    db1_id: str
    db1_type: str
    db1_host: str
    db1_port: Optional[int]
    db1_username: str
    db1_password: str
    db1_database: str
    db1_driver: str
    db1_trusted_connection: bool
    db1_connection_pool_max_size: int
    db1_connection_pool_recycle: int
    db1_autocommit: bool

    db2_id: str
    db2_type: str
    db2_host: str
    db2_port: Optional[int]
    db2_username: str
    db2_password: str
    db2_database: str
    db2_driver: str
    db2_trusted_connection: bool
    db2_connection_pool_max_size: int
    db2_connection_pool_recycle: int
    db2_autocommit: bool

    # Redis
    redis1_id: str
    redis1_host: Optional[str]
    redis1_port: Optional[int]
    redis1_username: Optional[str]
    redis1_password: Optional[str]
    redis1_ssl: Optional[bool]
    redis1_db: Optional[int]

    redis2_id: str
    redis2_host: Optional[str]
    redis2_port: Optional[int]
    redis2_username: Optional[str]
    redis2_password: Optional[str]
    redis2_ssl: Optional[bool]
    redis2_db: Optional[int]

    # File Storage
    file1_id: str
    file1_storage_path: Path
    file1_format: str

    # Data Provider API Keys
    databento_api_key: Optional[str]
    onyx_api_key: Optional[str]
    onyx_url: Optional[str]
    sparta_api_key: Optional[str]

    model_config = SettingsConfigDict(
        env_file=[Path(__file__).resolve().parent / '.env.common', Path(__file__).resolve().parent / '.env'],
        env_file_encoding="utf-8",
        extra="forbid",
        case_sensitive=False,
    )

    @field_validator("*", mode="before")
    @classmethod
    def all_fields(cls, v: Any, info):
        if v == '':
            field = cls.model_fields.get(info.field_name)
            if field and field.annotation:
                origin = get_origin(field.annotation)
                if origin is Union:
                    args = get_args(field.annotation)
                    if type(None) in args:
                        return None
        elif isinstance(v, str) and re.match(r"^file\d+_storage_path$", v):
            path = Path(v)
            if path.is_absolute():
                return path
            elif str(v).startswith("~"):
                return path.expanduser().resolve()
            else:
                raise ValueError(f"Invalid file storage path: {v}")
        return v

    @field_validator('time_series_config_path', mode='before')
    @classmethod
    def resolve_time_series_config_path(cls, v: str) -> Path:
        path = Path(v)
        if path.is_absolute():
            return path
        elif str(v).startswith('./'):
            settings_file_path = Path(__file__)
            return settings_file_path.parent.parent / v
        else:
            raise ValueError(f"Invalid time series config path: {v}")

    def all_databases(self) -> dict[str, DatabaseConfig]:
        databases = {}
        prefixes = set()

        for field_name, field in self.__class__.model_fields.items():
            if field_name.endswith('_id') and field_name.startswith('db'):
                prefix = field_name.replace('_id', '')
                prefixes.add(prefix)

        for prefix in prefixes:
            database_id = getattr(self, f'{prefix}_id')
            databases[database_id] = DatabaseConfig(
                id=database_id,
                orm=getattr(self, 'orm_type'),
                db_type=getattr(self, f'{prefix}_type'),
                host=getattr(self, f'{prefix}_host'),
                port=getattr(self, f'{prefix}_port', None),
                username=getattr(self, f'{prefix}_username'),
                password=getattr(self, f'{prefix}_password'),
                database=getattr(self, f'{prefix}_database'),
                driver=getattr(self, f'{prefix}_driver'),
                trusted_connection=getattr(self, f'{prefix}_trusted_connection'),
                connection_pool_max_size=getattr(self, f'{prefix}_connection_pool_max_size'),
                connection_pool_recycle=getattr(self, f'{prefix}_connection_pool_recycle'),
                autocommit=getattr(self, f'{prefix}_autocommit'),
            )
        return databases

    def all_redis(self) -> dict[str, RedisConfig]:
        redis = {}
        prefixes = set()

        for field_name, field in self.__class__.model_fields.items():
            if field_name.startswith('redis') and field_name.endswith('_id'):
                prefix = field_name.replace('_id', '')
                prefixes.add(prefix)

        for prefix in prefixes:
            redis_id = getattr(self, f'{prefix}_id')
            host = getattr(self, f'{prefix}_host')
            port = getattr(self, f'{prefix}_port')
            user_name = getattr(self, f'{prefix}_username')
            pwd = getattr(self, f'{prefix}_password')
            if host and port:
                redis[redis_id] = RedisConfig(
                    id=redis_id,
                    host=host,
                    port=port,
                    username=user_name if user_name else None,
                    password=pwd if pwd else None,
                    ssl=getattr(self, f'{prefix}_ssl'),
                    db=getattr(self, f'{prefix}_db'),
                )
            else:
                logger.error(f"Cannot create a valid Redis config with no host and no port: {prefix}")
        return redis

    def all_files(self) -> dict[str, FileConfig]:
        files = {}
        prefixes = set()
        for field_name, field in self.__class__.model_fields.items():
            if field_name.startswith('file') and field_name.endswith('_id'):
                prefix = field_name.replace('_id', '')
                prefixes.add(prefix)

        for prefix in prefixes:
            file_id = getattr(self, f'{prefix}_id')
            files[file_id] = FileConfig(
                id=file_id,
                file_storage_path=getattr(self, f'{prefix}_storage_path'),
                file_format=getattr(self, f'{prefix}_format'),
            )
        return files


settings = Settings()


if __name__ == '__main__':
    print(settings.all_databases())
    print(settings.all_redis())