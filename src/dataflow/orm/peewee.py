from dataclasses import fields
from peewee import PostgresqlDatabase, MySQLDatabase, SqliteDatabase,  Model
from peewee import BigIntegerField, CharField, DoubleField, BooleanField
from typing import get_origin, get_args, Dict, Any, Type, List

from dataflow.config.settings import DatabaseConfig
from dataflow.orm.base import BaseORMAdapter, LazyDB


def create_peewee_model(instance, db_instance=None, table_name=None):
    """
    Dynamically convert a dataclass to a Peewee model.
    Excludes attributes starting with '__'.
    """

    # Mapping of Python types to Peewee fields
    type_mapping = {
        int: BigIntegerField,
        str: CharField,
        float: DoubleField,
        bool: BooleanField,
    }

    # Prepare model attributes
    model_attrs = {}

    for field in fields(instance):
        # Skip attributes starting with '__'
        if field.name.startswith('__'):
            continue

        field_type = field.type
        is_optional = False

        # Check if field is Optional (Union with None)
        if get_origin(field_type) is type(None) or get_origin(field_type).__name__ == 'UnionType':
            # For Optional[X], get the actual type
            args = get_args(field_type)
            if args:
                field_type = args[0] if args[0] is not type(None) else args[1]
                is_optional = True

        # Get the appropriate Peewee field class
        peewee_field_cls = type_mapping.get(field_type, CharField)

        # Create field instance
        if peewee_field_cls == CharField:
            model_attrs[field.name] = peewee_field_cls(max_length=255, null=is_optional)
        else:
            model_attrs[field.name] = peewee_field_cls(null=is_optional)

    # Add Meta class
    meta_attrs = {
        'database': db_instance,
        'table_name': table_name or instance.__name__.lower()
    }
    model_attrs['Meta'] = type('Meta', (), meta_attrs)

    # Create and return the model class
    model_cls = type(instance.__name__, (Model,), model_attrs)

    return model_cls


def peewee_database(db_type, **config):
    """
    Create database instance based on type.

    Args:
        db_type: 'postgres', 'mssql', 'sqlite', 'mysql'
        **config: Database connection parameters
    """
    db_types = {
        'postgres': PostgresqlDatabase,
        'sqlite': SqliteDatabase,
        'mysql': MySQLDatabase,
    }

    db_class = db_types.get(db_type.lower())
    if not db_class:
        raise ValueError(f"Unsupported database type: {db_type}")

    return db_class(**config)


class PeeweeDB(BaseORMAdapter):

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.engine = self.create_engine()

    def create_model(self, name: str, fields: Dict[str, Any]) -> Type:
        pass

    def create_table(self, models: List[Type]) -> None:
        pass

    def save_data(self, data_obj):
        pass

    def bulk_insert(self, model: Type, data: List[Dict]) -> None:
        pass

    def insert_one(self, model: Type, data: Dict) -> Any:
        pass

    def connect(self, config: Dict[str, Any]) -> None:
        pass

    def close(self) -> None:
        pass

    def create_engine(self):
        conn_param = self.config.connection_params()
        db_type_map = {
            "postgres": PostgresqlDatabase,
            "mysql": MySQLDatabase,
            "sqlite": SqliteDatabase
        }
        db_class = db_type_map.get(self.config.db_type)
        if not db_class:
            raise ValueError(f"Unsupported database type for peewee: {self.config.db_type}")
        db = db_class(**conn_param)
        return db


LazyPeewee = LazyDB(PeeweeDB)
