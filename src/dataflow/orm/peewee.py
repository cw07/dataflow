from peewee import *
from dataclasses import fields
from typing import get_origin, get_args


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