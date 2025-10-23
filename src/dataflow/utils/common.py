import os
from enum import StrEnum


class ORM(StrEnum):
    SQLALCHEMY = 'sqlalchemy'
    PEEWEE = 'peewee'


class DataOutput(StrEnum):
    database = "database"
    redis = "redis"
    file = "file"

def set_env_vars(argument: dict):
    for arg_name, arg_value in argument.items():
        if arg_name in os.environ:
            raise ValueError(
                f"Environment variable {arg_name} is already set: {os.environ[arg_name]} "
                f"Refusing to override to {arg_value}"
            )
        if arg_value is not None:
            os.environ[arg_name] = arg_value


def parse_web_response(response) -> tuple:
    """
    Parse an HTTP response object assuming JSON content.

    Returns:
        tuple: (data, error_message)
               - If status code is 200: (parsed JSON object, None)
               - Otherwise: (None, error message string)
    """
    if response.status_code == 200:
        try:
            return response.json(), None
        except ValueError as e:
            # Handle case where response is 200 but not valid JSON
            return None, f"Failed to parse JSON: {e}"
    else:
        return None, f"HTTP {response.status_code}: {response.reason}"