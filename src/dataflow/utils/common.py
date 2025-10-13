import os


def set_env_vars(argument: dict):
    for arg_name, arg_value in argument.items():
        if arg_name in os.environ:
            raise ValueError(
                f"Environment variable {arg_name} is already set: {os.environ[arg_name]} "
                f"Refusing to override to {arg_value}"
            )
        if arg_value is not None:
            os.environ[arg_name] = arg_value