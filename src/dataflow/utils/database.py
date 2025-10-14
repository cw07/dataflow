from typing import Any

from urllib.parse import quote_plus


class DatabaseConnectionBuilder:
    """
    Utility class for building database connection strings and parameters.
    Supports SQLAlchemy and Peewee ORM backends.
    """

    @staticmethod
    def build_sqlalchemy_connection_string(
        db_type: str,
        host: str,
        database: str,
        username: str = "",
        password: str = "",
        driver: str = "",
        trusted_connection: bool = False,
        autocommit: bool = False,
    ) -> str:
        """
        Build a SQLAlchemy-compatible connection string based on database type.

        Args:
            db_type: Database type (e.g., 'mysql', 'postgresql', 'sqlite').
            host: Host address.
            database: Database name.
            username: Username (optional).
            password: Password (optional).
            driver: Database driver (e.g., 'ODBC Driver 17 for SQL Server').
            trusted_connection: Whether to use Windows Authentication.
            autocommit: Whether to enable autocommit mode.

        Returns:
            A SQLAlchemy connection string.

        Raises:
            ValueError: If db_type is not supported.
        """
        db_type = db_type.lower()

        if db_type == "mssql":
            return DatabaseConnectionBuilder._build_sqlalchemy_mssql(
                host, database, username, password, driver, trusted_connection, autocommit
            )
        elif db_type == "postgresql":
            return DatabaseConnectionBuilder._build_sqlalchemy_postgresql(
                host, database, username, password, trusted_connection
            )
        elif db_type == "mysql":
            return DatabaseConnectionBuilder._build_sqlalchemy_mysql(
                host, database, username, password, autocommit
            )
        elif db_type == "sqlite":
            return DatabaseConnectionBuilder._build_sqlalchemy_sqlite(database)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    @staticmethod
    def build_peewee_connection_params(
        db_type: str,
        host: str,
        database: str,
        username: str = "",
        password: str = "",
        driver: str = "",
        trusted_connection: bool = False,
        autocommit: bool = False,
    ) -> dict:
        """
        Build a dictionary of connection parameters for Peewee ORM.

        Args:
            db_type: Database type (e.g., 'postgresql', 'mysql', 'sqlite').
            host: Host address.
            database: Database name.
            username: Username (optional).
            password: Password (optional).
            driver: Database driver (for MSSQL).
            trusted_connection: Whether to use Windows Authentication.
            autocommit: Whether to enable autocommit mode.

        Returns:
            Dictionary of connection parameters.

        Raises:
            ValueError: If db_type is not supported.
        """
        db_type = db_type.lower()

        if db_type == "postgresql":
            return DatabaseConnectionBuilder._build_peewee_postgresql(
                host, database, username, password
            )
        elif db_type == "mysql":
            return DatabaseConnectionBuilder._build_peewee_mysql(
                host, database, username, password, autocommit
            )
        elif db_type == "sqlite":
            return DatabaseConnectionBuilder._build_peewee_sqlite(database)
        else:
            raise ValueError(f"Unsupported database type for Peewee: {db_type}")

    # === SQLAlchemy Builders ===

    @staticmethod
    def _build_sqlalchemy_mssql(
        host: str,
        database: str,
        username: str,
        password: str,
        driver: str,
        trusted_connection: bool,
        autocommit: bool,
    ) -> str:
        """
        Build SQLAlchemy connection string for MSSQL.
        """
        driver_encoded = quote_plus(driver) if driver else "ODBC+Driver+17+for+SQL+Server"

        if trusted_connection:
            conn_str = (
                f"mssql+pyodbc:///{host}/{database}"
                f"?driver={driver_encoded}"
                f"&trusted_connection=yes"
            )
        else:
            username_encoded = quote_plus(username)
            password_encoded = quote_plus(password)
            conn_str = (
                f"mssql+pyodbc:///{username_encoded}:{password_encoded}@{host}/{database}"
                f"?driver={driver_encoded}"
            )

        if autocommit:
            conn_str += "&autocommit=true"

        return conn_str

    @staticmethod
    def _build_sqlalchemy_postgresql(
            host: str,
            database: str,
            username: str,
            password: str,
            trusted_connection: bool = False,
            autocommit: bool = False,
    ) -> str:
        """
        Build SQLAlchemy connection string for PostgreSQL.
        """
        if trusted_connection:
            # PostgreSQL trust authentication (no password required)
            username_encoded = quote_plus(username) if username else "postgres"
            conn_str = f"postgresql+psycopg2:///{username_encoded}@{host}/{database}"
        else:
            # Standard username/password authentication
            username_encoded = quote_plus(username)
            password_encoded = quote_plus(password)
            conn_str = f"postgresql+psycopg2://{username_encoded}:{password_encoded}@{host}/{database}"

        # PostgreSQL supports autocommit as a connection parameter
        if autocommit:
            conn_str += "?autocommit=true"

        return conn_str

    @staticmethod
    def _build_sqlalchemy_mysql(
            host: str,
            database: str,
            username: str,
            password: str,
            trusted_connection: bool = False,
            autocommit: bool = False,
    ) -> str:
        """
        Build SQLAlchemy connection string for MySQL.
        """
        if trusted_connection:
            # MySQL socket authentication (Unix/Linux only)
            # Uses current OS user, no password needed
            username_encoded = quote_plus(username) if username else "root"
            conn_str = f"mysql+pymysql:///{username_encoded}@{host}/{database}"
        else:
            # Standard username/password authentication
            username_encoded = quote_plus(username)
            password_encoded = quote_plus(password)
            conn_str = f"mysql+pymysql://{username_encoded}:{password_encoded}@{host}/{database}"

        # Add autocommit parameter
        separator = "&" if "?" in conn_str else "?"
        if autocommit:
            conn_str += f"{separator}autocommit=true"

        return conn_str

    @staticmethod
    def _build_sqlalchemy_sqlite(database: str) -> str:
        """
        Build SQLAlchemy connection string for SQLite.
        """
        return f"sqlite:///{database}"

    # Peeew builders
    @staticmethod
    def _build_peewee_postgresql(
            host: str,
            database: str,
            username: str,
            password: str,
    ) -> dict:
        """
        Build Peewee connection parameters for PostgreSQL.
        """
        return {
            'host': host,
            'database': database,
            'user': username,
            'password': password,
        }

    @staticmethod
    def _build_peewee_mysql(
            host: str,
            database: str,
            username: str,
            password: str,
            autocommit: bool = False,
    ) -> dict:
        """
        Build Peewee connection parameters for MySQL.
        """
        params: dict[str, Any] = {
            'host': host,
            'database': database,
            'user': username,
            'password': password,
        }

        if autocommit:
            params['autocommit'] = True

        return params

    @staticmethod
    def _build_peewee_sqlite(database: str) -> dict:
        """
        Build Peewee connection parameters for SQLite.
        """
        return {
            'database': database,
        }