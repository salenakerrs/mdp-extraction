"""Config Mapping module."""
# import: standard
import os
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Optional
from typing import Type

# import: external
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class LocalLocation(BaseModel):
    """Pydantic class for local location's parameters."""

    filepath: str


class ADLSLocation(BaseModel):
    """Pydantic class for ADLS location's parameters."""

    account_name: str
    container_name: str
    sas_token: str
    filepath: str


class DataSourceSetting(BaseModel):
    """Represents the base configuration for connecting to data source.

    Args:
        dbtype (str): The database type. (oracledb, sqlserver, db2, mariadb, mongodb)
        username (str): The username used for authentication.
        password (str): The password used for authentication.
        database (str): The name of the database to connect to.
        server (str): The hostname or IP address of the database server.
        port (int): The port number of the database server.
        timeout (Optional[int]): The connection timeout in seconds. Default to 180.
    """

    dbtype: str
    username: str
    password: str
    database: str
    server: str = ""
    port: Optional[int] = 8080
    timeout: Optional[int] = 180


class SqlServerSetting(DataSourceSetting):
    """Represents the configuration for connecting to Microsoft SQL Server."""

    pass


class OracleDBSetting(DataSourceSetting):
    """Represents the configuration for connecting to Oracle Database."""

    pass


class DB2Setting(DataSourceSetting):
    """Represents the configuration for connecting to IBM DB2 Database.

    Args:
        schemaname (Optional[str]): The name of the database schema to connect to.
        securitymechanism (Optional[int]): The security mechanism to use for the connection.
    """

    schemaname: Optional[str] = None
    securitymechanism: Optional[int] = None


class MariaDBSetting(DataSourceSetting):
    """Represents the configuration for connecting to MariaDB Database."""

    pass


class MongoDBSetting(DataSourceSetting):
    """Represents the configuration for connecting to MongoDB Database."""

    collection: Optional[str] = None


DB_TYPE_MAPPING: Dict[str, Type[DataSourceSetting]] = {
    "sqlserver": SqlServerSetting,
    "oracledb": OracleDBSetting,
    "db2": DB2Setting,
    "mariadb": MariaDBSetting,
    "mongodb": MongoDBSetting,
    "mongodbsrv": MongoDBSetting,
}


class DecryptorSetting(BaseModel):
    """Represents the base configuration for decrypted data source.

    Args:
        pass_enc (Optional[str]): The passphrase for key file encryted in base64.
        key_file_path (Optional[str]): The private key file location.
        passphrase (Optional[str]): Path to the PGP private key file.
    """

    pass_enc: Optional[str] = None
    key_file_path: Optional[str] = None
    passphrase: Optional[str] = None


class PgpDecryptorSetting(DecryptorSetting):
    """Represents the base configuration for decrypted data source."""

    pass


class GpgDecryptorSetting(DecryptorSetting):
    """Represents the base configuration for decrypted data source."""

    pass


DECRYPTOR_TYPE_MAPPING: Dict[str, Type[DecryptorSetting]] = {
    "pgp": PgpDecryptorSetting,
    "gpg": GpgDecryptorSetting,
}


class EnvSettings(BaseSettings, extra="allow"):  # type: ignore[call-arg]
    """Set configurations from .env file using the pydantic models."""

    model_config = SettingsConfigDict(env_file=".env", extra="allow", env_nested_delimiter="__")

    mdp_inbnd: Optional[ADLSLocation] = None
    oih_inbnd: Optional[ADLSLocation] = None
    local_storage: LocalLocation
    connection_info: Optional[Dict[str, Any]] = None
    pgp_private_key: Optional[Dict[str, Any]] = None
    gpg_private_key: Optional[Dict[str, Any]] = None


@dataclass
class ConfigMapping:
    """Data class for mapping Jinja Template."""

    pos_dt: str
    ptn_yyyy: str = field(init=False)
    ptn_mm: str = field(init=False)
    ptn_dd: str = field(init=False)
    env: str = field(init=False)
    ptn_qtr: str = field(init=False)
    ptn_yyyy_be: str = field(init=False)

    def __post_init__(self) -> None:
        """Post init method to setup mapping values."""
        date_obj = datetime.strptime(self.pos_dt, "%Y-%m-%d")
        self.ptn_yyyy = date_obj.strftime("%Y")
        self.ptn_mm = date_obj.strftime("%m")
        self.ptn_dd = date_obj.strftime("%d")
        self.env = os.getenv("ENVIRONMENT", "dev")
        self.ptn_qtr = str((date_obj.month - 1) // 3 + 1).zfill(2)
        self.ptn_yyyy_be = str(date_obj.year + 543)
