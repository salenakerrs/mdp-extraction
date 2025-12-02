"""Module for extracting source data using ODBC connections."""
# import: standard
import csv
import logging
import os
import pathlib
from copy import deepcopy
from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union
from urllib.parse import quote_plus

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import DB_TYPE_MAPPING
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import ConfigMapping
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import DataSourceSetting
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    BaseDataExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    DataFileInformation,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    generate_data_file_info,
)
from mdp.framework.mdp_extraction_framework.utility.common_function import read_file
from mdp.framework.mdp_extraction_framework.utility.common_function import remove_files
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import render_template

# import: external
from pydantic import BaseModel
from pydantic import model_validator
from sqlalchemy import Engine
from sqlalchemy import Row
from sqlalchemy import create_engine
from sqlalchemy import text

POOL_TIMEOUT = 10800
POOL_RECYCLE = 1500


class DataExtractorNoRecordError(ValueError):
    """Custom error for Data Extractor with zero record."""

    pass


class FileNameFormatTaskConfigModel(BaseModel):
    """Configuration model for defining file name format with sequence numbers and other
    file name if any.

    Attributes:
        base_file_name (str): The base name of the file.
        date_suffix (str): The date suffix to be appended to the file name.
        part_suffix (str): The part suffix to be appended to the file name.
    """

    base_file_name: str
    date_suffix: str
    part_suffix: Optional[str] = "part"


class WritePropertyConfigModel(BaseModel):
    """Configuration model for Python CSV Writer properties.

    Attributes:
        header (bool): A boolean indicating whether to include a header row in the CSV file.
        format (Optional[str]): File format.
        option (Dict): A dictionary containing options for CSV writing.
                       Ref: https://docs.python.org/3/library/csv.html#dialects-and-formatting-parameters
                       eg: {
                                "delimiter": "|",
                                "quotechar": "\"",
                                "quoting": "QUOTE_ALL",
                                "escapechar": "\\"
                            }
    """

    header: bool = True
    format: Optional[str] = ""  # TODO: unused, can remove after update config.
    option: dict = {}


class FileOptionConfigModel(BaseModel, extra="allow"):  # type: ignore[call-arg]
    """Configuration model for file options during file opening.

    Attributes:
        mode (str): The mode in which the file is opened. Default is 'a' (append).
                    Ref: https://docs.python.org/3/library/functions.html#open
        newline (str): The string that represents the newline character(s). Default is universal newlines mode.
        encoding (str): The encoding used to read or write the file. Default is 'utf-8'.
    """

    mode: str = "a"
    newline: str = ""
    encoding: str = "utf-8"


@dataclass
class DBConnectionStrings:
    """Class for generating SQL Server connection strings based on the provided
    DataSourceSetting.

    Attributes:
        connection_info (Any): The connection information for the database.
    """

    connection_info: Any

    @property
    def sqlserver(self):
        """Generates and returns the SQL Server connection string.

        Returns:
            str: The generated SQL Server connection string.
        """
        return (
            "mssql+pyodbc:///?odbc_connect="
            + "Driver={ODBC Driver 18 for SQL Server};"
            + f"Server=tcp:{self.connection_info.server},{self.connection_info.port};"
            + f"Database={self.connection_info.database};"
            + f"Uid={self.connection_info.username};"
            + f"Pwd={self.connection_info.password};"
            + "Encrypt=yes;"
            + "TrustServerCertificate=yes;"
            + f"Connection Timeout={self.connection_info.timeout};"
        )

    @property
    def oracledb(self):
        """Generates and returns the Oracle connection string.

        Returns:
            str: The generated Oracle connection string.
        """
        return (
            "oracle+cx_oracle://"
            + f"{self.connection_info.username}:{self.connection_info.password}"
            + f"@{self.connection_info.server}:"
            + f"{self.connection_info.port}/"
            + f"?service_name={self.connection_info.database}"
            + "&encoding=UTF-8"
            + "&nencoding=UTF-8"
        )

    @property
    def db2(self):
        """Generates and returns the DB2 connection string.

        Returns:
            str: The generated DB2 connection string.
        """
        return (
            "ibm_db_sa://"
            + f"{self.connection_info.username}:{quote_plus(self.connection_info.password)}"
            + f"@{self.connection_info.server}:"
            + f"{self.connection_info.port}/"
            + f"{self.connection_info.database}"
            + f";currentSchema={self.connection_info.schemaname}"
            + f";securityMechanism={self.connection_info.securitymechanism}"
        )

    # TODO: Test connect with Maria DB, May need to update
    @property
    def mariadb(self):
        """Generates and returns the MariaDB connection string.

        Returns:
            str: The generated MariaDB connection string.
        """
        return (
            "mariadb+pyodbc:///?odbc_connect="
            + "DRIVER={MariaDB};"
            + f"SERVER={self.connection_info.server};"
            + f"DATABASE={self.connection_info.database};"
            + f"UID={self.connection_info.username};"
            + f"PWD={self.connection_info.password};"
            + f"PORT={self.connection_info.port};"
            + f"Connection Timeout={self.connection_info.timeout};"
        )

    # TODO: Test connect with MySQL, may need to update
    @property
    def mysql(self):
        """Generates and returns the MySQL connection string.

        Returns:
            str: The generated MySQL connection string.
        """
        return (
            "mysql+pyodbc:///?odbc_connect="
            + "DRIVER={MySQL ODBC 9.0 Unicode Driver};"
            + f"SERVER={self.connection_info.server};"
            + f"DATABASE={self.connection_info.database};"
            + f"UID={self.connection_info.username};"
            + f"PWD={self.connection_info.password};"
            + f"PORT={self.connection_info.port};"
            + f"Connection Timeout={self.connection_info.timeout};"
        )


class OdbcDatabaseConnector:
    """Class to establishes and manages a connection to a database using ODBC."""

    def __init__(self, connection_info: DataSourceSetting) -> None:
        """Initializes the DatabaseConnector with connection information.

        Args:
            connection_info (DataSourceSetting): The connection parameters.
        """
        self.connection_info = connection_info
        self.engine = self._connect_to_database()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _connect_to_database(self) -> Engine:
        """Creates an SQLAlchemy engine for the odbc database connection using the
        configuration.

        Returns:
            Engine: The created engine object.
        """
        connection_string = DBConnectionStrings(self.connection_info).__getattribute__(
            self.connection_info.dbtype.lower()
        )
        engine = create_engine(
            connection_string,
            pool_timeout=POOL_TIMEOUT,
            pool_recycle=POOL_RECYCLE,
            pool_pre_ping=True,
            echo=True,
            echo_pool="debug",
        )
        return engine

    def save_data(
        self,
        query: str,
        base_filename: str,
        file_extension: str,
        write_property: WritePropertyConfigModel,
        file_option: FileOptionConfigModel,
        header_col: list,
    ) -> tuple[str, Sequence[Row[Any]]]:
        """Execute a SQL query and save the results to a CSV file.

        Args:
            query (str): The SQL query to execute.
            base_filename (str): The name of the CSV file to create.
            file_extension (str): The name of the CSV file to create.
            write_property (WritePropertyConfigModel): Options for CSV writing.
            file_option (FileOptionConfigModel): File option for opening file.
            header_col (list): Sequence of strings representing the column headers.

        Returns:
            tuple[str, Sequence[Row[Any]]]: generated file name, and rows of data
        """
        with self.engine.connect() as connection:
            selected_data = connection.execute(text(query)).fetchall()
            file_name = f"{base_filename}.{file_extension}"
            self.write_to_csv(file_name, header_col, selected_data, write_property, file_option)
        self.engine.dispose()
        return file_name, selected_data

    def replaced_full_file_name(self, full_file_name: str, value: Union[int, str]) -> str:
        """Replace the 'part_number' variable in the full file name with the provided
        value.

        Args:
            full_file_name (str): The full file name containing the 'part_number' variable.
            value (Union[int, str]): The value to replace the 'part_number' variable in the file name.

        Returns:
            str: The full file name with the 'part_number' variable replaced by the specified value.
        """
        mapping = {"part_number": value}
        replaced_full_file_name = render_template(content=full_file_name, mapping=mapping)
        return replaced_full_file_name

    def search_existing_file(self, full_file_name: str, dir_name: str) -> list[str]:
        """Search for existing files in the specified directory path that match the file
        pattern.

        Args:
            full_file_name (str): The full file name containing the 'part_number' variable.
            dir_name (str): The directory path to search for files.

        Returns:
            list[str]: A list of matched file paths.
        """
        matched_files: list = []
        # Create a file pattern for seaching existing files, replace placeholder `part` with wild card
        file_pattern = self.replaced_full_file_name(full_file_name=full_file_name, value="*")
        # Append '.*' to the file pattern to match both delimited and ctl files
        file_pattern_with_ext_wildcard = f"{file_pattern}.*"
        self.logger.info(f"Searching file with pattern {file_pattern_with_ext_wildcard}")
        all_matched_paths = [
            path.as_posix() for path in pathlib.Path(dir_name).glob(file_pattern_with_ext_wildcard)
        ]
        matched_files.extend(all_matched_paths)
        self.logger.info(f"Searched files: {matched_files}")
        return matched_files

    def save_data_in_batches(
        self,
        query: str,
        base_filename: str,
        batch_size: int,
        file_extension: str,
        write_property: WritePropertyConfigModel,
        file_option: FileOptionConfigModel,
        allow_zero_record: bool,
    ) -> List[DataFileInformation]:
        """Executes the provided SQL query, fetches data in batches, and saves them to
        multiple CSV files with suffixes indicating part numbers, and save to CSV files.

        Args:
            query (str): The SQL query to execute.
            base_filename (str): Base filename for the output files (e.g., "data").
            batch_size (int): Number of rows to fetch in each batch. Defaults to 10000.
            file_extension (str): File extension. Defaults to csv.
            write_property (WritePropertyConfigModel): Options for CSV writing.
            file_option (FileOptionConfigModel): File option for opening file.
            allow_zero_record (dict): Flag to allow write file with 0 record.

        Returns:
            List[DataFileInformation]: generated file names
        """
        file_infos = []
        record_exist = False
        with self.engine.connect() as connection:
            with connection.execution_options(yield_per=batch_size).execute(text(query)) as result:
                header_col = result.keys()._keys
                part_number = 0
                for partition in result.partitions(batch_size):
                    if partition:
                        rendered_base_name = self.replaced_full_file_name(
                            base_filename, part_number
                        )
                        file_name = f"{rendered_base_name}.{file_extension}"
                        # partition is an iterable that will be at most 100 items
                        record_exist = True
                        self.write_to_csv(
                            file_name, header_col, partition, write_property, file_option
                        )
                        file_info = generate_data_file_info(file_name)
                        file_infos.append(file_info)
                        part_number += 1
                if not record_exist and allow_zero_record:
                    rendered_base_name = self.replaced_full_file_name(base_filename, part_number)
                    file_name = f"{rendered_base_name}.{file_extension}"
                    self.logger.info(f"Writing {file_name} with zero record.")
                    self.write_to_csv(file_name, header_col, [], write_property, file_option)
                    file_info = generate_data_file_info(file_name)
                    file_infos.append(file_info)
                elif not record_exist and not allow_zero_record:
                    message = "Found zero record. No writing to file as the allow_zero_record flag is set to False."
                    self.logger.info(message)
                    raise DataExtractorNoRecordError(message)
        self.engine.dispose()
        return file_infos

    def write_to_csv(
        self,
        file_name: str,
        header_col: Sequence[str],
        data: Sequence[tuple],
        write_property: WritePropertyConfigModel,
        file_option: FileOptionConfigModel,
    ):
        """Write data to a CSV file.

        Args:
            file_name (str): The name of the CSV file to create.
            header_col (Sequence[str]): Sequence of strings representing the column headers.
            data (Sequence[tuple]): A list of rows containing the data to be written.
            write_property (WritePropertyConfigModel): Options for CSV writing.
            file_option (FileOptionConfigModel): File option for opening file.
        """
        self.logger.info(f"Writing {file_name}.")
        option = deepcopy(write_property.option)
        quoting = option.get("quoting")
        if quoting:
            option["quoting"] = csv.__getattribute__(quoting)
        file_exists = os.path.exists(file_name)
        with open(file_name, **file_option.model_dump()) as csvfile:
            writer = csv.writer(csvfile, **option)
            if write_property.header and not file_exists:
                writer.writerow(header_col)
            writer.writerows(data)
        self.logger.info(f"Write {file_name} completed.")


class OdbcDataExtractorTaskConfigModel(BaseModel):
    """Configuration model for source data and query.

    Attributes:
        connection_name (str): The name of the source database specified in the environment file.
        query (Optional[str]): An SQL query statement to retrieve data from the source table.
                               Either one of 'query' or 'sql_file_path' is required.
        sql_file_path (Optional[str]): An SQL query file path to retrieve data from the source table.
        extract_file_location (str): The directory for extracted file.
        batch_size (Optional[int]): Number of rows to fetch in each batch. Defaults to 10000000.
        allow_zero_record (Optional[bool]): Flag to create a file if 0 record. Defaults to True.
        file_name_format (FileNameFormatTaskConfigModel): Configuration for the file name format.
        full_file_name (str): Full path and name for the output CSV file.
        file_extension (Optional[str]): File extension for the output CSV file. Defaults to "csv".
        file_option (Optional[FileOptionConfigModel]): File options during file opening. Defaults to default values of config model.
        write_property (WritePropertyConfigModel): Write Property for CSV writing.
    """

    connection_name: str
    query: Optional[str] = None
    sql_file_path: Optional[str] = None
    extract_file_location: str
    batch_size: Optional[int] = 10000000
    allow_zero_record: Optional[bool] = True
    file_name_format: FileNameFormatTaskConfigModel
    full_file_name: str
    file_extension: Optional[str] = "csv"
    file_option: Optional[FileOptionConfigModel] = FileOptionConfigModel()
    write_property: WritePropertyConfigModel

    @model_validator(mode="after")
    def verify_query_exist(self):
        """Validate if either one of query or sql_file_path is speicified.

        Raises:
            ValueError: If both 'query' and 'sql_file_path' are not specified, or both are specified.
        """
        if self.query is None and self.sql_file_path is None:
            raise ValueError("Either 'query' or 'sql_file_path' is required.")
        elif self.query and self.sql_file_path:
            raise ValueError("Expect only one input 'query' or 'sql_file_path'.")
        return self


class OdbcDataExtractorTask(BaseDataExtractorTask):
    """Class for extracting source data using ODBC."""

    parameter_config_model = OdbcDataExtractorTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
    ):
        """Initializes a SourceDataExtractorTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
        """
        super().__init__(module_config, job_parameters)
        self.full_file_name = render_template(
            content=self.module_config.full_file_name,
            mapping=vars(self.module_config.file_name_format),
        )
        self.full_file_path = self.module_config.extract_file_location + self.full_file_name

    def get_query(self) -> str:
        """Method to get query from sql script or query statement in config.

        Returns:
            str: query statement
        """
        if self.module_config.sql_file_path:
            template_query = read_file(self.module_config.sql_file_path)
            query = render_template(
                template_query, asdict(ConfigMapping(self.job_parameters.pos_dt))
            )
        else:
            query = self.module_config.query
        return query

    def execute(self) -> List[DataFileInformation]:
        """Executes the source data extraction process.

        Connects to the database, fetches data using specified queries.

        Returns:
            List[DataFileInformation]: generated file names
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")

        # Get Connection Info
        env_file = EnvSettings()
        connection_name = self.module_config.connection_name
        connection_data = env_file.connection_info.get(connection_name)

        dbtype = connection_data.get("dbtype")
        if not dbtype:
            raise ValueError(f"dbtype not defined for connection '{connection_name}'")

        settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
        if not settings_class:
            raise ValueError(f"Unsupported dbtype '{dbtype}' for connection '{connection_name}'")

        connection_info = settings_class(**connection_data)

        # Connect to ODBC Database
        connector = OdbcDatabaseConnector(connection_info=connection_info)

        # read sql file if exists
        query = self.get_query()

        # Removed existing leftover files
        self.logger.info(
            f"Removing existing leftover files from directory: {self.module_config.extract_file_location}, file name format: {self.full_file_name}"
        )
        leftover_files = connector.search_existing_file(
            self.full_file_name, self.module_config.extract_file_location
        )
        remove_files(leftover_files)
        self.logger.info(f"Removed existing leftover files {leftover_files}")

        # Extract and write data
        self.logger.info(
            f"Extracting Data from source {self.module_config.connection_name} using query: {query}"
        )
        file_infos = connector.save_data_in_batches(
            query,
            self.full_file_path,
            self.module_config.batch_size,
            self.module_config.file_extension,
            self.module_config.write_property,
            self.module_config.file_option,
            self.module_config.allow_zero_record,
        )

        self.logger.info(f"Execution of {self.__class__.__name__} completed.")

        return file_infos
