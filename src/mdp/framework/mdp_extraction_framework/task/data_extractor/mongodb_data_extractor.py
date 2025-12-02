"""Module for extracting source data using ODBC connections."""
# import: standard
import csv
import json
import logging
import os
import pathlib
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union
from urllib.parse import quote_plus

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import DB_TYPE_MAPPING
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
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    DataExtractorNoRecordError,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    FileNameFormatTaskConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    FileOptionConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    WritePropertyConfigModel,
)
from mdp.framework.mdp_extraction_framework.utility.common_function import read_file
from mdp.framework.mdp_extraction_framework.utility.common_function import remove_files
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import render_template

# import: external
from bson import Binary
from bson import DBRef
from bson import ObjectId
from bson import Timestamp
from pydantic import BaseModel
from pydantic import model_validator
from pymongo import MongoClient


@dataclass
class MongoDBConnectionStrings:
    """Class for generating SQL Server connection strings based on the provided
    DataSourceSetting.

    Attributes:
        connection_info (Any): The connection information for the database.
    """

    connection_info: Any

    @property
    def mongodb(self) -> str:
        """Generates a connection string for MongoDB.

        Returns:
            str: The connection string for MongoDB.
        """

        connection_string = (
            f"mongodb://{quote_plus(self.connection_info.username)}:"
            f"{quote_plus(self.connection_info.password)}@"
            f"{self.connection_info.server}"
        )

        return connection_string

    @property
    def mongodbsrv(self) -> str:
        """Generates a connection string for MongoDB.

        Returns:
            str: The connection string for MongoDB.
        """

        connection_string = (
            f"mongodb+srv://{quote_plus(self.connection_info.username)}:"
            f"{quote_plus(self.connection_info.password)}@"
            f"{self.connection_info.server}"
        )

        return connection_string


class MongoDatabaseConnector:
    """Class to establishes and manages a connection to a database using ODBC."""

    def __init__(self, connection_info: DataSourceSetting) -> None:
        """Initializes the DatabaseConnector with connection information.

        Args:
            connection_info (DataSourceSetting): The connection parameters.
        """
        self.connection_info = connection_info
        self.collection = self._connect_to_database()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _json_object_hook(self, dct):
        """Convert datetime strings in ISO format to datetime objects where
        applicable."""
        for key, value in dct.items():
            if isinstance(value, str):
                try:
                    if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z", value):
                        dct[key] = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    pass
            elif isinstance(value, dict):
                dct[key] = self._json_object_hook(value)
            elif isinstance(value, list):
                dct[key] = [
                    self._json_object_hook(item) if isinstance(item, dict) else item
                    for item in value
                ]
        return dct

    def json_serialisable(self, doc: Any) -> Any:
        """Recursively convert BSON types (ObjectId, Binary, Timestamp, datetime, etc.)
        and other non-serialisable types in a document to JSON-compatible types.

        Args:
            doc (Any): The document to convert.

        Returns:
            Any: The converted document.
        """
        if isinstance(doc, (ObjectId, Timestamp)):
            return str(doc)
        elif isinstance(doc, (Binary, bytes)):
            return doc.decode("utf-8", errors="ignore")
        elif isinstance(doc, DBRef):
            return {"$ref": doc.collection, "$id": str(doc.id)}
        elif isinstance(doc, datetime):
            return doc.isoformat()
        elif isinstance(doc, dict):
            return {key: self.json_serialisable(value) for key, value in doc.items()}
        elif isinstance(doc, list):
            return [self.json_serialisable(item) for item in doc]
        else:
            return doc

    def _connect_to_database(self) -> Any:
        """Establishes a connection to the database.

        Returns:
            Any: The database collection object.
        """
        connection_string = MongoDBConnectionStrings(self.connection_info).__getattribute__(
            self.connection_info.dbtype.lower()
        )
        client = MongoClient(connection_string)
        database = client[f"{self.connection_info.database}"]
        collection = database[f"{self.connection_info.collection}"]
        return collection

    def save_data(
        self,
        query: str,
        base_filename: str,
        file_extension: str,
        write_property: WritePropertyConfigModel,
        file_option: FileOptionConfigModel,
        header_col: list,
    ) -> tuple[str, list]:
        """Execute a MongoDB query and save the results to a file.

        Args:
            query (str): The MongoDB query to execute.
            base_filename (str): The name of the file to create.
            file_extension (str): The file extension for the output file.
            write_property (WritePropertyConfigModel): Options for file writing.
            file_option (FileOptionConfigModel): File option for opening file.
            header_col (list): Sequence of strings representing the column headers.

        Returns:
            tuple[str, list]: The name of the file created and the selected data.
        """
        try:
            query_dict = json.loads(query.strip(), object_hook=self._json_object_hook)

        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Invalid MongoDB query string, failed to parse query string: {e}")
            raise ValueError(f"Invalid MongoDB query string: {query}")

        selected_data = list(self.collection.aggregate(query_dict, allowDiskUse=True))
        file_name = f"{base_filename}.{file_extension}"
        if file_extension == "json":
            self.write_to_json(selected_data, file_name)
        else:
            self.write_to_csv(file_name, header_col, selected_data, write_property, file_option)
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
        """Executes the provided MongoDB query, fetches data in batches, and saves them
        to multiple files with suffixes indicating part numbers.

        Args:
            query (str): The MongoDB query to execute in JSON format.
            base_filename (str): Base filename for the output files (e.g., "data").
            batch_size (int): Number of rows to fetch in each batch. Defaults to 10000.
            file_extension (str): File extension (e.g., csv).
            write_property (WritePropertyConfigModel): Options for file writing.
            file_option (FileOptionConfigModel): File option for opening file.
            allow_zero_record (bool): Flag to allow writing a file with 0 records.

        Returns:
            List[DataFileInformation]: List of file information for generated files.
        """
        try:
            query_dict = json.loads(query.strip(), object_hook=self._json_object_hook)

        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Invalid MongoDB query string, failed to parse query string: {e}")
            raise ValueError(f"Invalid MongoDB query string: {query}")

        file_infos = []
        record_exist = False
        header_col = []
        seen_fields = set()
        batch_data = []
        part_number = 0

        try:
            if file_extension != "json":
                # Collect all headers, as they may vary between documents when build the header for text files
                cursor = self.collection.aggregate(query_dict, allowDiskUse=True).batch_size(
                    batch_size
                )
                for doc in cursor:
                    for key in doc.keys():
                        if key not in seen_fields:
                            seen_fields.add(key)
                            header_col.append(key)

            # Write data in batches
            cursor = self.collection.aggregate(query_dict, allowDiskUse=True).batch_size(batch_size)
            for doc in cursor:
                batch_data.append(doc)

                # Write data when the batch reaches the specified size
                if len(batch_data) >= batch_size:
                    rendered_base_name = self.replaced_full_file_name(base_filename, part_number)
                    file_name = f"{rendered_base_name}.{file_extension}"

                    if file_extension == "json":
                        self.write_to_json(batch_data, file_name)
                    else:
                        self.write_to_csv(
                            file_name, header_col, batch_data, write_property, file_option
                        )

                    file_infos.append(generate_data_file_info(file_name))
                    record_exist = True
                    part_number += 1
                    batch_data.clear()  # Clear the batch for the next partition

            # Write remaining data after processing all documents
            if batch_data:
                rendered_base_name = self.replaced_full_file_name(base_filename, part_number)
                file_name = f"{rendered_base_name}.{file_extension}"

                if file_extension == "json":
                    self.write_to_json(batch_data, file_name)
                else:
                    self.write_to_csv(
                        file_name, header_col, batch_data, write_property, file_option
                    )

                file_infos.append(generate_data_file_info(file_name))
                record_exist = True

        except Exception as e:
            self.logger.error(f"Failed to fetch or write data: {e}")
            raise

        if not record_exist:
            if allow_zero_record:
                rendered_base_name = self.replaced_full_file_name(base_filename, 0)
                file_name = f"{rendered_base_name}.{file_extension}"
                self.write_to_csv(file_name, header_col or [], [], write_property, file_option)
                file_infos.append(generate_data_file_info(file_name))
            else:
                self.logger.error("No records found and 'allow_zero_record' is False.")
                raise DataExtractorNoRecordError("No records found.")

        return file_infos

    def write_to_csv(
        self,
        file_name: str,
        header_col: Sequence[str],
        data: Sequence[dict],
        write_property: WritePropertyConfigModel,
        file_option: FileOptionConfigModel,
    ):
        """Write data to a file.

        Args:
            file_name (str): The name of the file to create.
            header_col (Sequence[str]): Sequence of strings representing the column headers.
            data (Sequence[dict]): A list of rows containing the data to be written.
            write_property (WritePropertyConfigModel): Options for file writing.
            file_option (FileOptionConfigModel): File option for opening file.
        """
        self.logger.info(f"Writing {file_name}.")
        option = deepcopy(write_property.option)
        quoting = option.get("quoting")
        if quoting:
            option["quoting"] = csv.__getattribute__(quoting)

        try:
            file_exists = os.path.exists(file_name)
            with open(file_name, **file_option.model_dump()) as csvfile:
                writer = csv.writer(csvfile, **option)
                if write_property.header and not file_exists:
                    writer.writerow(header_col)
                for document in data:
                    writer.writerow([document.get(field, "") for field in header_col])
            self.logger.info(f"Write {file_name} completed.")
        except IOError as e:
            self.logger.error(f"Failed to write file {file_name}: {e}")
            raise

    def write_to_json(self, data: list, output_file: str):
        """Write data to a JSON file.

        Args:
            data (list): The data to write to the file.
            output_file (str): The name of the file to create.
        """
        try:
            serialisable_data = [self.json_serialisable(doc) for doc in data]
            with open(output_file, mode="w", encoding="utf-8") as jsonfile:
                json.dump(serialisable_data, jsonfile, ensure_ascii=False, indent=4)
            self.logger.info(f"Data exported successfully to {output_file}")
        except IOError as e:
            self.logger.error(f"Failed to write file {output_file}: {e}")
            raise


class MongoDataExtractorTaskConfigModel(BaseModel):
    """Configuration model for source data and query.

    Attributes:
        connection_name (str): The name of the source database specified in the environment file.
        query (Optional[str]): An SQL query statement to retrieve data from the source table.
                               Either one of 'query' or 'sql_file_path' is required.
        json_file_path (Optional[str]): An JSON query file path to retrieve data from the source table.
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
    json_file_path: Optional[str] = None
    extract_file_location: str
    batch_size: Optional[int] = 10000000
    allow_zero_record: Optional[bool] = True
    file_name_format: FileNameFormatTaskConfigModel
    full_file_name: str
    file_extension: Optional[str] = "csv"
    file_option: Optional[FileOptionConfigModel] = FileOptionConfigModel()
    write_property: Optional[WritePropertyConfigModel] = WritePropertyConfigModel()

    @model_validator(mode="after")
    def verify_query_exist(self):
        """Validate if either one of query or json_file_path is provided.

        Raises:
            ValueError: If both 'query' and 'json_file_path' are not specified.
        """
        if self.query is None and self.json_file_path is None:
            raise ValueError("Either 'query' or 'json_file_path' must be specified.")
        elif self.query and self.json_file_path:
            raise ValueError("Expect only one input 'query' or 'json_file_path'.")
        return self


class MongoDataExtractorTask(BaseDataExtractorTask):
    """Class for extracting source data using ODBC."""

    parameter_config_model = MongoDataExtractorTaskConfigModel

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
        """Method to get query from json file or query string.

        Returns:
            str: The query to execute
        """
        if self.module_config.json_file_path:
            template_query = read_file(self.module_config.json_file_path)
            try:
                query = json.loads(template_query)
                query = json.dumps(query)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                self.logger.error(f"Error reading or parsing JSON file: {e}")
                raise ValueError(f"Invalid JSON file or query: {self.module_config.json_file_path}")

            query = template_query.replace("{{pos_dt}}", self.job_parameters.pos_dt)

        else:
            query = self.module_config.query

        return query

    def execute(self) -> List[DataFileInformation]:
        """Executes the MongoDB data extraction task.

        Returns:
            List[DataFileInformation]: A list of DataFileInformation objects.
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

        if dbtype.lower() not in ["mongodb", "mongodbsrv"]:
            raise ValueError(f"Unsupported dbtype '{dbtype}' for connection '{connection_name}'")

        connection_info = settings_class(**connection_data)

        # Connect to database
        connector = MongoDatabaseConnector(connection_info)

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
