"""Module for extracting source data using ODBC connections."""
# import: standard
import json
from typing import Optional
from typing import Tuple

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import DB_TYPE_MAPPING
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.control_file_generator.base_control_file_generator import (
    BaseControlFileGeneratorTask,
)
from mdp.framework.mdp_extraction_framework.task.control_file_generator.odbc_control_file_generator import (
    FileNameFormatTaskConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.mongodb_data_extractor import (
    MongoDatabaseConnector,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    FileOptionConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    WritePropertyConfigModel,
)
from mdp.framework.mdp_extraction_framework.utility.common_function import read_file
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import render_template

# import: external
from pydantic import BaseModel
from pydantic import model_validator


class MongoControlFileGeneratorTaskConfigModel(BaseModel):
    """Configuration model for source data and query.

    Attributes:
        connection_name (str): The name of the source database specified in the environment file.
        query (str): An SQL query statement to retrieve data from the source table.
                     Either one of 'query' or 'sql_file_path' is required.
        json_file_path (Optional[str]): An SQL query file path to retrieve data from the source table.
        extract_file_location (str): The directory for extracted file.
        header (bool): Flag indicating whether the CSV file should have a header.
        header_columns (Optional[List]): List of column names if 'header' is True, else None.
        file_name_format (FileNameFormatTaskConfigModel): Configuration for the file name format.
        full_file_name (str): Full path and name for the output CSV file.
        file_extension (Optional[str]): File extension for the output ctl file. Defaults to "ctl".
        file_option (Optional[FileOptionConfigModel]): File options during file opening. Defaults to default values of config model.
        write_property (WritePropertyConfigModel): Write Property for CSV writing.
    """

    connection_name: str
    query: Optional[str] = None
    json_file_path: Optional[str] = None
    extract_file_location: str
    header: bool
    header_columns: Optional[list]
    file_name_format: FileNameFormatTaskConfigModel
    full_file_name: str
    file_extension: Optional[str] = "ctl"
    file_option: Optional[FileOptionConfigModel] = FileOptionConfigModel()
    write_property: WritePropertyConfigModel

    @model_validator(mode="after")
    def verify_header(self):
        """Validate header and header_columns consistency.

        Raises:
            ValueError: If 'header' is True and 'header_columns' is None.
        """
        if self.header is True and self.header_columns is None:
            raise ValueError("header_columns is required if header is true")
        return self

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


class MongoControlFileGeneratorTask(BaseControlFileGeneratorTask):
    """Class for extracting mongo source data."""

    parameter_config_model = MongoControlFileGeneratorTaskConfigModel

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
        self.full_file_name = self.module_config.extract_file_location + render_template(
            content=self.module_config.full_file_name,
            mapping=vars(self.module_config.file_name_format),
        )

    def execute(self) -> Tuple[str, str]:
        """Executes the source data extraction process.

        Connects to the database, fetches data using specified queries.

        Returns:
            Tuple[str, str]: file name, ctl file details (for logging)
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")

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

        # read json file if query is not provided
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

        # Extract and write data
        self.logger.info(
            f"Extracting Data from source {self.module_config.connection_name} using query: {query}"
        )
        file_name, data = connector.save_data(
            query,
            self.full_file_name,
            self.module_config.file_extension,
            self.module_config.write_property,
            self.module_config.file_option,
            self.module_config.header_columns,
        )

        # Get tuple data in pipe-delimited format for logging
        data_str = ""
        column_str = "|".join(self.module_config.header_columns)
        if data and len(data) > 0:
            data_str_list = []
            for document in data:
                data_str_list.append(
                    "|".join(
                        str(document.get(field, "")) for field in self.module_config.header_columns
                    )
                )
                data_str = "\n".join(data_str_list)

        ctl_data_str = f"{column_str}\n{data_str}"

        self.logger.info(f"Debugging: {ctl_data_str}")
        self.logger.info(f"Execution of {self.__class__.__name__} completed.")

        return file_name, ctl_data_str
