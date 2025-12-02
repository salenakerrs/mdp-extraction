"""Source Data Extractor Test Module."""

# import: standard
import csv
import json
import os
import pathlib
from copy import deepcopy
from unittest.mock import patch

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import DB_TYPE_MAPPING
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import ConfigMapping
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    OdbcDatabaseConnector,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    OdbcDataExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    OdbcDataExtractorTaskConfigModel,
)
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import JSONReader

# import: external
import pytest
from pydantic import BaseModel

JOB_PARAMS = JobParameters(
    pos_dt="2023-10-31",
    config_file_path="",
)


class mock_model(BaseModel, extra="allow"):
    """A mock pydantic model."""

    pass


SQL_FILE_PATH = (
    "test/mdp/unit/mdp_extraction_framework/resources/task/data_extractor/extraction_query.sql"
)
parameters = {
    "connection_name": "ud",
    "sql_file_path": SQL_FILE_PATH,
    "extract_file_location": "/extraction/extracted_file/",
    "file_name_format": {
        "base_file_name": "CUSTOMER_TXN_DAILY",
        "date_suffix": "D{{ ptn_yyyy }}{{ ptn_mm }}{{ ptn_dd }}",
        "part_suffix": "part",
    },
    "full_file_name": "{{ base_file_name }}_{{ date_suffix }}_{{ part_suffix }}-{{ part_number }}",
    "batch_size": 10,
    "file_extension": "csv",
    "file_option": {"mode": "a", "newline": "", "encoding": "utf-8"},
    "write_property": {
        "format": "csv",
        "header": True,
        "option": {"delimiter": "|", "quotechar": '"', "quoting": "QUOTE_ALL", "escapechar": "\\"},
    },
}

MODULE_CONFIG = mock_model(
    module_name=OdbcDataExtractorTask, parameters=OdbcDataExtractorTaskConfigModel(**parameters)
)


def test_odbc_data_extractor_config_model():
    """Method to test checks if the OdbcDataExtractorTaskConfigModel can be instantiated
    with the provided parameters."""
    OdbcDataExtractorTaskConfigModel(**parameters)
    assert True


@pytest.mark.parametrize(
    "query, sql_file_path, expected_pass",
    [
        (None, None, False),
        ("SELECT", None, True),
        (None, "/path", True),
        ("SELECT", "/path", False),
    ],
)
def test_odbc_data_extractor_config_model_query_and_path(query, sql_file_path, expected_pass):
    """Method to test checks if the OdbcDataExtractorTaskConfigModel raises a ValueError
    when both 'query' and 'sql_file_path' are not specified, or both are specified."""
    test_param = deepcopy(parameters)
    test_param["query"] = query
    test_param["sql_file_path"] = sql_file_path

    if not expected_pass:
        with pytest.raises(ValueError):
            OdbcDataExtractorTaskConfigModel(**test_param)
    else:
        assert True


@pytest.mark.parametrize(
    "query, sql_file_path, expected_pass",
    [
        (None, SQL_FILE_PATH, True),
        ("SELECT * FROM test_tbl WHERE pos_dt = '2023-10-10'", None, True),
        (None, "/path", False),
    ],
    ids=["Query from SQL file", "Query from config", "Query from SQL File not exist"],
)
def test_get_query(query, sql_file_path, expected_pass):
    """Method to test get query from config or file path."""
    test_param = deepcopy(parameters)
    test_param["query"] = query
    test_param["sql_file_path"] = sql_file_path

    expected_query = "SELECT * FROM test_tbl WHERE pos_dt = '2023-10-10'"
    json_reader = JSONReader(config_file_path="")
    rendered_param = json_reader.render_jinja_template(
        json.dumps(test_param), ConfigMapping(pos_dt=JOB_PARAMS.pos_dt)
    )
    module_config = mock_model(
        module_name=OdbcDataExtractorTask,
        parameters=OdbcDataExtractorTaskConfigModel(**rendered_param),
    )
    odbc_task = OdbcDataExtractorTask(module_config, JOB_PARAMS)
    if not expected_pass:
        with pytest.raises(FileNotFoundError):
            odbc_task.get_query()
    else:
        query = odbc_task.get_query()
        assert (
            query.replace("\n", "") == expected_query
        ), "Actual query does not match with expected"


@pytest.fixture(scope="module", autouse=True)
def setup_environment_variables():
    """Setup environment variable to override the '.env' file for unit testing."""
    # environment variable for ADLS storage
    os.environ["MDP_INBND__ACCOUNT_NAME"] = "testadls001dev"
    os.environ["MDP_INBND__CONTAINER_NAME"] = "test_container"
    os.environ["MDP_INBND__SAS_TOKEN"] = "test_token"
    os.environ["MDP_INBND__filepath"] = "test_adls/filepath"
    # environment variable for local storage
    os.environ["LOCAL_STORAGE__filepath"] = "test_local/filepath"

    # CONNECTION INFO
    # ud
    os.environ["CONNECTION_INFO__UD__dbtype"] = "oracledb"
    os.environ["CONNECTION_INFO__UD__username"] = "test_user_ud"
    os.environ["CONNECTION_INFO__UD__password"] = "test_password_ud"
    os.environ["CONNECTION_INFO__UD__database"] = "test_sid_ud"
    os.environ["CONNECTION_INFO__UD__server"] = "test_server_ud"
    os.environ["CONNECTION_INFO__UD__port"] = "1525"

    # scf
    os.environ["CONNECTION_INFO__SCF__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__SCF__username"] = "test_user_scf"
    os.environ["CONNECTION_INFO__SCF__password"] = "test_password_scf"
    os.environ["CONNECTION_INFO__SCF__database"] = "test_database_scf"
    os.environ["CONNECTION_INFO__SCF__server"] = "test_server_scf"
    os.environ["CONNECTION_INFO__SCF__port"] = "1433"

    # dgtl_fctrng
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__username"] = "test_user_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__password"] = "test_password_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__database"] = "test_database_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__server"] = "test_server_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__port"] = "1433"

    # kbc_ivr
    os.environ["CONNECTION_INFO__KBC_IVR__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__KBC_IVR__username"] = "test_user_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__password"] = "test_password_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__database"] = "test_database_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__server"] = "test_server_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__port"] = "1433"

    # kbc_docsub
    os.environ["CONNECTION_INFO__KBC_DOCSUB__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__username"] = "test_user_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__password"] = "test_password_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__database"] = "test_database_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__server"] = "test_server_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__port"] = "1433"

    # smartserve
    os.environ["CONNECTION_INFO__SMARTSERVE__dbtype"] = "db2"
    os.environ["CONNECTION_INFO__SMARTSERVE__username"] = "test_user_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__password"] = "test_password_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__database"] = "test_database_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__server"] = "test_server_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__port"] = "50000"
    os.environ["CONNECTION_INFO__SMARTSERVE__schemaname"] = "test_schema_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__securitymechanism"] = "13"

    # mockmariadb
    os.environ["CONNECTION_INFO__MOCKMARIADB__dbtype"] = "mariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__username"] = "test_user_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__password"] = "test_password_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__database"] = "test_database_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__server"] = "test_server_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__port"] = "3306"


@pytest.mark.parametrize(
    "connection_name, expected_connection_string",
    [
        (
            "scf",
            (
                "mssql+pyodbc:///?odbc_connect="
                + "Driver={ODBC Driver 18 for SQL Server};"
                + "Server=tcp:test_server_scf,1433;"
                + "Database=test_database_scf;"
                + "Uid=test_user_scf;"
                + "Pwd=test_password_scf;"
                + "Encrypt=yes;"
                + "TrustServerCertificate=yes;"
                + "Connection Timeout=180;"
            ),
        ),
        (
            "ud",
            (
                "oracle+cx_oracle://"
                + "test_user_ud:test_password_ud"
                + "@test_server_ud:"
                + "1525/"
                + "?service_name=test_sid_ud"
                + "&encoding=UTF-8"
                + "&nencoding=UTF-8"
            ),
        ),
        (
            "smartserve",
            (
                "ibm_db_sa://"
                + "test_user_smartserve:test_password_smartserve"
                + "@test_server_smartserve:50000/"
                + "test_database_smartserve"
                + ";currentSchema=test_schema_smartserve"
                + ";securityMechanism=13"
            ),
        ),
        (
            "mockmariadb",
            (
                "mariadb+pyodbc:///?odbc_connect="
                + "DRIVER={MariaDB};"
                + "SERVER=test_server_mockmariadb;"
                + "DATABASE=test_database_mockmariadb;"
                + "UID=test_user_mockmariadb;"
                + "PWD=test_password_mockmariadb;"
                + "PORT=3306;"
                + "Connection Timeout=180;"
            ),
        ),
    ],
)
@patch(
    "mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor.create_engine"
)
def test_connect_to_database(mock_create_engine, connection_name, expected_connection_string):
    """Method to test SQLAlchemy engine for the database connection call."""

    env_file = EnvSettings()
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)

    connector = OdbcDatabaseConnector(data_source_setting)
    mock_create_engine.assert_called_once_with(
        expected_connection_string,
        pool_timeout=10800,
        pool_recycle=1500,
        pool_pre_ping=True,
        echo=True,
        echo_pool="debug",
    )
    assert (
        connector.connection_info == data_source_setting
    ), "Connection parameters does not match expected."


@pytest.mark.parametrize(
    "header, expected_content",
    [
        (True, [["id", "name"], ["001", "John"], ["002", "James"]]),
        (False, [["001", "John"], ["002", "James"]]),
    ],
    ids=["Write file with header", "Write file without header"],
)
@patch(
    "mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor.create_engine"
)
def test_write_to_csv(mock_create_engine, tmp_path, header, expected_content):
    """Method to test writing CSV file."""

    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)
    write_property = MODULE_CONFIG.parameters.write_property
    write_property.header = header

    connector = OdbcDatabaseConnector(data_source_setting)
    file_name = str(tmp_path) + "/extracted_file.csv"
    connector.write_to_csv(
        file_name=file_name,
        header_col=["id", "name"],
        data=[("001", "John"), ("002", "James")],
        write_property=write_property,
        file_option=MODULE_CONFIG.parameters.file_option,
    )

    with open(file_name, "r", newline="") as csvfile:
        csv_reader = csv.reader(
            csvfile,
            delimiter=MODULE_CONFIG.parameters.write_property.option.get("delimiter"),
        )
        data = [row for row in csv_reader]

    assert data == expected_content, "File content does not match with expected."


def test_render_full_file_path():
    """Method to test render full file path."""
    json_reader = JSONReader(config_file_path="")
    rendered_param = json_reader.render_jinja_template(
        json.dumps(parameters), ConfigMapping(pos_dt=JOB_PARAMS.pos_dt)
    )
    module_config = mock_model(
        module_name=OdbcDataExtractorTask,
        parameters=OdbcDataExtractorTaskConfigModel(**rendered_param),
    )
    odbc_task = OdbcDataExtractorTask(module_config, JOB_PARAMS)
    assert (
        odbc_task.full_file_path
        == "/extraction/extracted_file/CUSTOMER_TXN_DAILY_D20231031_part-{{ part_number }}"
    )


@patch(
    "mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor.create_engine"
)
def test_replaced_full_file_name(mock_create_engine):
    """Test Replace the 'part_number' variable in the full file name with the provided
    value."""

    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)
    connector = OdbcDatabaseConnector(data_source_setting)
    full_file_name = connector.replaced_full_file_name(
        "CUSTOMER_TXN_DAILY_D20231031_part-{{ part_number }}", 1
    )

    assert full_file_name == "CUSTOMER_TXN_DAILY_D20231031_part-1"


@patch(
    "mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor.create_engine"
)
def test_search_existing_file(mock_create_engine):
    # Create an instance of YourClass
    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)
    connector = OdbcDatabaseConnector(data_source_setting)

    full_file_name = "example_part_number"
    dir_name = "/path/to/directory"

    # Mock the file system operations
    with patch.object(pathlib.Path, "glob") as mock_glob:
        # Set up the return value of glob
        mock_glob.return_value = [
            pathlib.Path("/path/to/directory/file1.txt"),
            pathlib.Path("/path/to/directory/file2.txt"),
        ]

        matched_files = connector.search_existing_file(full_file_name, dir_name)

        # Check if glob was called with the correct argument
        mock_glob.assert_called_once_with("example_part_number.*")

        # Check if the returned matched_files list contains the expected file paths
        assert matched_files == ["/path/to/directory/file1.txt", "/path/to/directory/file2.txt"]


# Other tests are in SIT due to using database connection
