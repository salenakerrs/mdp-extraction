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
from mdp.framework.mdp_extraction_framework.task.data_extractor.mongodb_data_extractor import (
    MongoDatabaseConnector,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.mongodb_data_extractor import (
    MongoDataExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.mongodb_data_extractor import (
    MongoDataExtractorTaskConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    FileOptionConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    WritePropertyConfigModel,
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


JSON_FILE_PATH = (
    "test/mdp/unit/mdp_extraction_framework/resources/task/data_extractor/extraction_query.json"
)
parameters = {
    "connection_name": "mockmongo",
    "json_file_path": JSON_FILE_PATH,
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
    module_name=MongoDataExtractorTask, parameters=MongoDataExtractorTaskConfigModel(**parameters)
)


def test_odbc_data_extractor_config_model():
    """Method to test checks if the MongoDataExtractorTaskConfigModel can be
    instantiated with the provided parameters."""
    MongoDataExtractorTaskConfigModel(**parameters)
    assert True


@pytest.mark.parametrize(
    "query, json_file_path, expected_pass",
    [
        (None, None, False),
        ('{ "pos_dt": "2023-10-10" }', None, True),
        (None, "/path", True),
        ('{ "pos_dt": "2023-10-10" }', "/path", False),
    ],
)
def test_mongo_data_extractor_config_model_query_and_path(query, json_file_path, expected_pass):
    """Method to test checks if the MongoDataExtractorTaskConfigModel raises a
    ValueError when both 'query' and 'json_file_path' are not specified, or both are
    specified."""
    test_param = deepcopy(parameters)
    test_param["query"] = query
    test_param["json_file_path"] = json_file_path

    if not expected_pass:
        with pytest.raises(ValueError):
            MongoDataExtractorTaskConfigModel(**test_param)
    else:
        assert True


@pytest.mark.parametrize(
    "query, json_file_path, expected_pass",
    [
        (None, JSON_FILE_PATH, True),
        ('{ "pos_dt": "2023-10-10" }', None, True),
        (None, "/path", False),
    ],
    ids=["Query from SQL file", "Query from config", "Query from SQL File not exist"],
)
def test_get_query(query, json_file_path, expected_pass):
    """Method to test get query from config or file path."""
    test_param = deepcopy(parameters)
    test_param["query"] = query
    test_param["json_file_path"] = json_file_path

    expected_query = '{ "pos_dt": "2023-10-10" }'

    json_reader = JSONReader(config_file_path="")
    rendered_param = json_reader.render_jinja_template(
        json.dumps(test_param), ConfigMapping(pos_dt=JOB_PARAMS.pos_dt)
    )
    module_config = mock_model(
        module_name=MongoDataExtractorTask,
        parameters=MongoDataExtractorTaskConfigModel(**rendered_param),
    )
    mongo_task = MongoDataExtractorTask(module_config, JOB_PARAMS)
    if not expected_pass:
        with pytest.raises(FileNotFoundError):
            mongo_task.get_query()
    else:
        query = mongo_task.get_query()
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
    # mockmongo
    os.environ["CONNECTION_INFO__MOCKMONGO__dbtype"] = "mongodb"
    os.environ["CONNECTION_INFO__MOCKMONGO__username"] = "test_user_mockmongo"
    os.environ["CONNECTION_INFO__MOCKMONGO__password"] = "test_password_mockmongo"
    os.environ["CONNECTION_INFO__MOCKMONGO__database"] = "test_database_mockmongo"
    os.environ["CONNECTION_INFO__MOCKMONGO__server"] = "test_server_mockmongo:27017"
    os.environ["CONNECTION_INFO__MOCKMONGO__collection"] = "test_collection_mockmongo"

    # mockmongo with replica set
    os.environ["CONNECTION_INFO__MOCKMONGO_REPLICA__dbtype"] = "mongodb"
    os.environ["CONNECTION_INFO__MOCKMONGO_REPLICA__username"] = "test_user_mockmongo_replica"
    os.environ["CONNECTION_INFO__MOCKMONGO_REPLICA__password"] = "test_password_mockmongo_replica"
    os.environ["CONNECTION_INFO__MOCKMONGO_REPLICA__database"] = "test_database_mockmongo_replica"
    os.environ[
        "CONNECTION_INFO__MOCKMONGO_REPLICA__server"
    ] = "test_server_mockmongo_replica:27017,test_server_mockmongo_replica2:27017?replicaSet=rs0"
    os.environ[
        "CONNECTION_INFO__MOCKMONGO_REPLICA__collection"
    ] = "test_collection_mockmongo_replica"


@pytest.mark.parametrize(
    "connection_name, expected_connection_string",
    [
        (
            "mockmongo",
            (
                "mongodb://"
                + "test_user_mockmongo:test_password_mockmongo"
                + "@test_server_mockmongo:27017"
            ),
        ),
        (
            "mockmongo_replica",
            (
                "mongodb://"
                + "test_user_mockmongo_replica:test_password_mockmongo_replica"
                + "@test_server_mockmongo_replica:27017,test_server_mockmongo_replica2:27017"
                + "?replicaSet=rs0"
            ),
        ),
    ],
)
@patch("pymongo.MongoClient")
def test_connect_to_database(mock_client, connection_name, expected_connection_string):
    """Method to test connecting to the database."""

    env_file = EnvSettings()
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)

    connector = MongoDatabaseConnector(data_source_setting)
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
@patch("pymongo.MongoClient")
def test_write_to_csv(mock_client, tmp_path, header, expected_content):
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

    connector = MongoDatabaseConnector(data_source_setting)
    file_name = str(tmp_path) + "/extracted_file.csv"
    connector.write_to_csv(
        file_name=file_name,
        header_col=["id", "name"],
        data=[{"id": "001", "name": "John"}, {"id": "002", "name": "James"}],
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


@pytest.mark.parametrize(
    "expected_content",
    [
        [{"id": "001", "name": "John"}, {"id": "002", "name": "James"}],
    ],
)
@patch("pymongo.MongoClient")
def test_write_to_json(mock_client, tmp_path, expected_content):
    """Method to test writing JSON file."""

    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    db_type = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(db_type.lower())
    data_source_setting = settings_class(**connection_data)

    connector = MongoDatabaseConnector(data_source_setting)
    file_name = str(tmp_path) + "/extracted_file.json"
    connector.write_to_json(
        [{"id": "001", "name": "John"}, {"id": "002", "name": "James"}],
        file_name,
    )

    with open(file_name, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)

    assert data == expected_content, "File content does not match with expected."


def test_render_full_file_path():
    """Method to test render full file path."""
    json_reader = JSONReader(config_file_path="")
    rendered_param = json_reader.render_jinja_template(
        json.dumps(parameters), ConfigMapping(pos_dt=JOB_PARAMS.pos_dt)
    )
    module_config = mock_model(
        module_name=MongoDataExtractorTask,
        parameters=MongoDataExtractorTaskConfigModel(**rendered_param),
    )
    mongo_task = MongoDataExtractorTask(module_config, JOB_PARAMS)
    assert (
        mongo_task.full_file_path
        == "/extraction/extracted_file/CUSTOMER_TXN_DAILY_D20231031_part-{{ part_number }}"
    )


@patch("pymongo.MongoClient")
def test_replaced_full_file_name(mock_client):
    """Test Replace the 'part_number' variable in the full file name with the provided
    value."""

    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)
    connector = MongoDatabaseConnector(data_source_setting)
    full_file_name = connector.replaced_full_file_name(
        "CUSTOMER_TXN_DAILY_D20231031_part-{{ part_number }}", 1
    )

    assert full_file_name == "CUSTOMER_TXN_DAILY_D20231031_part-1"


@patch("pymongo.MongoClient")
def test_search_existing_file(mock_client):
    # Create an instance of YourClass
    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)
    connector = MongoDatabaseConnector(data_source_setting)

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


@pytest.mark.parametrize(
    "query, base_filename, batch_size, file_extension, write_property, file_option, allow_zero_record, expected_files",
    [
        (
            '{ "pos_dt": "2023-10-10" }',
            "data",
            1,
            "csv",
            WritePropertyConfigModel(format="csv", header=True, option={"delimiter": "|"}),
            FileOptionConfigModel(mode="w", newline="", encoding="utf-8"),
            True,
            ["data.csv"],
        ),
        (
            '{ "pos_dt": "2023-10-10" }',
            "data",
            1,
            "json",
            WritePropertyConfigModel(format="json"),
            FileOptionConfigModel(mode="w", newline="", encoding="utf-8"),
            True,
            ["data.json"],
        ),
    ],
)
@patch("pymongo.MongoClient")
def test_save_data_in_batches(
    mock_client,
    tmp_path,
    query,
    base_filename,
    batch_size,
    file_extension,
    write_property,
    file_option,
    allow_zero_record,
    expected_files,
):
    """Method to test saving data in batches."""

    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)

    connector = MongoDatabaseConnector(data_source_setting)
    connector.collection = mock_client().db.collection

    # Mock the cursor to return a list of documents
    mock_cursor = [{"id": "001", "name": "John"}, {"id": "002", "name": "James"}]
    connector.collection.aggregate.return_value.batch_size.return_value = mock_cursor

    file_infos = connector.save_data_in_batches(
        query=query,
        base_filename=str(tmp_path / base_filename),
        batch_size=batch_size,
        file_extension=file_extension,
        write_property=write_property,
        file_option=file_option,
        allow_zero_record=allow_zero_record,
    )

    generated_files = list(set(file_info.file_location for file_info in file_infos))
    assert generated_files == [
        str(tmp_path / file) for file in expected_files
    ], "Generated files do not match expected files."


@pytest.mark.parametrize(
    "query, base_filename, file_extension, write_property, file_option, header_col, expected_file, expected_data",
    [
        (
            '{ "pos_dt": "2023-10-10" }',
            "data",
            "csv",
            WritePropertyConfigModel(format="csv", header=True, option={"delimiter": "|"}),
            FileOptionConfigModel(mode="w", newline="", encoding="utf-8"),
            ["id", "name"],
            "data.csv",
            [{"id": "001", "name": "John"}, {"id": "002", "name": "James"}],
        ),
        (
            '{ "pos_dt": "2023-10-10" }',
            "data",
            "json",
            WritePropertyConfigModel(format="json"),
            FileOptionConfigModel(mode="w", newline="", encoding="utf-8"),
            ["id", "name"],
            "data.json",
            [{"id": "001", "name": "John"}, {"id": "002", "name": "James"}],
        ),
    ],
)
@patch("pymongo.MongoClient")
def test_save_data(
    mock_client,
    tmp_path,
    query,
    base_filename,
    file_extension,
    write_property,
    file_option,
    header_col,
    expected_file,
    expected_data,
):
    """Method to test saving data to a file."""

    env_file = EnvSettings()
    connection_name = MODULE_CONFIG.parameters.connection_name
    data_source_setting = env_file.connection_info.get(connection_name)
    connection_data = env_file.connection_info.get(connection_name)
    dbtype = connection_data.get("dbtype")
    settings_class = DB_TYPE_MAPPING.get(dbtype.lower())
    data_source_setting = settings_class(**connection_data)

    connector = MongoDatabaseConnector(data_source_setting)
    connector.collection = mock_client().db.collection

    # Mock the cursor to return a list of documents
    mock_cursor = [{"id": "001", "name": "John"}, {"id": "002", "name": "James"}]
    connector.collection.aggregate.return_value = mock_cursor

    file_name, selected_data = connector.save_data(
        query=query,
        base_filename=str(tmp_path / base_filename),
        file_extension=file_extension,
        write_property=write_property,
        file_option=file_option,
        header_col=header_col,
    )

    assert file_name == str(tmp_path / expected_file), "File name does not match expected."
    assert selected_data == expected_data, "Selected data does not match expected."

    if file_extension == "csv":
        with open(file_name, "r", newline="") as csvfile:
            csv_reader = csv.reader(
                csvfile,
                delimiter=write_property.option.get("delimiter"),
            )
            data = [row for row in csv_reader]
            expected_csv_data = [header_col] + [
                [doc.get(col, "") for col in header_col] for doc in expected_data
            ]
            assert data == expected_csv_data, "CSV file content does not match expected."
    elif file_extension == "json":
        with open(file_name, "r", encoding="utf-8") as jsonfile:
            data = json.load(jsonfile)
            assert data == expected_data, "JSON file content does not match expected."
