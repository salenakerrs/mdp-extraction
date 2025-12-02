"""ODBC Control File Generator Test Module."""

# import: standard
from copy import deepcopy

# import: internal
from mdp.framework.mdp_extraction_framework.task.control_file_generator.odbc_control_file_generator import (
    OdbcControlFileGeneratorTaskConfigModel,
)

# import: external
import pytest

SQL_FILE_PATH = "test/mdp/unit/mdp_extraction_framework/resources/task/control_file_generator/control_file_query.sql"

parameters = {
    "connection_name": "ud",
    "sql_file_path": SQL_FILE_PATH,
    "extract_file_location": "/Volumes/mdp{{ env }}/extraction/extracted_file/mdp/sample_system/",
    "header": True,
    "header_columns": ["record_count", "timestamp", "pos_date"],
    "file_name_format": {
        "base_file_name": "CUSTOMER_TXN_DAILY",
        "date_suffix": "D{{ ptn_yyyy }}{{ ptn_mm }}{{ ptn_dd }}",
    },
    "full_file_name": "{{ base_file_name }}_{{ date_suffix }}",
    "file_extension": "ctl",
    "file_option": {"mode": "a", "newline": "", "encoding": "utf-8"},
    "write_property": {
        "format": "csv",
        "header": True,
        "option": {"delimiter": "|", "quotechar": '"', "quoting": "QUOTE_ALL", "escapechar": "\\"},
    },
}


def test_odbc_control_file_config_model():
    """Method to test checks if the OdbcControlFileGeneratorTaskConfigModel can be
    instantiated with the provided parameters."""
    OdbcControlFileGeneratorTaskConfigModel(**parameters)
    assert True


@pytest.mark.parametrize(
    "header",
    [
        True,
        False,
    ],
)
def test_odbc_control_file_config_model_missing_header_columns(header):
    """Method to test checks if the OdbcControlFileGeneratorTaskConfigModel raises a
    ValueError when the 'header_columns' parameter is missing and 'header' is True.

    If 'header' is False, the test passes without raising an exception.
    """
    no_header_param = deepcopy(parameters)
    no_header_param["header"] = header
    no_header_param.pop("header_columns")
    if header:
        with pytest.raises(ValueError):
            OdbcControlFileGeneratorTaskConfigModel(**no_header_param)
    else:
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
def test_odbc_control_file_config_model_query_and_path(query, sql_file_path, expected_pass):
    """Method to test checks if the OdbcControlFileGeneratorTaskConfigModel raises a
    ValueError when both 'query' and 'sql_file_path' are not specified, or both are
    specified."""
    test_param = deepcopy(parameters)
    test_param["query"] = query
    test_param["sql_file_path"] = sql_file_path

    if not expected_pass:
        with pytest.raises(ValueError):
            OdbcControlFileGeneratorTaskConfigModel(**test_param)
    else:
        assert True


# Other tests are in SIT due to using database connection
