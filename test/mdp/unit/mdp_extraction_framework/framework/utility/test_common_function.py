"""Test module for Common Function."""

# import: standard
import logging
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

# import: internal
from mdp.framework.mdp_extraction_framework.utility.common_function import bypass_wrapper
from mdp.framework.mdp_extraction_framework.utility.common_function import convert_config_to_tuples
from mdp.framework.mdp_extraction_framework.utility.common_function import get_class_object
from mdp.framework.mdp_extraction_framework.utility.common_function import get_log_filename
from mdp.framework.mdp_extraction_framework.utility.common_function import read_file
from mdp.framework.mdp_extraction_framework.utility.common_function import remove_files
from mdp.framework.mdp_extraction_framework.utility.common_function import setup_logger

# import: external
import pytest

DATABASE = "my_database"
TABLE = "my_table"
CURRENT_DATETIME = datetime.now()


class TestCommonFunction:
    """Mock Class for testing."""

    bypass_flag: bool

    @bypass_wrapper
    def execute(self):
        """Execute method for testing."""
        return "executed"


@pytest.mark.parametrize("bypass_flag , expected", [(True, None), (False, "executed")])
def test_bypass_wrapper(bypass_flag, expected):
    """Test the bypass_wrapper decorator.

    Args:
        bypass_flag (bool): Flag to bypass execution.
        expected: Expected result based on the bypass flag.
    """
    obj = TestCommonFunction()
    obj.bypass_flag = bypass_flag
    assert obj.execute() == expected


def test_get_class_object():
    """Test the get_class_object function."""
    result = get_class_object(__name__, "TestCommonFunction")
    assert result == TestCommonFunction


def test_get_log_filename():
    """Test generates a directory path and a filename with the current year-month, job
    name."""
    # Prepare test data
    job_name = "test_job"
    pos_dt = "2024-05-15"

    executed_year_month = CURRENT_DATETIME.strftime("%Y-%m")
    expected_directory = Path(f"/app_log_mdp/mdp/extraction/{executed_year_month}/test_job")
    expected_timestamp = CURRENT_DATETIME.strftime(
        "%Y%m%d%H"
    )  # Remove %M%S for time difference during testing
    expected_pos_dt_str = "20240515"
    expected_file_name = f"extraction_fw_test_job_{expected_pos_dt_str}_{expected_timestamp}"

    # Call the method
    directory, file_name = get_log_filename(job_name, pos_dt)

    # Assertions
    assert directory == expected_directory
    assert file_name.startswith(expected_file_name)


@patch("mdp.framework.mdp_extraction_framework.utility.common_function.get_log_filename")
def test_setup_logger(mock_get_log_filename, tmp_path):
    """Test Sets up a logger handlers."""
    # Patch log directory
    tmp_log_directory = tmp_path / "logs"
    mock_get_log_filename.return_value = (tmp_log_directory, "test_log_file.log")

    logger = setup_logger(job_name="test_job", pos_dt="2024-05-15")
    assert type(logger) == logging.Logger


def test_convert_config_to_tuples():
    """Test the convert_config_to_tuples function if it convert dictionary to a list of
    tuples correctly."""
    test_param = {
        "format": "csv",
        "option": {
            "header": "false",
            "delimiter": "|",
        },
    }
    result = convert_config_to_tuples(config=test_param)
    expected_result = [
        ("format", "csv"),
        ("option", "header", "false"),
        ("option", "delimiter", "|"),
    ]
    assert isinstance(result, list)
    assert (
        result == expected_result
    ), f"The result {result} does not match the expected output {expected_result}."


@pytest.fixture(scope="function")
def mock_test_file(tmp_path):
    # Create a temporary test file
    test_file_path = tmp_path / "test_file.txt"
    with open(test_file_path, "w") as f:
        f.write("Test content")

    yield str(test_file_path)
    if os.path.exists(test_file_path):
        os.remove(test_file_path)


def test_read_file(mock_test_file):
    """Test read content of a file."""
    content = read_file(mock_test_file)
    assert content == "Test content"


def test_remove_files(mock_test_file):
    """Test if the files has been removed."""
    remove_files([mock_test_file])
    assert not os.path.exists(mock_test_file)
