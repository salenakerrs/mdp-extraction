"""Unit test job param converter Module."""


# import: standard
import logging
from unittest.mock import mock_open
from unittest.mock import patch

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.job_param.job_param_converter import (
    add_value_to_job_param,
)
from mdp.framework.mdp_extraction_framework.job_param.job_param_converter import modify_job_param

# import: external
import pytest

logger = logging.getLogger(__name__)


def test_modify_job_param():
    # Create a mock JobParameters object
    job_param = JobParameters(pos_dt="2024-08-19", config_file_path="")

    # Use context managers to mock functions and methods
    with patch(
        "mdp.framework.mdp_extraction_framework.utility.date.common.get_holiday"
    ) as mock_get_holiday, patch(
        "mdp.framework.mdp_extraction_framework.utility.date.common.get_offset_businessdays"
    ) as mock_get_offset_businessdays:
        mock_file_data = "2024-01-01\n2024-12-25\n2024-11-11\n"
        # Mock the open function within the context of the get_holiday function
        with patch("builtins.open", mock_open(read_data=mock_file_data)):
            # Mock the return value of get_holiday
            mock_get_holiday.return_value = ["2024-08-15", "2024-08-16"]

            # Mock the return value of get_offset_businessdays
            mock_get_offset_businessdays.return_value = "2024-08-16"

            # Define the overwrite_job_param dictionary
            modify_job_param_dict = {"source_type": "lpm"}

            # Call the function with the mocks
            result = modify_job_param(modify_job_param_dict, job_param)

            # Assert that the pos_dt has been updated correctly
            assert result.pos_dt == "2024-08-16"


def test_modify_job_param_invalid_source_type():
    # Create a mock JobParameters object
    job_param = JobParameters(pos_dt="2024-08-19", config_file_path="")

    # Define an invalid overwrite_job_param dictionary
    modify_job_param_dict = {"source_type": "invalid_type"}

    # Expect a ValueError to be raised
    with pytest.raises(ValueError, match="Input source_type is not correct."):
        modify_job_param(modify_job_param_dict, job_param)


def test_add_value_to_job_param():
    # Create a mock JobParameters object with initial values
    job_param = JobParameters(pos_dt="", config_file_path="")

    # Define the config dictionary with values to be added
    config = {
        "job_name": "TestJob",
        "job_info": "Test information",
        "area_name": "TestArea",
        "job_seq": "001",
        "pipeline_name": "TestPipeline",
    }

    # Call the function with the mock objects
    result = add_value_to_job_param(config, job_param)

    # Assert that the JobParameters object has been updated correctly
    assert result.job_name == "TestJob"
    assert result.job_info == "Test information"
    assert result.area_name == "TestArea"
    assert result.job_seq == "001"
    assert result.pipeline_name == "TestPipeline"
