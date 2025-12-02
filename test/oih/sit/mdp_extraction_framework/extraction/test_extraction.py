"""System Integration Test Module."""

# import: standard
import glob
import os
from datetime import date
from datetime import datetime

# import: internal
from mdp.framework.mdp_extraction_framework.utility.common_function import get_log_filename
from mdp.framework.mdp_extraction_framework.utility.common_function import setup_logger
from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_file import (
    validate_local_file_exists,
)
from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_transfer_azcopy import (
    validate_file_exists,
)

# import: external
import pytest
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# load the environment variables from the ".env" file
load_dotenv("/app_oih/oih/script/extraction/.env", override=True)
load_dotenv("/app_oih/oih/script/extraction/.env.secret", override=True)

today = date.today()
first_day_of_last_month = (today - relativedelta(months=1)).replace(day=1)
last_day_of_last_2_month = first_day_of_last_month - relativedelta(days=1)
FILE_POS_DT = datetime.strftime(last_day_of_last_2_month, "%Y%m%d")
SAS_TOKEN = os.environ["OIH_INBND__SAS_TOKEN"]
ACCOUNT_NAME = os.environ["OIH_INBND__ACCOUNT_NAME"]
CONTAINER_NAME = os.environ["OIH_INBND__CONTAINER_NAME"]
FILEPATH = os.environ["OIH_INBND__FILEPATH"]
FW_TEST_SOURCE_FILE_DIR = os.environ["LOCAL_STORAGE__FILEPATH"]


@pytest.fixture(scope="session", autouse=True)
def setup_test_logger() -> None:
    """Setup function for test_extraction's logger.

    The logger is used in the validation function, so the object returned from the
    function "setup_logger" is unused here.
    """
    current_datetime = datetime.now()
    current_datetime_str = current_datetime.strftime("%Y-%m-%d")
    # Initialize application logging validate function
    setup_logger(job_name="extrct_sit_pytest", pos_dt=current_datetime_str)


@pytest.mark.parametrize(
    "extracted_file",
    [
        f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_trf_test_d_20240101.csv",
    ],
    ids=[
        "Check extracted file",
    ],
)
def test_local_storage_extracted_file_exist(extracted_file) -> None:
    """Method to test data extraction extracted file exist."""
    assert validate_local_file_exists(
        extracted_file
    ), f"Local storage extracted file {extracted_file} does not exist"


@pytest.mark.parametrize(
    "expected_file, job_name",
    [
        ("extrct_sit_trf_test_d_20240101.csv", "extrct_sit_trf_test_d"),
    ],
    ids=[
        "Check transfer only expected_file file",
    ],
)
def test_adls_file_exists(expected_file, job_name) -> None:
    """Method to test if raw table contains record."""
    storage_url = (
        f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy/"
    )
    adls_files = validate_file_exists(storage_url=storage_url, sas_token=SAS_TOKEN)
    assert (
        expected_file in adls_files
    ), f"The return file listed from the storage: {adls_files} does not contain the expected file: {expected_file}"


def test_run_only_azcopy():
    """Method to assert log when running only azcopy task by checking any .log file in
    the directory."""

    job_name = "extrct_sit_trf_test_d"
    pos_dt = "2024-01-01"

    directory, _ = get_log_filename(job_name, pos_dt)

    assert os.path.exists(directory), f"Directory not found: {directory}"

    log_files = glob.glob(os.path.join(directory, "*.log"))

    assert log_files, f"No log files found in directory: {directory}"

    for log_file_path in log_files:
        with open(log_file_path, "r") as log_file:
            log_content = log_file.read()

            assert (
                "Starting execution of AzCopyDataTransferTask." in log_content
            ), f"AzCopyDataTransferTask execution not found in {log_file_path}."

            starting_count = log_content.count("Starting")
            assert starting_count == 2, f"Log {log_file_path} does not contain the word 'Starting'."
