"""Config test Module."""
# import: standard
import os
import shutil
from datetime import date
from datetime import datetime
from pathlib import Path

# import: internal
from mdp.framework.mdp_extraction_framework.__main__ import entrypoint
from mdp.framework.mdp_extraction_framework.utility.common_function import get_log_filename
from mdp.framework.mdp_extraction_framework.utility.common_function import setup_logger
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
import pytest
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from pkg_resources import resource_filename

# os.environ["no_proxy"] = "*"

load_dotenv("/app_oih/oih/script/extraction/.env", override=True)
load_dotenv("/app_oih/oih/script/extraction/.env.secret", override=True)

today = date.today()
first_day_of_last_month = (today - relativedelta(months=1)).replace(day=1)
last_day_of_last_2_month = first_day_of_last_month - relativedelta(days=1)
POS_DT = datetime.strftime(last_day_of_last_2_month, "%Y-%m-%d")
FW_TEST_CONFIG_DIR = "/app_oih/oih/config/extraction/test_area"
FW_TEST_SOURCE_FILE_DIR = "/datasource/inbound/source_file/oih/test_area"

SAS_TOKEN = os.environ["OIH_INBND__SAS_TOKEN"]
ACCOUNT_NAME = os.environ["OIH_INBND__ACCOUNT_NAME"]
CONTAINER_NAME = os.environ["OIH_INBND__CONTAINER_NAME"]
FILEPATH = os.environ["OIH_INBND__FILEPATH"]


@pytest.fixture(scope="session", autouse=True)
def setup_resources_extraction_pipeline() -> None:
    """Set Up Test Method Before run test module."""

    extraction_resources_path = resource_filename(__name__, "extraction/resources")

    # Pipeline Config
    Path(FW_TEST_CONFIG_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_trf_test_d.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_trf_test_d.json",
    )

    # Data File
    Path(FW_TEST_SOURCE_FILE_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_trf_test_d/extrct_sit_trf_test_d_20240101.csv",
        f"{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_trf_test_d_20240101.csv",
    )
    # Mock existing files in the ADLS storage
    mock_directory_url = (
        f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy/"
    )
    run_command(
        f'azcopy cp "{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_trf_test_d_20240101.csv" "{mock_directory_url}" --recursive=true'
    )

    yield ("Setup resources complete")


@pytest.fixture(scope="session", autouse=True)
def teardown_resources_extraction_pipeline() -> None:
    """Teardown Test Method after run test module."""
    yield ("Run complete, start tear down resource steps")

    run_only_azcopy_directory, _ = get_log_filename("extrct_sit_run_only_azcopy", "1990-01-01")
    extract_ctl_directory, _ = get_log_filename("extrct_sit_run_only_extract_and_ctl", "1990-01-01")

    # Teardown config and test data source folder
    shutil.rmtree(FW_TEST_CONFIG_DIR)
    shutil.rmtree(FW_TEST_SOURCE_FILE_DIR)
    try:
        shutil.rmtree(run_only_azcopy_directory)
        shutil.rmtree(extract_ctl_directory)
    except FileNotFoundError:
        pass

    # Teardown the transferred files
    mock_directory_url = (
        f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy/"
    )
    run_command(f'azcopy rm "{mock_directory_url}" --recursive=true')


@pytest.fixture(scope="session", autouse=True)
def trigger_extraction_pipeline() -> None:
    """Method to call the entrypoint to run the extraction framework."""

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_trf_test_d", pos_dt="2024-01-01")

    # Trigger success transfer file only job
    logger.info("Starting transfer and cleanup folder job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_trf_test_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
        "--project",
        "oih",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction transfer and cleanup folder job framework run completed.")
