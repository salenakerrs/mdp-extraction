"""Config test Module."""
# import: standard
import os
import shutil
from datetime import date
from datetime import datetime
from pathlib import Path

# import: internal
from mdp.framework.mdp_extraction_framework.__main__ import entrypoint
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    DataExtractorNoRecordError,
)
from mdp.framework.mdp_extraction_framework.utility.common_function import get_log_filename
from mdp.framework.mdp_extraction_framework.utility.common_function import setup_logger
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
import pytest
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from pkg_resources import resource_filename

os.environ["no_proxy"] = "*"

load_dotenv("/app_mdp/mdp/script/extraction/.env", override=True)
load_dotenv("/app_mdp/mdp/script/extraction/.env.secret", override=True)

today = date.today()
first_day_of_last_month = (today - relativedelta(months=1)).replace(day=1)
last_day_of_last_2_month = first_day_of_last_month - relativedelta(days=1)
POS_DT = datetime.strftime(last_day_of_last_2_month, "%Y-%m-%d")
FW_TEST_CONFIG_DIR = "/app_mdp/mdp/config/extraction/test_area"
FW_TEST_SOURCE_FILE_DIR = "/datasource/inbound/source_file/mdp/test_area"

SAS_TOKEN = os.environ["MDP_INBND__SAS_TOKEN"]
ACCOUNT_NAME = os.environ["MDP_INBND__ACCOUNT_NAME"]
CONTAINER_NAME = os.environ["MDP_INBND__CONTAINER_NAME"]
FILEPATH = os.environ["MDP_INBND__FILEPATH"]


@pytest.fixture(scope="session", autouse=True)
def setup_resources_extraction_pipeline() -> None:
    """Set Up Test Method Before run test module."""

    extraction_resources_path = resource_filename(__name__, "extraction/resources")

    # Pipeline Config
    Path(FW_TEST_CONFIG_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        f"{extraction_resources_path}/extrct_db2_sit_table_d.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_db2_sit_table_d.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_db2_sit_table_d_ctl.sql",
        f"{FW_TEST_CONFIG_DIR}/extrct_db2_sit_table_d_ctl.sql",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_mongodb_csv_sit_table_d_query.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_mongodb_csv_sit_table_d_query.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_mongodb_csv_sit_table_d.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_mongodb_csv_sit_table_d.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_mongodb_json_sit_table_d.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_mongodb_json_sit_table_d.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_oracle_sit_table_d.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_oracle_sit_table_d.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_mssql_sit_table_d.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_mssql_sit_table_d.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_mssql_sit_table_d.sql",
        f"{FW_TEST_CONFIG_DIR}/extrct_mssql_sit_table_d.sql",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_mssql_sit_table_d_ctl.sql",
        f"{FW_TEST_CONFIG_DIR}/extrct_mssql_sit_table_d_ctl.sql",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_table_zero_true.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_zero_true.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_table_zero_false.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_zero_false.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_table_fail_syntax_query.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_fail_syntax_query.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_table_fail_syntax_script.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_fail_syntax_script.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_table_fail_syntax.sql",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_fail_syntax.sql",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_file_transfer.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_file_transfer.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_file_not_exist.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_file_not_exist.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_table_zero_overwrite.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_zero_overwrite.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_unzip.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_unzip.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_pgp.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_pgp.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_unzip_pgp.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_unzip_pgp.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_hsm_key_gen_body.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_hsm_key_gen_body.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_hsm_key_gen_header.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_hsm_key_gen_header.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_hsm_key_gen_zip_body.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_hsm_key_gen_zip_body.json",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_trf_test_folder_d.json",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_trf_test_folder_d.json",
    )

    # Data File
    Path(FW_TEST_SOURCE_FILE_DIR).mkdir(parents=True, exist_ok=True)
    # Mock existing files in the local storage
    shutil.copytree(
        src=f"{extraction_resources_path}/extrct_sit_mssql_table_d",
        dst=f"{FW_TEST_SOURCE_FILE_DIR}/",
        dirs_exist_ok=True,
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_trf_test_d/extrct_sit_trf_test_d_20240101.csv",
        f"{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_trf_test_d_20240101.csv",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_unzip/test_compress.zip",
        f"{FW_TEST_SOURCE_FILE_DIR}/test_compress.zip",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_pgp/pgp_encrypted_file.txt",
        f"{FW_TEST_SOURCE_FILE_DIR}/pgp_encrypted_file.txt",
    )
    shutil.copytree(
        f"{extraction_resources_path}/extrct_sit_trf_test_folder_d",
        f"{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_trf_test_folder_d",
        dirs_exist_ok=True,
    )
    # Mock existing files in the ADLS storage
    mock_directory_url = f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy/?{SAS_TOKEN}"
    mock_directory_url_trf_folder = f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy_folder/?{SAS_TOKEN}"
    run_command(
        f'azcopy cp "{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_mssql_table_d*" "{mock_directory_url}" --recursive=true'
    )
    run_command(
        f'azcopy cp "{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_trf_test_d_20240101.csv" "{mock_directory_url}" --recursive=true'
    )
    run_command(
        f'azcopy cp "{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_trf_test_folder_d/extrct_sit_trf_test_folder_d_20240101/" "{mock_directory_url_trf_folder}/extrct_sit_trf_test_folder_d" --recursive=true'
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_hsm_key_gen/extrct_sit_hsm_key_body_d_20240806.txt",
        f"{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_hsm_key_body_d_20240806.txt",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_hsm_key_gen/extrct_sit_hsm_key_header_d_20240902.txt",
        f"{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_hsm_key_header_d_20240902.txt",
    )
    shutil.copy(
        f"{extraction_resources_path}/extrct_sit_hsm_key_gen/extrct_sit_hsm_key_body.zip",
        f"{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_hsm_key_body.zip",
    )

    # Remove files before testing
    extra_file_path = Path(
        f"{FW_TEST_SOURCE_FILE_DIR}/extrct_sit_trf_test_folder_d/extrct_sit_trf_test_folder_d_20240101/extrct_sit_trf_test_d_20240101_2.csv"
    )
    extra_file_path.unlink()
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
    mock_directory_url = f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy/?{SAS_TOKEN}"
    mock_directory_url_trf_folder = f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy_folder/?{SAS_TOKEN}"
    run_command(f'azcopy rm "{mock_directory_url}" --recursive=true')
    run_command(f'azcopy rm "{mock_directory_url_trf_folder}" --recursive=true')


@pytest.fixture(scope="session", autouse=True)
def trigger_extraction_pipeline() -> None:
    """Method to call the entrypoint to run the extraction framework."""

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_file_transfer", pos_dt="2024-01-01")

    # Trigger success transfer file only job
    logger.info("Starting transfer file only job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_file_transfer.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction transfer file only job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_mssql_sit_table_d", pos_dt="2024-02-28")

    # Trigger MSSQL Job
    logger.info("Starting MSSQL Job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-02-28",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_mssql_sit_table_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction MSSQL Job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_oracle_sit_table_d", pos_dt="2024-03-01")

    # Trigger ORACLE Job
    logger.info("Starting ORACLE Job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        POS_DT,
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_oracle_sit_table_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction ORACLE Job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_table_zero_true", pos_dt="2024-01-01")

    # Trigger allow zero record Job
    logger.info("Starting allow zero record Job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "1990-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_zero_true.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction allow zero record Job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_table_zero_false", pos_dt="1990-01-01")

    # Trigger not allow zero record Job
    logger.info("Starting not allow zero record Job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "1990-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_zero_false.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    with pytest.raises(DataExtractorNoRecordError):
        entrypoint(entry_point_param)
    logger.info("Extraction not allow zero record Job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_table_zero_overwrite", pos_dt="1990-01-01")

    # Trigger overwrite config allow zero record True (from False config)
    logger.info("Starting overwrite config allow zero record True Job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "1990-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_zero_overwrite.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
        "--overwrite_config",
        '{"source_data_extractor_task":{"parameters": {"allow_zero_record":"True"}}}',
        "--overwrite_config",
        '{"tasks":{"source_data_extractor_task":{"parameters": {"allow_zero_record":"True"}}}}',
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction overwrite config allow zero record True Job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_table_fail_syntax_query", pos_dt="1990-01-01")

    # Trigger fail syntax query
    logger.info("Starting fail syntax query job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_fail_syntax_query.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    with pytest.raises(Exception):
        entrypoint(entry_point_param)
    logger.info("Extraction fail syntax query job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_table_fail_syntax_script", pos_dt="2024-01-01")

    # Trigger fail syntax script
    logger.info("Starting fail syntax script job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_table_fail_syntax_script.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    with pytest.raises(Exception):
        entrypoint(entry_point_param)
    logger.info("Extraction fail syntax script job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_file_not_exist", pos_dt="2024-01-01")

    # Trigger fail transfer script for not existing file script
    logger.info("Starting fail transfer script job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_file_not_exist.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    with pytest.raises(Exception):
        entrypoint(entry_point_param)
    logger.info("Extraction fail transfer script job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_hsm_key_gen_body", pos_dt="2024-08-06")

    # Trigger fail transfer script for not existing file script
    logger.info("Starting HSM key file generator from body section job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-08-06",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_hsm_key_gen_body.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("HSM key file generator from body section job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_hsm_key_gen_header", pos_dt="2024-09-02")

    # Trigger fail transfer script for not existing file script
    logger.info("Starting HSM key file generator from header section job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-09-02",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_hsm_key_gen_header.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("HSM key file generator from header section job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_hsm_key_gen_zip_body", pos_dt="2024-08-06")

    # Trigger fail transfer script for not existing file script
    logger.info(
        "Starting HSM key file generator from body section from zip file job extraction framework SIT"
    )
    entry_point_param = [
        "--pos_dt",
        "2024-08-06",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_hsm_key_gen_zip_body.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info(
        "HSM key file generator from body section from zip file job framework run completed."
    )

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_unzip", pos_dt="1990-01-01")

    # Trigger zip file extract
    logger.info("Starting zip file extract job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "1990-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_unzip.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction zip file extract Job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_pgp", pos_dt="1990-01-01")

    # Trigger decrypt pgp
    logger.info("Starting pgp file decrypt job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "1990-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_pgp.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction pgp file decrypt job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_unzip_pgp", pos_dt="1990-01-01")
    # Trigger unzip file and decrypt pgp unzip
    logger.info("Starting unzip and pgp file decrypt job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "1990-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_unzip_pgp.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction unzip and pgp file decrypt Job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_trf_fwtest_db2_sample_sit_d", pos_dt="2024-04-24")

    # Trigger azcopy only job
    logger.info("Starting DB2 job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-04-24",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_db2_sit_table_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction DB2 job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_mongodb_csv_sit_table_d", pos_dt="2024-04-24")

    # Trigger azcopy only job
    logger.info("Starting Mongo CSV job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-04-24",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_mongodb_csv_sit_table_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction Mongo CSV job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_mongodb_json_sit_table_d", pos_dt="2024-04-24")

    # Trigger azcopy only job
    logger.info("Starting Mongo JSON job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-04-24",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_mongodb_json_sit_table_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction Mongo JSON job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_run_only_extract_and_ctl", pos_dt="1990-01-01")

    # Trigger extract and generate ctl file only job
    logger.info("Starting extract and generate ctl file only job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-04-24",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_db2_sit_table_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
        "--run_only_task",
        "source_data_extractor_task,generate_control_file_task",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction extract and generate ctl file only job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_run_only_azcopy", pos_dt="1990-01-01")

    # Trigger azcopy only job
    logger.info("Starting azcopy only job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-04-24",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_db2_sit_table_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
        "--run_only_task",
        "azcopy_data_transfer_task",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction azcopy only job framework run completed.")

    # Initialize application logging for each job
    logger = setup_logger(job_name="extrct_sit_trf_test_folder_d", pos_dt="2024-01-01")

    # Trigger success transfer file only job
    logger.info("Starting transfer and cleanup folder job extraction framework SIT")
    entry_point_param = [
        "--pos_dt",
        "2024-01-01",
        "--config_file_path",
        f"{FW_TEST_CONFIG_DIR}/extrct_sit_trf_test_folder_d.json",
        "--adb_job_id",
        "",
        "--adb_run_id",
        "",
        "--scheduler_id",
        "9999",
    ]
    entrypoint(entry_point_param)
    logger.info("Extraction transfer and cleanup folder job framework run completed.")
