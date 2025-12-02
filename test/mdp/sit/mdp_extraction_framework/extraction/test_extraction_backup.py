# """System Integration Test Module."""

# # import: standard
# import csv
# import glob
# import json
# import os
# from datetime import date
# from datetime import datetime

# # import: internal
# from mdp.framework.mdp_extraction_framework.utility.common_function import get_log_filename
# from mdp.framework.mdp_extraction_framework.utility.common_function import setup_logger
# from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_file import (
#     get_csv_column_value,
# )
# from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_file import (
#     get_csv_row_count,
# )
# from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_file import (
#     validate_csv_header,
# )
# from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_file import (
#     validate_local_file_exists,
# )
# from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_transfer_azcopy import (
#     validate_file_exists,
# )

# # import: external
# import pytest
# from dateutil.relativedelta import relativedelta
# from dotenv import load_dotenv

# # load the environment variables from the ".env" file
# load_dotenv("/app_mdp/mdp/script/extraction/.env", override=True)
# load_dotenv("/app_mdp/mdp/script/extraction/.env.secret", override=True)

# today = date.today()
# first_day_of_last_month = (today - relativedelta(months=1)).replace(day=1)
# last_day_of_last_2_month = first_day_of_last_month - relativedelta(days=1)
# FILE_POS_DT = datetime.strftime(last_day_of_last_2_month, "%Y%m%d")
# SAS_TOKEN = os.environ["MDP_INBND__SAS_TOKEN"]
# ACCOUNT_NAME = os.environ["MDP_INBND__ACCOUNT_NAME"]
# CONTAINER_NAME = os.environ["MDP_INBND__CONTAINER_NAME"]
# FILEPATH = os.environ["MDP_INBND__FILEPATH"]
# FW_TEST_SOURCE_FILE_DIR = os.environ["LOCAL_STORAGE__FILEPATH"]


# @pytest.fixture(scope="session", autouse=True)
# def setup_test_logger() -> None:
#     """Setup function for test_extraction's logger.

#     The logger is used in the validation function, so the object returned from the
#     function "setup_logger" is unused here.
#     """
#     current_datetime = datetime.now()
#     current_datetime_str = current_datetime.strftime("%Y-%m-%d")
#     # Initialize application logging validate function
#     setup_logger(job_name="extrct_sit_pytest", pos_dt=current_datetime_str)


# @pytest.mark.parametrize(
#     "extracted_file",
#     [
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mssql_table_d_20240228_part-0.csv",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_oracle_table_d_{FILE_POS_DT}.csv",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_db2_table_d_20240424.csv",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mongodb_csv_table_d_20240424.csv",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mongodb_json_table_d_20240424.json",
#     ],
#     ids=[
#         "Check mssql extracted file",
#         "Check oracle extracted file",
#         "Check db2 extracted file",
#         "Check mongodb extracted csv file",
#         "Check mongodb extracted json file",
#     ],
# )
# def test_local_storage_extracted_file_exist(extracted_file) -> None:
#     """Method to test data extraction extracted file exist."""
#     assert validate_local_file_exists(
#         extracted_file
#     ), f"Local storage extracted file {extracted_file} does not exist"


# @pytest.mark.parametrize(
#     "extracted_files",
#     [
#         (
#             "extrct_sit_mssql_table_d_20240228_part-0.csv",
#             "extrct_sit_mssql_table_d_20240228_part-1.csv",
#         )
#     ],
#     ids=[
#         "Check mssql extracted parts file",
#     ],
# )
# def test_local_storage_extracted_parts_file_count(extracted_files) -> None:
#     """Method to test data extraction extracted file exist."""
#     # Use os.listdir to get only the file list in the extraction location
#     local_files = os.listdir(f"{FW_TEST_SOURCE_FILE_DIR}/test_area/")
#     assert set(extracted_files).issubset(
#         set(local_files)
#     ), f"The return files listed from the local storage: {local_files} does not contains files in extracted files: {extracted_files}"


# @pytest.mark.parametrize(
#     "extracted_file",
#     [
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_oracle_table_d_{FILE_POS_DT}.csv",
#     ],
#     ids=[
#         "Check oracle extracted file record count",
#     ],
# )
# def test_local_storage_extracted_file_record_count(extracted_file) -> None:
#     """Method to test data extraction extracted file exist."""
#     row_count = get_csv_row_count(extracted_file)
#     assert (
#         row_count > 1
#     ), f"CSV file row count {row_count} does not match expected records (more than 1)."


# @pytest.mark.parametrize(
#     "extracted_file, expected_header",
#     [
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mssql_table_d_20240228_part-0.csv",
#             [
#                 "OrgCode",
#                 "PurRefNo",
#                 "PurNo",
#                 "ClientCode",
#                 "DebtorCode",
#                 "TrnsDate",
#                 "DocDate",
#                 "Currency",
#                 "FacFeeAmt",
#                 "FacFeeWht",
#                 "UpdateBy",
#                 "UpdateDate",
#                 "UpdateTime",
#                 "Version",
#                 "ClassLevel",
#                 "MaturityDate",
#                 "CloseDate",
#                 "AREReverseAR",
#                 "AREReverseAP",
#                 "PrevAREReverseAR",
#                 "PrevAREReverseAP",
#             ],
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_oracle_table_d_{FILE_POS_DT}.csv",
#             [
#                 "kb_finality_code",
#                 "kb_finality_desc",
#                 "kb_finality_desc_en",
#                 "kb_is_active",
#                 "kb_order",
#                 "kb_pos_dt",
#                 "updated_dt",
#                 "updated_by",
#                 "created_dt",
#                 "created_by",

#             ],
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_db2_table_d_20240424.csv",
#             [
#                 "pxcommitdatetime",
#                 "pxcreatedatetime",
#                 "pxcreateopname",
#                 "pxcreateoperator",
#                 "pxcreatesystemid",
#                 "pxinsname",
#                 "pxobjclass",
#                 "pxupdatedatetime",
#                 "pxupdateopname",
#                 "pxupdateoperator",
#                 "pxupdatesystemid",
#                 "pylabel",
#                 "pyrulesetname",
#                 "pzinskey",
#                 "pzpvstream",
#                 "hopname",
#                 "hopnamereport",
#                 "previoushopname",
#                 "servicetype",
#                 "hopnumber",
#                 "incomingpercent",
#                 "display",
#                 "productgroup",
#                 "productgroupth",
#                 "servicegroup",
#                 "servicegroupreport",
#             ],
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mongodb_csv_table_d_20240424.csv",
#             [
#                 "_id",
#                 "campaign_id",
#                 "cis_id",
#                 "campaign_name",
#                 "tier_tier_id",
#                 "tier_tier_name",
#                 "tier_start_progress",
#                 "tier_end_progress",
#                 "tier_unit",
#                 "tier_point_amount",
#                 "tier_group_id",
#                 "tier_remain",
#             ],
#         ),
#     ],
#     ids=[
#         "Check mssql extracted file",
#         "Check oracle extracted file",
#         "Check db2 extracted file",
#         "Check mongodb extracted csv file",
#     ],
# )
# def test_validate_local_storage_extracted_file_format(extracted_file, expected_header) -> None:
#     """Method to test data extraction extracted file exist."""
#     assert validate_csv_header(
#         extracted_file, expected_header
#     ), f"Extracted file {extracted_file} does not match expected header {expected_header}"


# @pytest.mark.parametrize(
#     "extracted_file, expected_keys",
#     [
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mongodb_json_table_d_20240424.json",
#             [
#                 "_id",
#                 "campaign_id",
#                 "cis_id",
#                 "campaign_name",
#                 "tier_tier_id",
#                 "tier_tier_name",
#                 "tier_start_progress",
#                 "tier_end_progress",
#                 "tier_unit",
#                 "tier_point_amount",
#                 "tier_group_id",
#                 "tier_remain",
#             ],
#         ),
#     ],
#     ids=["Check mongodb extracted json file"],
# )
# def test_validate_local_storage_extracted_json_format(extracted_file, expected_keys) -> None:
#     """Method to test data extraction extracted JSON file format."""
#     with open(extracted_file, "r") as file:
#         data = json.load(file)
#         assert all(
#             key in data[0] for key in expected_keys
#         ), f"Extracted JSON file {extracted_file} does not match expected keys {expected_keys}"


# @pytest.mark.parametrize(
#     "extracted_file, expected_result",
#     [
#         (f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_table_zero_true_19900101.csv", True),
#         (f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_table_zero_overwrite_19900101.csv", True),
#         (f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_table_zero_false_19900101.csv", False),
#     ],
#     ids=[
#         "Extracted file with zero record from job with allow zero flag is True",
#         "Extracted file with zero record from job with allow zero flag is True (from overwrite config)",
#         "Extracted file with zero record from job with allow zero flag is False",
#     ],
# )
# def test_allow_zero_record_file(extracted_file, expected_result) -> None:
#     """Method to test data extraction extracted file with zero record exist.

#     Parametrize:
#         1. Expecting file to be generated
#         2. Not expecting file to be generated
#     """
#     assert (
#         validate_local_file_exists(extracted_file) == expected_result
#     ), f"Extracted file {extracted_file} does not match expected behavior with allow zero record: {expected_result}"


# @pytest.mark.parametrize(
#     "control_file",
#     [
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mssql_table_d_20240228.ctl",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_oracle_table_d_{FILE_POS_DT}.ctl",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_db2_table_d_20240424.ctl",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mongodb_csv_table_d_20240424.ctl",
#     ],
#     ids=[
#         "Check mssql control file",
#         "Check oracle control file",
#         "Check db2 control file",
#         "Check mongodb control file",
#     ],
# )
# def test_control_file_exist(control_file) -> None:
#     """Method to test data extraction control_file exist."""
#     assert validate_local_file_exists(
#         control_file
#     ), f"Local storage control file {control_file} does not exist"


# @pytest.mark.parametrize(
#     "control_file",
#     [
#         (f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mssql_table_d_20240228.ctl"),
#         (f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_oracle_table_d_{FILE_POS_DT}.ctl"),
#         (f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_db2_table_d_20240424.ctl"),
#         (f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mongodb_csv_table_d_20240424.ctl"),
#     ],
#     ids=[
#         "Check mssql control file",
#         "Check oracle control file",
#         "Check db2 control file",
#         "Check mongodb control file",
#     ],
# )
# def test_control_file_record_count(control_file) -> None:
#     """Method to test data extraction control file record count match with expected."""
#     record_count = get_csv_column_value(control_file, "record_count")
#     assert (
#         record_count > "1"
#     ), f"Control file {control_file} does not match expected record count (more than 1 record)"


# @pytest.mark.parametrize(
#     "control_file, expected_header",
#     [
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mssql_table_d_20240228.ctl",
#             ["record_count", "timestamp", "pos_date"],
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_oracle_table_d_{FILE_POS_DT}.ctl",
#             ["record_count", "timestamp", "pos_date"],
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_db2_table_d_20240424.ctl",
#             ["record_count", "timestamp", "pos_date"],
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_mongodb_csv_table_d_20240424.ctl",
#             ["record_count", "timestamp", "pos_date"],
#         ),
#     ],
#     ids=[
#         "Check mssql control file",
#         "Check oracle control file",
#         "Check db2 control file",
#         "Check mongodb control file",
#     ],
# )
# def test_validate_control_file_format(control_file, expected_header) -> None:
#     """Method to test data extraction extracted file exist."""
#     assert validate_csv_header(
#         control_file, expected_header
#     ), f"Control file {control_file} does not match expected header {expected_header}"


# @pytest.mark.parametrize(
#     "expected_file, job_name",
#     [
#         ("extrct_sit_trf_test_d_20240101.csv", "extrct_trf_fwtest_extrct_sit_trf_test_d"),
#         ("extrct_sit_mssql_table_d_20240228_part-0.csv", "extrct_trf_fwtest_mssql_sample_sit_d"),
#         (f"extrct_sit_oracle_table_d_{FILE_POS_DT}.csv", "extrct_trf_fwtest_oracle_sample_sit_d"),
#         ("extrct_sit_db2_table_d_20240424.csv", "extrct_trf_fwtest_db2_sample_sit_d"),
#         (
#             "extrct_sit_mongodb_csv_table_d_20240424.csv",
#             "extrct_trf_fwtest_mongodb_csv_sample_sit_d",
#         ),
#         (
#             "extrct_sit_mongodb_json_table_d_20240424.json",
#             "extrct_trf_fwtest_mongodb_json_sample_sit_d",
#         ),
#     ],
#     ids=[
#         "Check mssql transfer only expected_file file",
#         "Check mssql expected_file file",
#         "Check oracle expected_file file",
#         "Check db2 expected_file file",
#         "Check mongodb expected_file file",
#         "Check mongodb expected_file file",
#     ],
# )
# def test_adls_file_exists(expected_file, job_name) -> None:
#     """Method to test if raw table contains record."""
#     storage_url = (
#         f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy/"
#     )
#     adls_files = validate_file_exists(storage_url=storage_url, sas_token=SAS_TOKEN)
#     assert (
#         expected_file in adls_files
#     ), f"The return file listed from the storage: {adls_files} does not contain the expected file: {expected_file}"


# @pytest.mark.parametrize(
#     "leftover_files, job_name",
#     [
#         (
#             [
#                 "extrct_sit_mssql_table_d_20240228_part-2.csv",
#                 "extrct_sit_mssql_table_d_20240228_part-3.csv",
#                 "extrct_sit_mssql_table_d_20240228_part-4.csv",
#                 "extrct_sit_mssql_table_d_20240228_part-5.csv",
#             ],
#             "extrct_trf_fwtest_mssql_sample_sit_d",
#         ),
#     ],
#     ids=["Check mssql leftover local files"],
# )
# def test_leftover_files_not_exist_local(leftover_files, job_name) -> None:
#     """Method to test if the leftover files on local storage have been cleaned up before
#     extraction."""

#     # Use os.listdir to get only the file list in the extraction location
#     local_files = os.listdir(f"{FW_TEST_SOURCE_FILE_DIR}/test_area/")

#     assert not any(
#         file in leftover_files for file in local_files
#     ), f"The return files listed from the local storage: {local_files} contains files in leftover files: {leftover_files}"


# @pytest.mark.parametrize(
#     "leftover_files, job_name",
#     [
#         (
#             [
#                 "extrct_sit_mssql_table_d_20240228_part-2.csv",
#                 "extrct_sit_mssql_table_d_20240228_part-3.csv",
#                 "extrct_sit_mssql_table_d_20240228_part-4.csv",
#                 "extrct_sit_mssql_table_d_20240228_part-5.csv",
#             ],
#             "extrct_trf_fwtest_mssql_sample_sit_d",
#         ),
#         (["extrct_sit_trf_test_d_20240101_2.csv"], "extrct_sit_trf_test_folder_d"),
#     ],
#     ids=["Check mssql leftover ADLS files", "Check folder transfer leftover adls files"],
# )
# def test_leftover_files_not_exist_adls(leftover_files, job_name) -> None:
#     """Method to test if the leftover files on ADLS have been cleaned up before
#     transfer."""
#     storage_url = (
#         f"https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER_NAME}/{FILEPATH}/fw_test/azcopy/"
#     )
#     adls_files = validate_file_exists(storage_url=storage_url, sas_token=SAS_TOKEN)
#     assert not any(
#         file in leftover_files for file in adls_files
#     ), f"The return files listed from the storage: {adls_files} contains files in leftover files: {leftover_files}"


# @pytest.mark.parametrize(
#     "unzipped",
#     [
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/_tmp_test_compress/encrypted_file.txt",
#     ],
#     ids=[
#         "Check unzip file existed",
#     ],
# )
# def test_unzipped_file_exists(unzipped) -> None:
#     """Method to test data extraction unzipped exist."""
#     assert validate_local_file_exists(
#         unzipped
#     ), f"Local storage extracted zip file {unzipped} does not exist"


# @pytest.mark.parametrize(
#     "decrypted_file",
#     [
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/pgp_encrypted_file_decrypted.txt",
#         f"{FW_TEST_SOURCE_FILE_DIR}/test_area/_tmp_test_compress/encrypted_file_decrypted.txt",
#     ],
#     ids=[
#         "Check decrypted pgp file existed",
#         "Check unzip decrypted pgp file existed",
#     ],
# )
# def test_decrypted_file_exists(decrypted_file) -> None:
#     """Method to test data extraction decrypted_file exist."""
#     assert validate_local_file_exists(
#         decrypted_file
#     ), f"Local storage decrypted pgp file {decrypted_file} does not exist"


# @pytest.mark.parametrize(
#     "key_file_path, expected_header_col, expected_key_count",
#     [
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_hsm_key_header_d_20240902.key",
#             ["date_of_key", "date_of_generated_key", "encrypted_key", "data_file_name"],
#             1,
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/extrct_sit_hsm_key_body_d_20240806.key",
#             ["date_of_key", "date_of_generated_key", "encrypted_key", "hsm_key", "data_file_name"],
#             1,
#         ),
#         (
#             f"{FW_TEST_SOURCE_FILE_DIR}/test_area/_tmp_extrct_sit_hsm_key_body/extrct_sit_hsm_key_body_20240806.key",
#             ["date_of_key", "date_of_generated_key", "encrypted_key", "hsm_key", "data_file_name"],
#             8,
#         ),
#     ],
#     ids=[
#         "Check header and record count of key file (key in header)",
#         "Check header and record count of key file (key in body)",
#         "Check header and record count of key file (zip file)",
#     ],
# )
# def test_header_and_record_count_key_file(
#     key_file_path, expected_header_col, expected_key_count
# ) -> None:
#     """Method to test if the header and record count of key file are correct."""
#     with open(key_file_path, mode="r") as file:
#         reader = csv.reader(file, delimiter="|")
#         headers = next(reader)  # Get the headers (first row)

#         # Count the records (rows) in the file
#         record_count = sum(1 for row in reader)

#     # Check if header is correct
#     if headers == expected_header_col:
#         header_check = True
#     else:
#         header_check = False

#     if record_count == expected_key_count:
#         count_check = True
#     else:
#         count_check = False

#     assert header_check, f"Header is not match in {key_file_path}."
#     assert count_check, f"Key count is not match in {key_file_path}."


# def test_run_only_azcopy():
#     """Method to assert log when running only azcopy task by checking any .log file in
#     the directory."""

#     job_name = "extrct_sit_run_only_azcopy"
#     pos_dt = "1990-01-01"

#     directory, _ = get_log_filename(job_name, pos_dt)

#     assert os.path.exists(directory), f"Directory not found: {directory}"

#     log_files = glob.glob(os.path.join(directory, "*.log"))

#     assert log_files, f"No log files found in directory: {directory}"

#     for log_file_path in log_files:
#         with open(log_file_path, "r") as log_file:
#             log_content = log_file.read()

#             assert (
#                 "Starting execution of AzCopyDataTransferTask." in log_content
#             ), f"AzCopyDataTransferTask execution not found in {log_file_path}."

#             starting_count = log_content.count("Starting")
#             assert starting_count == 2, f"Log {log_file_path} does not contain the word 'Starting'."


# def test_run_only_extract_and_gen_ctl():
#     """Method to assert log when running only extract data task and generate control
#     file task by checking any .log file in the directory."""

#     job_name = "extrct_sit_run_only_extract_and_ctl"
#     pos_dt = "1990-01-01"

#     directory, _ = get_log_filename(job_name, pos_dt)

#     assert os.path.exists(directory), f"Directory not found: {directory}"

#     log_files = glob.glob(os.path.join(directory, "*.log"))

#     assert log_files, f"No log files found in directory: {directory}"

#     for log_file_path in log_files:
#         with open(log_file_path, "r") as log_file:
#             log_content = log_file.read()

#             assert (
#                 "Starting execution of OdbcDataExtractorTask." in log_content
#             ), f"OdbcDataExtractorTask execution not found in {log_file_path}."

#             assert (
#                 "Starting execution of OdbcControlFileGeneratorTask." in log_content
#             ), f"OdbcControlFileGeneratorTask execution not found in {log_file_path}."

#             starting_count = log_content.count("Starting")
#             assert starting_count == 3, f"Log {log_file_path} does not contain the word 'Starting'."
