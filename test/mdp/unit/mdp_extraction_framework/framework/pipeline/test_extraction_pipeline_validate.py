"""Test Pipeline Task Validate Config."""

# import: internal
from mdp.framework.mdp_extraction_framework.pipeline.extraction import ExtractionPipeline
from mdp.framework.mdp_extraction_framework.pipeline.extraction import JobParameters

# import: external
import pytest

CONFIG = {
    "job_name": "exrt_azcopy_sample_sit",
    "area_name": "test_area",
    "job_seq": 1,
    "pipeline_name": "ExtractionPipeline",
    "job_info": {
        "data_source_name": "TEST_SOURCE",
        "file_name": "/Volumes/mdp{{ env }}/extraction/transfer_file/mdp/transfer_azcopy_sample_sit.txt",
        "data_target_type": "ADLSLocation",
        "data_target_location": "https://adlstestraw001.dfs.core.windows.net/raw/fw_test/azcopy/test_file.txt",
    },
    "tasks": {
        "azcopy_data_transfer_task": {
            "module_name": "AzCopyDataTransferTask",
            "bypass_flag": "false",
            "parameters": {
                "azcopy_command": "cp",
                "source": {
                    "type": "LocalLocation",
                    "filepath": "local_storage.filepath/test_area/extrct_sit_table_d_20240101.txt",
                },
                "target": {
                    "type": "ADLSLocation",
                    "storage_account": "mdp_inbnd.account_name",
                    "storage_container": "mdp_inbnd.container_name }}",
                    "sas_token": "mdp_inbnd.sas_token",
                    "filepath": "mdp_inbnd.filepath/fw_test/azcopy/extrct_sit_table_d_20240101.txt",
                },
            },
        }
    },
}

JOB_PARAMETER_MOCK = JobParameters(
    pos_dt="1999-10-01",
    config_file_path="mockpath",
    adb_job_id="",
    adb_run_id="",
    scheduler_id="",
    job_info={},
    job_name="",
    area_name="",
    job_seq=0,
    pipeline_name="",
)


def test_extraction_pipeline_validate_pipeline_config_pass():
    """Unit test to validate the successful configs of both pipeline config level and
    task config level."""
    config = CONFIG.copy()
    ExtractionPipeline(config=config, job_parameters=JOB_PARAMETER_MOCK)


def test_extraction_pipeline_validate_pipeline_config_fail():
    """Unit test to validate the incorrect config on the pipeline config level."""
    config = CONFIG.copy()
    config.pop("pipeline_name")
    with pytest.raises(Exception):
        ExtractionPipeline(config=config, job_parameters=JOB_PARAMETER_MOCK)
