"""Test Extraction Oper Log."""
# import: standard
from unittest.mock import patch

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.operation_log.extraction_oper_log import (
    DeltaTableOperation,
)
from mdp.framework.mdp_extraction_framework.operation_log.extraction_oper_log import (
    ExtractionPipelineOperLog,
)
from mdp.framework.mdp_extraction_framework.pipeline.extraction import (
    ExtractionPipelineExecutedValues,
)

# import: external
import pytest
from deltalake import DeltaTable
from deltalake import Schema

JOB_PARAM = JobParameters(
    job_name="example_job",
    pos_dt="2023-01-01",
    scheduler_id="scheduler_1",
    area_name="area_1",
    job_seq=1,
    config_file_path="",
)

SCHEMA = """
        {
            "type": "struct",
            "fields": [
                {"name": "job_nm", "type": "string", "nullable": false, "metadata": {}},
                {"name": "pos_dt", "type": "string", "nullable": false, "metadata": {}},
                {"name": "scheduler_id", "type": "string", "nullable": true, "metadata": {}},
                {"name": "job_start_datetime", "type": "timestamp_ntz", "nullable": true, "metadata": {}},
                {"name": "job_end_datetime", "type": "timestamp_ntz", "nullable": true, "metadata": {}},
                {"name": "job_status", "type": "string", "nullable": false, "metadata": {}},
                {"name": "job_message", "type": "string", "nullable": true, "metadata": {}},
                {"name": "area_nm", "type": "string", "nullable": true, "metadata": {}},
                {"name": "job_seq", "type": "integer", "nullable": true, "metadata": {}},
                {"name": "extract_file_path","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}},
                {"name": "target_file_path", "type": "string", "nullable": true, "metadata": {}},
                {"name": "files_size","type":{"type":"array","elementType":"integer","containsNull":true},"nullable":true,"metadata":{}}
            ]
        }
    """


@pytest.fixture
def delta_operation():
    """Fixture that provides a DeltaTableOperation instance for testing.

    Returns:
        DeltaTableOperation: The instance used for Delta table operations.
    """
    return DeltaTableOperation()


@pytest.fixture(scope="function")
def mock_tmp_path(tmp_path):
    """Temp path to store log table.

    Yields:
        Path: The path of the mock Delta table.
    """
    table_path = tmp_path / "log_table"
    yield table_path


@pytest.fixture(scope="function")
def create_mock_table(mock_tmp_path, delta_operation):
    """Fixture that creates and returns a mock Delta table for testing.

    Args:
        tmp_path (Path): The temporary path for creating the table.
        delta_operation (DeltaTableOperation): The DeltaTableOperation instance.
    """
    table_schema = Schema.from_json(SCHEMA)
    delta_operation.create_table_if_not_exist(
        table_uri=str(mock_tmp_path), schema=table_schema, partition_by=["pos_dt"]
    )


@pytest.fixture(scope="function")
def extraction_oper_log(mock_tmp_path):
    """Fixture that provides a ExtractionPipelineOperLog instance for testing.

    Returns:
        ExtractionPipelineOperLog: The instance used for Extraction oper log.
    """
    with patch.object(ExtractionPipelineOperLog, "table_uri", mock_tmp_path):
        # Initialize the pipeline log with the mock Delta table
        pipeline_log = ExtractionPipelineOperLog()
        yield pipeline_log


def test_create_log_table_if_not_exist(extraction_oper_log, mock_tmp_path):
    """Test the creation of a Delta table if it doesn't already exist.

    Args:
        tmp_path (Path): The temporary path where the table is created.
    """
    extraction_oper_log.create_log_table_if_not_exist()
    assert DeltaTable(str(mock_tmp_path))


def test_insert_log(mock_tmp_path, create_mock_table, extraction_oper_log):
    """Test inserting a log entry into the extraction pipeline operation log.

    Args:
        mock_table (Path): The path of the mock Delta table.
    """
    # Set up executed values
    executed_values = ExtractionPipelineExecutedValues(
        extract_file_path=["path/to/file1"], target_file_path="path/to/target", files_size=[100]
    )

    # Insert log
    extraction_oper_log.insert_log(
        job_parameters=JOB_PARAM,
        job_start_datetime="2023-01-01T00:00:00",
        job_end_datetime="2023-01-01T01:00:00",
        job_status="SUCCESS",
        job_message="Job completed successfully",
        executed_values=executed_values,
    )

    # Validate log was inserted
    loaded_df = DeltaTable(str(mock_tmp_path)).to_pandas()
    assert not loaded_df.empty
    assert loaded_df["job_nm"].iloc[0] == "example_job"
