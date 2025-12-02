"""Test Delta Table Utility."""
# import: standard
import os
from pathlib import Path

# import: internal
from mdp.framework.mdp_extraction_framework.operation_log.extraction_oper_log import (
    DeltaTableOperation,
)

# import: external
import pandas as pd
import pytest
from deltalake import DeltaTable
from deltalake import Schema

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


@pytest.fixture
def sample_dataframe():
    """Fixture that provides a sample DataFrame for testing Delta operations.

    Returns:
        pd.DataFrame: A DataFrame with mock data for testing.
    """
    return pd.DataFrame(
        {
            "job_nm": ["example_job"],
            "pos_dt": ["2023-01-01"],
            "scheduler_id": ["scheduler_1"],
            "job_start_datetime": ["2023-01-01T00:00:00"],
            "job_end_datetime": ["2023-01-01T01:00:00"],
            "job_status": ["SUCCESS"],
            "job_message": ["Job completed successfully"],
            "area_nm": ["area_1"],
            "job_seq": [1],
            "extract_file_path": [["path/to/file1", "path/to/file2"]],
            "target_file_path": ["path/to/target"],
            "files_size": [[100, 200]],
        }
    )


def test_create_table_if_not_exist(delta_operation, tmp_path):
    """Test the creation of a Delta table if it doesn't already exist.

    Args:
        delta_operation (DeltaTableOperation): The instance used to perform the Delta table operation.
        tmp_path (Path): The temporary path where the table is created.
    """
    table_path = tmp_path / "log_table"
    table_schema = Schema.from_json(SCHEMA)
    delta_operation.create_table_if_not_exist(
        table_uri=str(table_path), schema=table_schema, partition_by=["pos_dt"]
    )

    assert DeltaTable(str(table_path))  # Assert that the table exists


@pytest.fixture(scope="function")
def mock_table(tmp_path, delta_operation):
    """Fixture that creates and returns a mock Delta table for testing.

    Args:
        tmp_path (Path): The temporary path for creating the table.
        delta_operation (DeltaTableOperation): The DeltaTableOperation instance.

    Yields:
        Path: The path of the created mock Delta table.
    """
    table_path = tmp_path / "log_table"
    table_schema = Schema.from_json(SCHEMA)
    delta_operation.create_table_if_not_exist(
        table_uri=str(table_path), schema=table_schema, partition_by=["pos_dt"]
    )
    yield table_path


@pytest.fixture(scope="function")
def mock_write_data(mock_table, sample_dataframe, delta_operation):
    """Fixture that writes sample data to a mock Delta table for testing.

    Args:
        mock_table (Path): The path of the mock Delta table.
        sample_dataframe (pd.DataFrame): The sample DataFrame to write to the table.
        delta_operation (DeltaTableOperation): The DeltaTableOperation instance.
    """
    delta_operation.write_delta_table(
        delta_table=DeltaTable(mock_table),
        data=sample_dataframe,
        mode="append",
        partition_by=["pos_dt"],
    )


def test_write_delta_table(delta_operation, mock_table, sample_dataframe, mock_write_data):
    """Test writing data to a Delta table and validating the written content.

    Args:
        delta_operation (DeltaTableOperation): The instance used for Delta operations.
        mock_table (Path): The path of the mock Delta table.
        sample_dataframe (pd.DataFrame): The sample DataFrame to write to the table.
    """
    # Load data back to validate
    loaded_df = DeltaTable(mock_table).to_pandas()
    pd.testing.assert_frame_equal(
        sample_dataframe[["job_nm", "pos_dt", "job_status"]],
        loaded_df[["job_nm", "pos_dt", "job_status"]],
    )


def test_load_table_as_df(delta_operation, mock_table, sample_dataframe, mock_write_data):
    """Test loading data from a Delta table and validating the content.

    Args:
        delta_operation (DeltaTableOperation): The instance used for Delta operations.
        mock_table (Path): The path of the mock Delta table.
        sample_dataframe (pd.DataFrame): The sample DataFrame to write to the table.
        mock_write_data (function): A fixture that writes data to the mock table.
    """
    loaded_df = delta_operation.load_table_as_df(DeltaTable(mock_table))
    pd.testing.assert_frame_equal(
        sample_dataframe[["job_nm", "pos_dt", "job_status"]],
        loaded_df[["job_nm", "pos_dt", "job_status"]],
    )


@pytest.fixture(scope="function")
def mock_write_multiple_data(mock_table, sample_dataframe, delta_operation):
    """Fixture that writes multiple entries of sample data to a mock Delta table.

    Args:
        mock_table (Path): The path of the mock Delta table.
        sample_dataframe (pd.DataFrame): The sample DataFrame to write to the table.
        delta_operation (DeltaTableOperation): The DeltaTableOperation instance.
    """
    for i in range(3):
        delta_operation.write_delta_table(
            delta_table=DeltaTable(mock_table),
            data=sample_dataframe,
            mode="append",
            partition_by=["pos_dt"],
        )


def test_is_num_files_over_threshold(delta_operation, mock_table, mock_write_multiple_data):
    """Test checking if the number of files in a Delta table exceeds a given threshold.

    Args:
        delta_operation (DeltaTableOperation): The instance used for Delta operations.
        mock_table (Path): The path of the mock Delta table.
        mock_write_multiple_data (function): A fixture that writes data to the mock table.
    """
    result = delta_operation.is_num_files_over_threshold(path=mock_table, file_threshold=2)
    assert result is True


def test_compact_and_clean_table(delta_operation, mock_table, mock_write_multiple_data, tmp_path):
    """Test compacting and cleaning a Delta table to reduce file count.

    Args:
        delta_operation (DeltaTableOperation): The instance used for Delta operations.
        mock_table (Path): The path of the mock Delta table.
        mock_write_multiple_data (function): A fixture that writes data to the mock table.
        tmp_path (Path): Temporary path for testing.
    """
    lock_file = Path(mock_table).parent / "mock.lock"
    delta_operation.compact_and_clean_table(
        delta_table=DeltaTable(mock_table), lock_file_path=lock_file
    )

    # Validate that there is only one file remaining in the table directory
    partition_path = os.path.join(mock_table, "pos_dt=2023-01-01")
    files_in_table = os.listdir(partition_path)
    assert len(files_in_table) == 1
    os.remove(lock_file)
