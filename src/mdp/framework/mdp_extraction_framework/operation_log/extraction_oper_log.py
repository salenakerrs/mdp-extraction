"""Extraction Oper Log."""
# import: standard
import logging
import os
from typing import ClassVar
from typing import List

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.pipeline.extraction import (
    ExtractionPipelineExecutedValues,
)
from mdp.framework.mdp_extraction_framework.utility.delta_table.delta_table import (
    DeltaTableOperation,
)

# import: external
from deltalake import DeltaTable
from deltalake import Schema
from pandas import DataFrame
from pandas import to_datetime

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")


def get_log_config(project: str, env: str) -> dict:
    """Dynamically generate log configuration based on project and environment."""
    base_path = f"/app_log_{project}/{project}/fw_log/extraction"
    return {
        "table_name": f"{project}{env}.fw_log.extraction",
        "table_uri": base_path,
        "table_lock_file": f"{base_path}.lock",
    }


class ExtractionPipelineOperLog:
    """A class to manage logging of extraction pipeline operations to a Delta Table.

    Attributes:
        table_name (ClassVar[str]): Fully qualified name of the Delta table.
        table_uri (ClassVar[str]): URI of the Delta table storage location.
        table_lock_file (ClassVar[str]): Lock file path for concurrency.
        schema (ClassVar[str]): Schema definition of the Delta table in JSON format.
        partition_by (ClassVar[List[str]]): List of columns used to partition the table.
    """

    _project = os.getenv("PROJECT", "mdp").lower()
    _env = os.getenv("ENVIRONMENT", "dev")

    _log_config = get_log_config(_project, _env)

    table_name: ClassVar[str] = _log_config["table_name"]
    table_uri: ClassVar[str] = _log_config["table_uri"]
    table_lock_file: ClassVar[str] = _log_config["table_lock_file"]

    schema: ClassVar[
        str
    ] = """
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
                {"name": "files_size","type":{"type":"array","elementType":"long","containsNull":true},"nullable":true,"metadata":{}}
            ]
        }
    """

    # TODO: Check if partition_by is needed for the log table, originally was only partitioned by pos_dt
    partition_by: ClassVar[List[str]] = ["pos_dt"]

    def __init__(self) -> None:
        """Initialize `ExtractionPipelineOperLog` object and create the table if it does
        not exist."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.delta_table = DeltaTableOperation()

    def create_log_table_if_not_exist(self) -> None:
        """Creates a log table at the specified URI if it does not already exist."""
        table_schema = Schema.from_json(self.schema)
        self.delta_table.create_table_if_not_exist(
            table_uri=self.table_uri, schema=table_schema, partition_by=self.partition_by
        )

    def insert_log(
        self,
        job_parameters: JobParameters,
        job_start_datetime: str,
        job_end_datetime: str,
        job_status: str,
        job_message: str,
        executed_values: ExtractionPipelineExecutedValues,
    ) -> None:
        """Logs the job status and performs Delta table housekeeping if needed.

        Args:
            job_parameters (JobParameters): Parameters for the job execution.
            job_start_datetime (str): The start datetime of the job execution.
            job_end_datetime (str): The end datetime of the job execution.
            job_status (str): Status of the job (e.g., SUCCESS, FAILED).
            job_message (str): Additional details or messages about the job execution.
            executed_values (ExtractionPipelineExecutedValues): Execution-specific values for logging.
        """
        log_info = {
            "job_nm": job_parameters.job_name,
            "pos_dt": job_parameters.pos_dt,
            "scheduler_id": job_parameters.scheduler_id,
            "job_start_datetime": to_datetime(job_start_datetime),
            "job_end_datetime": to_datetime(job_end_datetime),
            "job_status": job_status,
            "job_message": job_message or "",
            "area_nm": job_parameters.area_name,
            "job_seq": job_parameters.job_seq,
            "extract_file_path": executed_values.extract_file_path or [""],
            "target_file_path": executed_values.target_file_path or "",
            "files_size": executed_values.files_size or [0],
        }
        log_records = DataFrame([log_info])

        self.delta_table.write_delta_table(
            delta_table=DeltaTable(self.table_uri),
            data=log_records,
            mode="append",
            predicate=f"pos_dt='{job_parameters.pos_dt}' and job_nm='{job_parameters.job_name}'",
            partition_by=self.partition_by,
        )
        self.logger.info(f"Job '{job_parameters.job_name}' status logged as '{job_status}'.")

    def housekeeping(self) -> None:
        """Perform housekeeping operations on the Delta table if the file count exceeds
        the threshold."""
        if self.delta_table.is_num_files_over_threshold(path=self.table_uri):
            self.delta_table.compact_and_clean_table(
                DeltaTable(self.table_uri), self.table_lock_file
            )
