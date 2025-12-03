"""AzCopy Data Transfer Module."""

# import: standard
import glob
import json
import os
import re
from pathlib import Path
from typing import List
from typing import Optional

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    DataFileInformation,
)
from mdp.framework.mdp_extraction_framework.task.data_transfer.base_data_transfer import (
    BaseDataTransferTask,
)
from mdp.framework.mdp_extraction_framework.utility.common.file_utils import cleanup_files
from mdp.framework.mdp_extraction_framework.utility.common_function import get_class_object
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import CommandResult
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
from pydantic import BaseModel
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential

MAX_AZCOPY_RETRY = 5
AZCOPY_CAP_MBPS = 150
os.environ["AZCOPY_DISABLE_SYSLOG"] = "true"


class LocalLocation(BaseModel, extra="allow"):  # type: ignore[call-arg]
    """Pydantic class for local location's parameters."""

    type: str
    filepath: str


class ADLSLocation(BaseModel, extra="allow"):  # type: ignore[call-arg]
    """Pydantic class for ADLS location's parameters."""

    type: str
    account_name: str
    container_name: str
    sas_token: str
    filepath: str
    filepath_without_token: Optional[str] | None = ""
    cleanup_file_pattern: str

    def update_adls_filepath_url(self, auth_mode: str = "sas"):
        """Function for getting the ADLS storage url from each attribute."""
        storage_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{self.filepath}"
        if auth_mode == "sas":
            storage_url_token = f"{storage_url}?{self.sas_token}"
        else:
            storage_url_token = f"{storage_url}"
        return self.model_copy(
            update={"filepath_without_token": storage_url, "filepath": storage_url_token}
        )


class DataTransferTaskConfigModel(BaseModel):
    """Pydantic class to validate the DataTransferTask.

    Args:
        BaseModel: pydantic base model
    """

    azcopy_command: str
    target: dict
    source: Optional[dict] | None = {}
    cleanup_dest_flag: Optional[str] | None = "True"
    cleanup_options: Optional[str] | None = ""
    azcopy_options: Optional[str] | None = ""
    allow_empty_file: Optional[str] | None = "False"
    allow_zero_file: Optional[str] | None = "False"
    archive_flag: Optional[str] | None = "False"
    archive_path: Optional[str] | None = ""
    cleanup_source_flag: Optional[str] | None = "False"
    auth_mode: Optional[str] | None = "sas"


def check_file_exists(file_path):
    result = os.path.exists(file_path)
    return result


def validate_transfer_file(
    azcopy_output: CommandResult,
    retry_count: int = 0,
    ignore_message: bool = False,
    allow_empty_file: str = "False",
    allow_zero_file: str = "False",
    file_exists: bool = False,
) -> None:
    """Validate if the transfer process is successful. The process is considered
    successful if Azcopy returns with exit code 0 and the number of transferred file is
    not 0.

    Args:
        azcopy_output (CommandResult): Output from the Azcopy run by shell.
        retry_count (int, optional): Number of time that have been retried. Defaults to 0.
        ignore_message (bool, optional): ignore message flag. Defaults to False.

    Raise:
        ValueError: raise value error if the transferred file process is not successful.
    """
    # list of failure cases to be detected from the Azcopy output's string
    failure_cases = []
    if allow_zero_file == "True":
        if file_exists:
            failure_cases.append("Total Number of Transfers: 0")
            if allow_empty_file == "False":
                failure_cases.append("TotalBytesTransferred: 0")
    else:
        failure_cases.append("Total Number of Transfers: 0")
        if allow_empty_file == "False":
            failure_cases.append("TotalBytesTransferred: 0")

    if azcopy_output.exit_code != 0:
        skip_error = False
        if allow_zero_file == "True":
            if file_exists is not True:
                if "no such file or directory" in azcopy_output.output:
                    skip_error = True

        if skip_error is not True:
            raise ValueError(
                f"Azcopy command returned with exit_code: {azcopy_output.exit_code}.\nRetry count: {retry_count} \nOutput: \n{azcopy_output.output}\nError_message: \n{azcopy_output.error}"
            )

    if ignore_message is False:
        detected_errors = [
            failure_case for failure_case in failure_cases if failure_case in azcopy_output.output
        ]
        if len(detected_errors) > 0:
            raise ValueError(
                f"Detected failure cases from the transfer process. \nRetry count: {retry_count} \nOutput: \n{azcopy_output.output}\nThe matched failure cases are: {detected_errors}."
            )


def validate_archive_path(archive_path: str) -> None:
    """Validate that the archive path exists and is a directory.

    Args:
        archive_path (str): The archive path to validate.

    Raises:
        ValueError: If the archive path is invalid or does not exist.
    """
    path = Path(archive_path)
    if not path.exists():
        raise ValueError(f"Archive path does not exist: {archive_path}")
    if not path.is_dir():
        raise ValueError(f"Archive path is not a directory: {archive_path}")


class AzCopyDataTransferTask(BaseDataTransferTask):
    """Class for transfer files data using the AzCopy tool."""

    parameter_config_model = DataTransferTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
        file_infos: List[DataFileInformation] = None,
    ):
        """Initializes a AzCopyDataTransferTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
            file_infos (List[DataFileInformation]): A list containing paths of files to transfer.
        """
        super().__init__(module_config, job_parameters)
        self.copy_retry_count = 0
        self.cleanup_retry_count = 0
        self.files_to_transfer = (
            [file_info.file_location for file_info in file_infos] if file_infos else None
        )

    @retry(
        wait=wait_exponential(multiplier=1.5, min=20, max=300),
        stop=stop_after_attempt(MAX_AZCOPY_RETRY),
        reraise=True,
    )
    def azcopy_transfer_file(
        self,
        data_source_location: str,
        data_target_location: str,
        azcopy_command: str = "cp",
        azcopy_options: str = "",
        allow_empty_file: str = "False",
        allow_zero_file: str = "True",
    ) -> List[str]:
        """
        Transfer files from a source location to a target location using AzCopy and
        return a list of successfully transferred files.

        This function wraps the AzCopy command with a retry mechanism to ensure robust
        data transfer. It inspects AzCopy JSON-formatted stdout to determine the outcome
        of the transfer operation, including the total number of files processed,
        the number successfully completed, and whether any failures occurred.

        Behaviour:
            * If AzCopy transfers one or more files successfully, the function returns a
            list of the planned source file paths.
            * If AzCopy performs zero transfers (e.g. wildcard did not match files,
            destination up-to-date, or no files to process), the function returns an
            empty list and does NOT raise an exception.
            * If AzCopy reports failed transfers, a RuntimeError is raised and the retry
            policy may re-attempt the transfer.
            * The retry policy is configured via the @retry decorator and applies only
            when exceptions are raised (e.g. connection issues, authentication errors,
            or explicit AzCopy failures).

        Args:
            data_source_location (str):
                The source path for the files to transfer. Supports local paths, remote
                URLs, or wildcard patterns (e.g. '*.csv').
            data_target_location (str):
                The Azure Data Lake Storage (ADLS) or blob storage location where files
                will be transferred.
            azcopy_command (str, optional):
                The AzCopy command to execute. Defaults to "cp" for file copy operations.
                Other commands such as "sync" may also be used if supported by AzCopy.
            azcopy_options (str, optional):
                Additional command-line options to pass to AzCopy, such as
                '--recursive', '--overwrite=ifSourceNewer', or performance tuning flags.
            allow_empty_file (str, optional):
                Flag passed through to `validate_transfer_file` to determine whether a
                zero-byte transferred file is considered valid. Defaults to "False".
            allow_zero_file (str, optional):
                Controls how zero-transfer scenarios are handled. When "True", zero
                transfers are treated as success and the function returns an empty list
                rather than raising an exception. Defaults to "True".

        Returns:
            List[str]:
                A list of file paths that were successfully transferred. For local
                sources, this corresponds to the resolved absolute paths of the planned
                files. For remote sources without local file resolution, this contains
                the original `data_source_location` entry. An empty list is returned when
                no file transfers were performed.

        Raises:
            FileNotFoundError:
                Raised when planned local source files do not exist.
            RuntimeError:
                Raised when AzCopy reports one or more failed transfers.
            Any exception raised by `validate_transfer_file` or the retrying logic if
                AzCopy encounters operational errors (authentication, network, exit code
                issues, etc.).

        Notes:
            * The retry behaviour is implemented using `tenacity` and applies only when
            exceptions are raised.
            * `--output-type=json` is automatically appended to the AzCopy command to
            enable structured parsing of progress and result details.
            * This function does not invoke 'azcopy jobs show' and relies solely on the
            structured progress messages emitted by AzCopy for determining success.
        """
        self.logger.info(f"Start AzCopy file transfer. Retry count: {self.copy_retry_count}")
        self.copy_retry_count += 1

        # -------------------------------------------------------------------------
        # 1) Build planned source_files list (local paths or the original source)
        # -------------------------------------------------------------------------
        if any(ch in data_source_location for ch in ["*", "?", "[", "]"]):
            # Local glob pattern
            source_files = [str(Path(p).resolve()) for p in glob.glob(data_source_location)]
            if not source_files:
                self.logger.info(
                    f"AzCopy source pattern '{data_source_location}' matched 0 local files; "
                    "calling AzCopy anyway and will accept 0 transfers."
                )
                source_files = []
        else:
            p = Path(data_source_location)
            if p.exists():
                p = p.resolve()
                if p.is_dir():
                    # Caller can control recursion with azcopy_options
                    source_files = [str(x.resolve()) for x in p.iterdir() if x.is_file()]
                else:
                    source_files = [str(p)]
            else:
                # Remote URL or non-local path – let AzCopy handle it.
                source_files = [data_source_location]

        self.logger.debug(f"Planned files to transfer ({len(source_files)}): {source_files}")

        # Optional local-existence check if you only expect local paths
        if all(not f.startswith("http") for f in source_files):
            missing = [f for f in source_files if not Path(f).exists()]
            if missing:
                raise FileNotFoundError(f"Planned source files not found: {missing}")

        # -------------------------------------------------------------------------
        # 2) Run AzCopy with JSON output
        # -------------------------------------------------------------------------
        cmd = (
            f"azcopy {azcopy_command} '{data_source_location}' '{data_target_location}' "
            f"{azcopy_options} --cap-mbps={AZCOPY_CAP_MBPS} --output-type=json"
        )
        command_result = run_command(command=cmd)

        # -------------------------------------------------------------------------
        # 3) Global validation (exit code, zero transfers, etc.)
        # -------------------------------------------------------------------------
        validate_transfer_file(
            azcopy_output=command_result,
            retry_count=self.copy_retry_count - 1,
            allow_empty_file=allow_empty_file,
            allow_zero_file=allow_zero_file,
            file_exists=True,
        )

        self.logger.info(f"AzCopy stdout:\n{command_result.output}")

        # -------------------------------------------------------------------------
        # 4) Parse summary from AzCopy JSON-line stdout
        # -------------------------------------------------------------------------
        total: Optional[int] = None
        completed: Optional[int] = None
        failed: Optional[int] = None
        job_status: Optional[str] = None

        stdout = command_result.output or ""

        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue

            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue

            if evt.get("MessageType") not in ("Progress", "EndOfJob"):
                continue

            msg = evt.get("MessageContent")
            if not isinstance(msg, str):
                continue

            try:
                inner = json.loads(msg)
            except json.JSONDecodeError:
                continue

            if "TotalTransfers" in inner:
                try:
                    total = int(inner["TotalTransfers"])
                except (TypeError, ValueError):
                    pass

            if "TransfersCompleted" in inner:
                try:
                    completed = int(inner["TransfersCompleted"])
                except (TypeError, ValueError):
                    pass

            if "TransfersFailed" in inner:
                try:
                    failed = int(inner["TransfersFailed"])
                except (TypeError, ValueError):
                    pass

            if "JobStatus" in inner:
                job_status = inner["JobStatus"]

        self.logger.info(
            f"AzCopy summary: JobStatus={job_status}, "
            f"TotalTransfers={total}, TransfersCompleted={completed}, TransfersFailed={failed}"
        )

        # -------------------------------------------------------------------------
        # 5) Behaviour check
        # -------------------------------------------------------------------------
        # 5.1 No files transferred → success, return empty list (no retry)
        if total == 0:
            self.logger.info(
                "AzCopy reported TotalTransfers=0; treating as successful no-op and returning []."
            )
            return []

        # 5.2 If AzCopy reports failures → raise so @retry can re-attempt
        if failed is not None and failed > 0:
            raise RuntimeError(
                f"AzCopy reported failed transfers: TransfersFailed={failed} / TotalTransfers={total}"
            )

        # 5.3 If counts are slightly odd, warn but still return planned list
        if completed is not None and total is not None and completed != total:
            self.logger.warning(
                f"AzCopy TransfersCompleted ({completed}) != TotalTransfers ({total}). "
                "Returning planned source_files; check logs if this is unexpected."
            )

        # If we reach here, we treat all planned files as successfully transferred
        return source_files

    @retry(
        wait=wait_exponential(multiplier=1.5, min=20, max=300),
        stop=stop_after_attempt(MAX_AZCOPY_RETRY),
        reraise=True,
    )
    def azcopy_cleanup_file(
        self,
        cleanup_filepath: str,
        cleanup_file_pattern: str,
        cleanup_options: str,
        sas_token: str = None,
        azcopy_command: str = "rm",
        auth_mode: str = "sas",
    ) -> None:
        """Cleanup files at the destination ADLS before transfer, to support rerunning
        multiple files.

        Args:
            cleanup_filepath (str): The file path on ADLS for cleaning up existing files
            cleanup_file_pattern (str): The file pattern to search for files to be cleaned up
            cleanup_options (str): The azcopy's options for the 'rm' command
            sas_token (str): The SAS Token of the target ADLS
            azcopy_command (str): A command listed in AzCopy's available commands, default as "cp" for copy
        """
        # Log the retry count of file cleanup
        self.logger.info(f"Start Azcopy file cleanup. Retry count: {self.cleanup_retry_count}")
        self.cleanup_retry_count += 1

        cleanup_file_pattern = os.path.basename(cleanup_file_pattern)
        # Run azcopy command to cleanup existing files
        if auth_mode == "sas":
            cleanup_command = f"azcopy rm '{cleanup_filepath}?{sas_token}' --include-pattern '{cleanup_file_pattern}' {cleanup_options}"
            # Log cleanup command without sas_token
            self.logger.info(
                f"Cleanup command: azcopy rm '{cleanup_filepath}?<sas_token>' --include-pattern '{cleanup_file_pattern}'"
            )

        if auth_mode == "service_principal":
            cleanup_command = f"azcopy rm '{cleanup_filepath}' --include-pattern '{cleanup_file_pattern}' {cleanup_options}"
            # Log cleanup command without sas_token
            self.logger.info(
                f"Cleanup command: azcopy rm '{cleanup_filepath}' --include-pattern '{cleanup_file_pattern}'"
            )

        command_result = run_command(command=cleanup_command)

        # Validate the output of the azcopy command
        self.logger.info(f"Output from the azcopy command: \n{command_result.output}")

    def execute(self) -> str:
        """The method that orchestrates the data transfer process.

        Processes the source and target's location based on their type, and call the
        'azcopy' command to transfer the data.

        Returns:
            str: target file location
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")

        if self.module_config.source:
            # Validate source config using the class from the source's type
            source_validate_class = get_class_object(__name__, self.module_config.source["type"])
            source_config = source_validate_class(**self.module_config.source)
            if self.module_config.source["type"] == "ADLSLocation":
                source_config = source_config.update_adls_filepath_url(self.module_config.auth_mode)
            source_configs = [source_config]
        elif self.files_to_transfer:
            source_configs = [
                LocalLocation(filepath=file, type="LocalLocation")
                for file in self.files_to_transfer
            ]
        else:
            err_msg = "Please input Source parameter in config"
            self.logger.error(err_msg)
            raise ValueError(err_msg)

        for source_config in source_configs:
            self.logger.info(f"Peforming transfer on source config file: {source_config.filepath}")
            # Validate target config using the class from the source's type
            target_validate_class = get_class_object(__name__, self.module_config.target["type"])
            target_configs = target_validate_class(
                **self.module_config.target,
                cleanup_file_pattern=os.path.basename(source_config.filepath),
            )
            if self.module_config.target["type"] == "ADLSLocation":
                target_configs = target_configs.update_adls_filepath_url(
                    self.module_config.auth_mode
                )

            # Cleanup existing files with the same pattern on the destination
            if self.module_config.cleanup_dest_flag == "True":
                filepath_without_token = str(target_configs.filepath_without_token)
                self.azcopy_cleanup_file(
                    cleanup_filepath=str(target_configs.filepath_without_token),
                    cleanup_file_pattern=str(source_config.filepath),
                    cleanup_options=self.module_config.cleanup_options,
                    azcopy_command="rm",
                    sas_token=str(target_configs.sas_token),
                    auth_mode=self.module_config.auth_mode,
                )

                self.logger.info(
                    f"Cleaned up file with pattern: {target_configs.cleanup_file_pattern} completed."
                )

            # Execute data transfer
            success_file = self.azcopy_transfer_file(
                data_source_location=str(source_config.filepath),
                data_target_location=str(target_configs.filepath),
                azcopy_command=self.module_config.azcopy_command,
                azcopy_options=self.module_config.azcopy_options,
                allow_empty_file=self.module_config.allow_empty_file,
                allow_zero_file=self.module_config.allow_zero_file,
            )

            if self.module_config.cleanup_source_flag == "True":
                self.logger.info(f"Cleaning up source file: {source_config.filepath}")
                cleanup_files(success_file)

        self.logger.info(f"Execution of {self.__class__.__name__} completed.")

        return filepath_without_token
