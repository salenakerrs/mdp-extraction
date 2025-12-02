"""Zip File Extractor Module."""

# import: standard
from pathlib import Path
from typing import List
from typing import Optional

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    DataFileInformation,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    generate_data_file_info,
)
from mdp.framework.mdp_extraction_framework.task.file_extractor.base_file_extractor import (
    BaseFileExtractorTask,
)
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
from pydantic import BaseModel


class ZipFileExtractorTaskConfigModel(BaseModel):
    """Pydantic class to validate the DataTransferTask.

    Args:
        BaseModel: pydantic base model
    """

    source_file_location: str
    unzip_location: Optional[str] = ""


class ZipFileExtractorTask(BaseFileExtractorTask):
    """Class for extracting files from a zip archive."""

    parameter_config_model = ZipFileExtractorTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
    ):
        """Initializes a ZipFileExtractorTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
        """
        super().__init__(module_config, job_parameters)

    def make_tmp_dir(
        self,
        source_file_location: str,
    ) -> str:
        """Creates a temporary directory for extracting files.

        If no unzip_location is provided, creates a `_tmp_<filename>` directory
        in the same folder as the source file. Otherwise, creates the directory
        specified in `self.module_config.unzip_location`.

        Args:
            source_file_location (str): The path of the source zip file.

        Returns:
            str: The path of the newly created temporary directory.

        Raises:
            RuntimeError: If the command to create the directory fails.
        """
        if self.module_config.unzip_location.strip() == "":
            folder_location = Path(source_file_location).parent
            file_name = Path(source_file_location).stem
            tmp_folder_location = str(folder_location / f"_tmp_{file_name}")
        else:
            tmp_folder_location = self.module_config.unzip_location

        # remove existing dir
        rm_dir_command = f"rm -rf {tmp_folder_location}"
        self.logger.info(f"Remove existing directory command: {rm_dir_command}")
        run_command(command=rm_dir_command)

        # create dir
        mk_tmp_dir_command = f"mkdir -p {tmp_folder_location}"
        self.logger.info(f"Make directory command: {mk_tmp_dir_command}")
        command_result = run_command(command=mk_tmp_dir_command)

        if command_result.exit_code != 0:
            raise RuntimeError(
                f"Make directory failed with exit code: {command_result.exit_code}, "
                f"output: \n{command_result.output}, \nerror_message: \n{command_result.error}"
            )

        self.logger.info(f"Output from the make directory command: \n{command_result.output}")
        return tmp_folder_location

    def unzip_file(
        self,
        source_file_location: str,
        tmp_folder_location: str,
    ) -> None:
        """Unzips a file to the specified temporary directory.

        Args:
            source_file_location (str): The path of the source zip file.
            tmp_folder_location (str): The path of the temporary directory.

        Raises:
            RuntimeError: If the unzip command fails.
        """
        unzip_command = f"unzip {source_file_location} -d {tmp_folder_location}"
        self.logger.info(f"Unzip command: {unzip_command}")
        command_result = run_command(command=unzip_command)
        # Check if the command was successful
        if command_result.exit_code != 0:
            raise RuntimeError(
                f"Unzip command failed with exit code: {command_result.exit_code}, output: \n{command_result.output}, \nerror_message: \n{command_result.error}"
            )

        self.logger.info(f"Output from the unzip command: \n{command_result.output}")

    def list_files_in_folder(
        self,
        tmp_folder_location: str,
    ) -> List[str]:
        """Lists all files in the specified directory.

        Executes a shell command to find and list all files in the temporary folder.

        Args:
            tmp_folder_location (str): The path of the temporary directory.

        Returns:
            List[str]: A list of file paths within the directory.

        Raises:
            RuntimeError: If the list files command fails.
            Exception: If an error occurs while processing the command output.
        """
        list_files_command = f"find {tmp_folder_location} -type f"
        self.logger.info(f"List files command: {list_files_command}")
        command_result = run_command(command=list_files_command)
        # Check if the command was successful
        if command_result.exit_code != 0:
            raise RuntimeError(
                f"List files command failed with exit code: {command_result.exit_code}, output: \n{command_result.output}, \nerror_message: \n{command_result.error}"
            )

        self.logger.info(f"Output from the list files command: \n{command_result.output}")

        try:
            output_list = command_result.output.split("\n")
            # remove any empty strings from the list (in case of trailing newlines)
            output_list = [item for item in output_list if item]
            return output_list
        except Exception as e:
            self.logger.error(f"Error processing List files command output. {e}")
            raise e

    def execute(self) -> List[DataFileInformation]:
        """Executes the file extraction process.

        1. Creates a temporary directory.
        2. Unzips the source file into the temporary directory.
        3. Lists all the files in the temporary directory.

        Returns:
            List[DataFileInformation]: A list of paths to the unzipped files.
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")

        if self.module_config.unzip_location == "":
            upzip_location = self.make_tmp_dir(self.module_config.source_file_location)
        else:
            upzip_location = self.make_tmp_dir(self.module_config.unzip_location)

        self.unzip_file(self.module_config.source_file_location, upzip_location)
        unzipped_files_path = self.list_files_in_folder(upzip_location)
        file_infos = [generate_data_file_info(file) for file in unzipped_files_path]

        return file_infos
