"""Module for extracting source data using SubmitCommandScript connections."""
# import: standard
from typing import Optional

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.preprocess.base_preprocess import (
    BasePreprocessTask,
)
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
from pydantic import BaseModel


class FileNameFormatTaskConfigModel(BaseModel):
    """Configuration model for defining file name format with sequence numbers and other
    file name if any.

    Attributes:
        base_file_name (str): The base name of the file.
        date_suffix (str): The date suffix to be appended to the file name.
    """

    base_file_name: str
    date_suffix: str


class SubmitCommandScriptTaskConfigModel(BaseModel):
    file_extension: Optional[str] = "ctl"
    shell_command: Optional[str] = None
    python_command: Optional[str] = None


class SubmitCommandScriptTask(BasePreprocessTask):
    """Class for extracting source data using SubmitCommandScript."""

    parameter_config_model = SubmitCommandScriptTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
    ):
        """Initializes a SourceDataExtractorTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
        """
        super().__init__(module_config, job_parameters)
        self.module_config = module_config
        self.job_parameters = job_parameters
        self.parameters = self.module_config.parameters

    def execute(self) -> None:
        """Executes Generate Ctl file process.

        Returns:
            Ctl file details (for logging)
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")

        if self.module_config.module_name.__name__ == "SubmitCommandScriptTask":
            if hasattr(self.parameters, "shell_command") and self.parameters.shell_command:
                command = self.parameters.shell_command
            elif hasattr(self.parameters, "python_command") and self.parameters.python_command:
                command = self.parameters.python_command

            if command:
                result = run_command(command)
                self.logger.info(f"Generate Control File {result}")
