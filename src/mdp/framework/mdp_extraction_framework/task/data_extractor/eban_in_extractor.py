"""Module for extracting source data using ODBC connections."""
# import: standard

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    BaseDataExtractorTask,
)
from mdp.framework.mdp_extraction_framework.utility.shell_script.common import run_command

# import: external
from pydantic import BaseModel
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential

MAX_RETRY = 5


class EBANInExtractorTaskConfigModel(BaseModel):
    """Pydantic class to validate the EBANInExtractor. Currently not require any
    parameter as the configs are used from the EBAN-IN's configuration file on the VM.

    Args:
        BaseModel: pydantic base model
    """

    pass


class EBANInExtractorTask(BaseDataExtractorTask):
    """Class for executing the EBAN-IN extraction script."""

    parameter_config_model = EBANInExtractorTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
    ):
        """Initializes a EBANInExtractorTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
        """
        super().__init__(module_config, job_parameters)

    @retry(
        wait=wait_exponential(multiplier=1.5, min=20, max=300),
        stop=stop_after_attempt(MAX_RETRY),
        reraise=True,
    )
    def execute_eban_in_script(
        self,
    ) -> None:
        """Execute the EBAN-IN shell script with scheduler_id (JOB_NAME) and pos_dt
        (POS_DT) as arguments."""
        # Execute the EBAN-IN Extraction script and validate the results if the script run is successful.
        self.logger.info(
            f"Executing EBAN-IN extraction script with scheduler_id: {self.job_parameters.scheduler_id}, pos_dt: {self.job_parameters.pos_dt}"
        )
        command_result = run_command(
            command=f"/app_mdp/mdp/script/extraction/foundation/mdp_extraction_foundation.sh {self.job_parameters.scheduler_id} {self.job_parameters.pos_dt}"
        )
        # validate_script_result(output=command_result)
        if command_result.exit_code != 0:
            raise ValueError(
                f"EBAN-IN extraction returned with exit_code: {command_result.exit_code}, output: \n{command_result.output}, \nerror_message: \n{command_result.error}"
            )
        self.logger.info(f"Output from the EBAN-IN script: {command_result.output}")

    def execute(self):
        """The method that orchestrates the EBAN-IN Extraction process.

        Execute the EBAN-IN extraction script using scheduler_id (JOB_NAME) and pos_dt
        (POS_DT) as arguments.
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")

        # Execute data transfer
        self.execute_eban_in_script()

        self.logger.info(f"Execution of {self.__class__.__name__} completed.")

        return None
