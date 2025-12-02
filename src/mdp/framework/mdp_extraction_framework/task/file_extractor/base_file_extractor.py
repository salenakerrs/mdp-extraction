"""Base Data Unzip Module."""

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.base_task import Task


class BaseFileExtractorTask(Task):
    """Base Data Transfer Task, for data transfer both from and to MDP.

    Args:
        Task (class): The base class for defining tasks in the workflow.
    """

    def __init__(self, module_config: dict, job_parameters: JobParameters) -> None:
        """Initializes a BaseDataTransferTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration.
            job_parameters (JobParameters): An object containing job parameters.
        """
        super().__init__(module_config, job_parameters)
