"""Abstract Base Class for Task modules."""

# import: standard
import logging
from abc import ABC
from abc import abstractmethod
from typing import Any

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters


class Task(ABC):
    """Abstract base class for defining tasks.

    This class serves as a template for creating specific tasks in a pipeline.
    Subclasses must implement the abstract method execute() to define the actual
    behavior of the task.
    """

    def __init__(self, module_config: dict, job_parameters: JobParameters) -> None:
        """Initialize the Task instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
        """
        self.job_parameters = job_parameters
        self.module_config = module_config.parameters
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"Initializing Task for: {module_config.module_name.__name__}")

    @abstractmethod
    def execute(self) -> Any:
        """Abstract method for execute method."""
        pass
