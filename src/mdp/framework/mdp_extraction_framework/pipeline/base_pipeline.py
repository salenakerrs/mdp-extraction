"""Base Pipeline Module."""

# import: standard
import logging
from copy import deepcopy
from typing import Any

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.common import PipelineConfigModel
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.control_file_generator.mongodb_control_file_generator import (  # noqa
    MongoControlFileGeneratorTask,
)
from mdp.framework.mdp_extraction_framework.task.control_file_generator.odbc_control_file_generator import (  # noqa
    OdbcControlFileGeneratorTask,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.eban_in_extractor import (  # noqa
    EBANInExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.mongodb_data_extractor import (  # noqa
    MongoDataExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (  # noqa
    OdbcDataExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.data_transfer.azcopy_data_transfer import (  # noqa
    AzCopyDataTransferTask,
)
from mdp.framework.mdp_extraction_framework.task.encryption_key_file_generator.hsm_encryption_key_file_generator import (  # noqa
    HSMEncryptionKeyFileGeneratorTask,
)
from mdp.framework.mdp_extraction_framework.task.file_decryptor.gpg_file_decryptor import (  # noqa
    GpgFileDecryptorTask,
)

# from mdp.framework.mdp_extraction_framework.task.file_generator.delimited_file_generator import (  # noqa
#     DelimitedFileGeneratorTask,
# )
# from mdp.framework.mdp_extraction_framework.task.file_generator.parquet_file_generator import (  # noqa
#     ParquetFileGeneratorTask,
# )
from mdp.framework.mdp_extraction_framework.task.file_decryptor.pgp_file_decryptor import (  # noqa
    PgpFileDecryptorTask,
)
from mdp.framework.mdp_extraction_framework.task.file_extractor.zip_file_extractor import (  # noqa
    ZipFileExtractorTask,
)
from mdp.framework.mdp_extraction_framework.task.preprocess.submit_command_script import (  # noqa
    SubmitCommandScriptTask,
)
from mdp.framework.mdp_extraction_framework.utility.common_function import get_class_object


class BasePipeline:
    """Base class for data processing pipelines."""

    def __init__(
        self, config: dict, job_parameters: JobParameters, pipeline_task_model: Any
    ) -> None:
        """Init method of the base pipeline. Set the parameters according to the config
        file and tasks.

        Args:
            config (dict): A dictionary containing pipeline configuration.
            job_parameters (JobParameters): Job parameters for the pipeline.
            pipeline_task_model (Any): A model defining the pipeline tasks and parameters.
        """
        self.job_parameters = job_parameters
        self.pipeline_config = PipelineConfigModel(**config)
        self.pipeline_tasks = pipeline_task_model(**self.pipeline_config.tasks)
        self.module_parameters = deepcopy(self.pipeline_tasks)

        # Set module class and parameters for each process
        for task_name, task_parameters in self.pipeline_config.tasks.items():
            module_object = get_class_object(__name__, task_parameters.module_name)
            module_param = module_object.parameter_config_model(**task_parameters.parameters)
            task = getattr(self.module_parameters, task_name)
            # changes to pydantic 2.0 will return task as a TaskConfigModel instead of dict
            # task["module_name"] = module_object
            # task["parameters"] = module_param
            task.module_name = module_object
            task.parameters = module_param

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"job parameters : {self.job_parameters}")
        self.logger.info(f"tasks config : {self.module_parameters}")
