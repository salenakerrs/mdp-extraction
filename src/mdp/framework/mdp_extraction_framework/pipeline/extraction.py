"""Extraction Pipeline."""

# import: standard
from dataclasses import dataclass
from typing import Any
from typing import List
from typing import Optional

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.common import PipelineTaskModel
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.pipeline.base_pipeline import BasePipeline
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    DataFileInformation,
)


@dataclass
class ExtractionPipelineExecutedValues:
    """Dataclass to store output from Extraction tasks."""

    extract_file_path: Optional[list[str]] | None = None
    target_file_path: Optional[str] | None = None
    files_size: Optional[list[int]] | None = None
    ctl_file_details: Optional[str] | None = None


class ExtractionPipelineTaskModel(PipelineTaskModel):
    """Extraction Pipeline Process base model.

    Args:
        PipelineTaskModel: model for pipeline task
    """

    eban_in_extractor_task: Any | None = None
    source_data_extractor_task: Any | None = None
    generate_control_file_task: Any | None = None
    file_extractor_task: Any | None = None
    file_decryptor_task: Any | None = None
    preprocess_extractor_task: Any | None = None
    hsm_encryption_key_file_generator_task: Any | None = None
    azcopy_data_transfer_task: Any | None = None


class ExtractionPipeline(BasePipeline):
    """Pipeline for data ingestion.

    Args:
        BasePipeline: model for base pipeline
    """

    # pipeline_oper_log = ExtractionOperLog

    def __init__(self, config: dict, job_parameters: JobParameters) -> None:
        """Initialize  ExtractionPipeline object instance.

        Args:
            config (dict): A dictionary containing pipeline configuration.
            job_parameters (JobParameters): Job parameters for the pipeline.
        """
        super().__init__(
            config=config,
            job_parameters=job_parameters,
            pipeline_task_model=ExtractionPipelineTaskModel,
        )
        self.executed_values = ExtractionPipelineExecutedValues()
        self.run_only_task = (
            None
            if self.job_parameters.run_only_task is None
            else self.job_parameters.run_only_task.split(",")
        )

    def execute_eban_in_extractor_task(
        self,
    ) -> None:
        """Execute the EBAN-IN Extractor Task."""
        task_params = self.module_parameters.eban_in_extractor_task
        if task_params and not task_params.bypass_flag:
            if self.run_only_task is None or "eban_in_extractor_task" in self.run_only_task:
                self.logger.info("Start EBAN-IN Extractor Task")
                eban_in_extractor_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                )
                eban_in_extractor_task_object.execute()

    def execute_source_data_extractor_task(
        self,
    ) -> list:
        """Execute the Data Extractor Task.

        Returns:
            list: file names from source data extractor
        """
        task_params = self.module_parameters.source_data_extractor_task
        if task_params and not task_params.bypass_flag:
            if self.run_only_task is None or "source_data_extractor_task" in self.run_only_task:
                self.logger.info("Start Source Data Extractor Task")
                data_extractor_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                )
                file_infos = data_extractor_task_object.execute()
                self.executed_values.files_size = [file_info.file_size for file_info in file_infos]
                self.executed_values.extract_file_path = [
                    file_info.file_location for file_info in file_infos
                ]
                return file_infos
            else:
                return None
        else:
            return None

    def execute_generate_control_file_task(self) -> None:
        """Execute the Control File Generator Task."""
        task_params = self.module_parameters.generate_control_file_task
        if task_params and not task_params.bypass_flag:
            if self.run_only_task is None or "generate_control_file_task" in self.run_only_task:
                self.logger.info("Start Generate Control File Task")
                control_file_gen_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                )
                (
                    file_name,
                    self.executed_values.ctl_file_details,
                ) = control_file_gen_task_object.execute()

    def execute_file_extractor_task(self, file_infos: List[DataFileInformation]) -> list:
        """Execute the File extractor Task.

        Args:
            file_infos (List[DataFileInformation]): A list of file names from the source data extractor.

        Returns:
            List[DataFileInformation]: A list of extracted files. If the task is bypassed, returns the original `file_infos`.
        """
        task_params = self.module_parameters.file_extractor_task
        if task_params and not task_params.bypass_flag:
            if self.run_only_task is None or "file_extractor_task" in self.run_only_task:
                self.logger.info("Start File Extractor Task")
                file_extractor_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                )
                extracted_files_list = file_extractor_task_object.execute()
                return extracted_files_list
            else:
                return file_infos
        else:
            return file_infos

    def execute_command_script_task(self) -> None:
        """Execute the File extractor Task."""
        task_params = self.module_parameters.preprocess_extractor_task
        if task_params and not task_params.bypass_flag:
            if self.run_only_task is None or "preprocess_extractor_task" in self.run_only_task:
                self.logger.info("Start File Extractor Task")
                preprocess_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                )
                preprocess_task_object.execute()

    def execute_hsm_encryption_key_file_generator_task(
        self, file_infos: List[DataFileInformation]
    ) -> list:
        """Execute the HSM encryption key generator Task."""
        task_params = self.module_parameters.hsm_encryption_key_file_generator_task
        if task_params and not task_params.bypass_flag:
            if (
                self.run_only_task is None
                or "hsm_encryption_key_file_generator_task" in self.run_only_task
            ):
                self.logger.info("Start HSM encryption key generator Task")
                hsm_encryption_key_file_generator_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                    file_infos=file_infos,
                )
                extracted_key_files_list = hsm_encryption_key_file_generator_task_object.execute()
                return extracted_key_files_list
            else:
                return file_infos
        else:
            return file_infos

    def execute_file_decryptor(self, file_infos: List[DataFileInformation]) -> list:
        """Execute the File decryptor Task.

        Args:
            file_names (list): A list of file names from the source data extractor.

        Returns:
            list: A list of decrypted files. If the task is bypassed, returns the original `file_names`.
        """
        task_params = self.module_parameters.file_decryptor_task
        if task_params and not task_params.bypass_flag:
            if self.run_only_task is None or "file_decryptor_task" in self.run_only_task:
                self.logger.info("Start File Decryptor Task")
                file_decryptor_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                    file_infos=file_infos,
                )
                decrypted_files_infos = file_decryptor_task_object.execute()
                return decrypted_files_infos
            else:
                return file_infos
        else:
            return file_infos

    def execute_transfer_file_azcopy_task(self, file_infos: List[DataFileInformation]) -> None:
        """Execute the File Transfer Task.

        Args:
            file_infos (List[DataFileInformation]): A list containing paths of files to transfer.
        """
        task_params = self.module_parameters.azcopy_data_transfer_task
        if task_params and not task_params.bypass_flag:
            if self.run_only_task is None or "azcopy_data_transfer_task" in self.run_only_task:
                self.logger.info("Start Transfer File Azcopy Task")
                transfer_file_azcopy_task_object = task_params.module_name(
                    module_config=task_params,
                    job_parameters=self.job_parameters,
                    file_infos=file_infos,
                )
                self.executed_values.target_file_path = transfer_file_azcopy_task_object.execute()

    def execute(self) -> ExtractionPipelineExecutedValues:
        """Method to run the pipeline.

        Returns:
            ExtractionPipelineExecutedValues: An object contains values being updated in the operation log.
        """
        self.logger.info("Start Extraction Pipeline Execution")

        # Task 0: eban-in extraction step (extract and transfer from shell script)
        self.execute_eban_in_extractor_task()

        # Task 1: source extraction step
        file_infos = self.execute_source_data_extractor_task()

        # Task 2: Generate control files
        self.execute_generate_control_file_task()

        # Task 3: File unzipper
        extracted_file_infos = self.execute_file_extractor_task(file_infos)

        # Task 4: Run command script
        self.execute_command_script_task()

        # Task 5: File decryptor
        decrypted_files_infos = self.execute_file_decryptor(extracted_file_infos)

        # Task 6: HSM encryption Key file generator
        extracted_encrypted_file_infos = self.execute_hsm_encryption_key_file_generator_task(
            decrypted_files_infos
        )

        # Task 7: Transfer File
        self.execute_transfer_file_azcopy_task(extracted_encrypted_file_infos)

        self.logger.info("Extraction Pipeline Execution Completed.")
        return self.executed_values
