"""GPG File Decryptor Module."""

# import: standard
import os
import shlex
import subprocess
import time
from glob import glob
from typing import List
from typing import Optional

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import (
    DECRYPTOR_TYPE_MAPPING,
)
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import (
    GpgDecryptorSetting,
)
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
from mdp.framework.mdp_extraction_framework.utility.test_utils.common.validate_file import (
    validate_local_file_exists,
)

# import: external
from pydantic import BaseModel

COMMAND_GPG_DECRYPT_FILE = """gpg --batch --yes --pinentry-mode loopback --passphrase "{passphrase}" -d -o "{decrypted_file_path}" "{encrypted_file_path}" """


class GpgFileDecryptorValueError(ValueError):
    """Custom error for GPG file decryptor."""

    pass


class GpgFileDecryptorFileError(TypeError):
    """Custom error for GPG file decryptor."""

    pass


class GpgFileDecryptorTaskConfigModel(BaseModel):
    """Pydantic class to validate the DataTransferTask.

    Args:
        BaseModel: pydantic base model
    """

    source_system_name: str
    source_file_location: Optional[str] | None = ""
    file_name_suffix: Optional[str] = "_decrypted"
    cleanup_flag: Optional[
        str
    ] | None = (
        "False"  # NEW: flag to control whether to delete encrypted files after successful decrypt
    )
    file_complete_check_flag: Optional[
        str
    ] | None = "False"  # NEW: flag to control whether to check file compleness


class GpgFileDecryptorTask(BaseFileExtractorTask):
    """Class for decrypting GPG encrypted files."""

    parameter_config_model = GpgFileDecryptorTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
        file_infos: List[DataFileInformation] = None,
    ):
        """Initializes a GpgFileDecryptorTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
            file_infos (List[DataFileInformation]): A list containing paths of files to decrypt.
        """
        super().__init__(module_config, job_parameters)
        self.files_list = (
            [file_info.file_location for file_info in file_infos] if file_infos else None
        )

    def load_env_setting(self, source_system_name: str) -> GpgDecryptorSetting:
        """Load ENV settings of GpgPrivateKey.

        Args:
            source_system_name (str): source system name

        Raises:
            GpgFileDecryptorValueError: when no source system key is in ENV

        Returns:
            GpgDecryptorSetting: private key setting of source system
        """
        env_file = EnvSettings()
        gpg_key_setting = env_file.gpg_private_key.get(source_system_name.lower())

        settings_class = DECRYPTOR_TYPE_MAPPING.get("gpg")
        if not settings_class:
            raise ValueError("Unsupported decryptor gpg for decrypted data source")

        key_setting = settings_class(**gpg_key_setting)

        if not key_setting.passphrase:
            err_msg = f"Missing ENV 'GPG_PRIVATE_KEY__{source_system_name}__PASSPHRASE'"
            self.logger.error(err_msg)
            raise GpgFileDecryptorValueError(err_msg)
        self.logger.info(f"Loaded GPG Key ENV of {source_system_name}")

        return key_setting

    def generate_decrypt_file_path(self, encrypted_file_path: str) -> str:
        """Writes the decrypted message to a text file.

        Args:
            encrypted_file_path (str): The path to the original encrypted file.

        Returns:
            str: The path to the newly created decrypted file.
        """
        base_file_name, extension = os.path.splitext(os.path.basename(encrypted_file_path))
        base_file_name, extension = os.path.splitext(
            os.path.basename(encrypted_file_path.removesuffix(extension))
        )
        decrypted_file_name = f"{base_file_name}{self.module_config.file_name_suffix}{extension}"
        decrypted_file_path = os.path.join(
            os.path.dirname(encrypted_file_path), decrypted_file_name
        )

        return decrypted_file_path

    def decrypt_gpg_file(
        self, passphrase: str, encrypted_file_path: str, decrypted_file_path: str
    ) -> str:
        """Writes the decrypted message to a text file.

        Args:
            passphrase (str): Path to the PGP private key file.
            encrypted_file_path (str): The path to the original encrypted file.
            decrypted_file_path (str): The path to the decrypted file.

        Returns:
            str: The path to the newly created decrypted file.
        """
        try:
            command = COMMAND_GPG_DECRYPT_FILE.format(
                passphrase=passphrase,
                encrypted_file_path=encrypted_file_path,
                decrypted_file_path=decrypted_file_path,
            )

            command_list = shlex.split(command)
            subprocess.run(command_list, check=True, shell=False)
        except Exception as e:
            err_msg = f"File {encrypted_file_path} is not GPG encrypted file."
            self.logger.error(err_msg)
            raise GpgFileDecryptorFileError(e)

        return decrypted_file_path

    def cleanup_encrypted_file(self, encrypted_file_path: str) -> None:
        """Remove the original encrypted file after successful decryption."""
        try:
            os.remove(encrypted_file_path)
            self.logger.info(f"Cleaned up encrypted file: {encrypted_file_path}")
        except FileNotFoundError:
            self.logger.warning(f"Encrypted file not found during cleanup: {encrypted_file_path}")
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.error(
                f"Failed to clean up encrypted file: {encrypted_file_path}. Error: {exc}"
            )

    def is_busy(self, path):
        """Command to check if file is in other processes (busy)"""
        return (
            subprocess.run(
                ["fuser", path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ).returncode
            == 0
        )

    def wait_until_all_files_complete(self, file_list, interval=3):
        """Iterate file list to check if all files are available to process other
        tasks."""
        self.logger.info(f"Checking file completeness: {len(file_list)}")
        remaining = list(file_list)

        round_no = 1
        while remaining:
            busy = [file for file in remaining if self.is_busy(file)]
            self.logger.info(f"Round {round_no}: processing remains - {len(busy)} files")
            remaining = busy  # update remaining to use for next rounds
            if remaining:
                time.sleep(interval)
            round_no += 1

        self.logger.info("All files are available to process other tasks")

    def execute(self) -> List[DataFileInformation]:
        """Executes the file decryptor process.

        Returns:
            List[DataFileInformation]: A list of paths to the decrypted files.
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")
        decrypted_file_infos = []
        key_setting = self.load_env_setting(self.module_config.source_system_name)

        # Set encoded file list based on input types, from config or from previous task
        if self.module_config.source_file_location:
            encrypted_file_list = glob(self.module_config.source_file_location)
        else:
            encrypted_file_list = self.files_list

        if self.module_config.file_complete_check_flag == "True":
            encrypted_file_list = [
                file for file in encrypted_file_list if validate_local_file_exists(file)
            ]
            if encrypted_file_list:
                self.wait_until_all_files_complete(encrypted_file_list)

        # Perform Decryption
        for encrypted_file_path in encrypted_file_list:
            self.logger.info(f"Reading gpg encrypted file: {encrypted_file_path}")
            decrypted_file_path = self.generate_decrypt_file_path(encrypted_file_path)
            decrypted_file_path = self.decrypt_gpg_file(
                key_setting.passphrase,
                encrypted_file_path,
                decrypted_file_path,
            )

            decrypted_file_info = generate_data_file_info(decrypted_file_path)
            decrypted_file_infos.append(decrypted_file_info)

            # NEW: optional cleanup of encrypted file
            if self.module_config.cleanup_flag == "True":
                self.cleanup_encrypted_file(encrypted_file_path)

        return decrypted_file_infos
