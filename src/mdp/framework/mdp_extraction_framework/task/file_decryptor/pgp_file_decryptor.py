"""PGP File Decryptor Module."""

# import: standard
import os
from base64 import b64decode
from glob import glob
from typing import List
from typing import Optional

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import (
    DECRYPTOR_TYPE_MAPPING,
)
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import (
    PgpDecryptorSetting,
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

# import: external
from pgpy import PGPKey
from pgpy import PGPMessage
from pydantic import BaseModel


class PgpFileDecryptorValueError(ValueError):
    """Custom error for Pgp file decryptor."""

    pass


class PgpFileDecryptorFileError(TypeError):
    """Custom error for Pgp file decryptor."""

    pass


class PgpFileDecryptorTaskConfigModel(BaseModel):
    """Pydantic class to validate the DataTransferTask.

    Args:
        BaseModel: pydantic base model
    """

    source_system_name: str
    source_file_location: Optional[str] | None = ""
    file_name_suffix: Optional[str] = "_decrypted"


class PgpFileDecryptorTask(BaseFileExtractorTask):
    """Class for decrypting PGP encrypted files."""

    parameter_config_model = PgpFileDecryptorTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
        file_infos: List[DataFileInformation] = None,
    ):
        """Initializes a PgpFileDecryptorTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration settings.
            job_parameters (JobParameters): An object containing job parameters.
            file_infos (List[DataFileInformation]): A list containing paths of files to decrypt.
        """
        super().__init__(module_config, job_parameters)
        self.files_list = (
            [file_info.file_location for file_info in file_infos] if file_infos else None
        )

    def load_env_setting(self, source_system_name: str) -> PgpDecryptorSetting:
        """Load ENV settings of DecryptorSetting.

        Args:
            source_system_name (str): source system name

        Raises:
            PgpFileDecryptorValueError: when no source system key is in ENV

        Returns:
            PgpDecryptorSetting: private key setting of source system
        """
        env_file = EnvSettings()
        pgp_key_setting = env_file.pgp_private_key.get(source_system_name.lower())

        settings_class = DECRYPTOR_TYPE_MAPPING.get("pgp")
        if not settings_class:
            raise ValueError("Unsupported decryptor pgp for decrypted data source")

        key_setting = settings_class(**pgp_key_setting)

        if not key_setting.pass_enc:
            err_msg = f"Missing ENV 'PGP_PRIVATE_KEY__{source_system_name}__PASS_ENC'"
            self.logger.error(err_msg)
            raise PgpFileDecryptorValueError(err_msg)
        self.logger.info(f"Loaded PGP Key ENV of {source_system_name}")

        return key_setting

    def read_pgp_key(self, key_path: str) -> PGPKey:
        """Reads a PGP private key from the provided file path.

        Args:
            key_path (str): Path to the PGP private key file.

        Returns:
            PGPKey: The loaded PGP key object.
        """
        return PGPKey.from_file(key_path)

    def read_pgp_encrypted_message(self, encoded_file_path: str) -> PGPMessage:
        """Reads a PGP encrypted message from the provided file.

        Args:
            encoded_file_path (str): Path to the file containing the PGP encrypted message.

        Returns:
            PGPMessage: The loaded PGP encrypted message object.
        """
        try:
            pgp_message = PGPMessage.from_file(encoded_file_path)
        except Exception as e:
            err_msg = f"File {encoded_file_path} is not PGP encrypted file."
            self.logger.error(err_msg)
            raise PgpFileDecryptorFileError(e)
        return pgp_message

    def decrypt_base64(self, encoded_message: str) -> str:
        """Decodes a base64-encoded message.

        Args:
            encoded_message (str): The base64 encoded message.

        Returns:
            str: The decoded message as a UTF-8 string.
        """
        return b64decode(encoded_message).decode("utf-8").strip()

    def decrypt_pgp_message(
        self, encoded_message: PGPMessage, private_key_path: str, passphrase: str
    ) -> str:
        """Decrypts a PGP message using a private key and passphrase.

        Args:
            encoded_message (PGPMessage): The PGP encrypted message object.
            private_key_path (str): Path to the PGP private key file.
            passphrase (str): Passphrase to unlock the private key.

        Returns:
            str: The decrypted message as a UTF-8 string.
        """
        private_key = self.read_pgp_key(private_key_path)

        with private_key[0].unlock(passphrase) as unlock_key:
            decrypted_message = unlock_key.decrypt(encoded_message).message
        return decrypted_message.decode("utf-8")

    def write_to_txt(self, decrypted_message: str, encoded_file_path: str) -> str:
        """Writes the decrypted message to a text file.

        Args:
            decrypted_message (str): The decrypted message content.
            encoded_file_path (str): The path to the original encoded file.

        Returns:
            str: The path to the newly created decrypted file.

        Raises:
            Exception: If an error occurs during the file writing process.
        """
        try:
            base_file_name, extension = os.path.splitext(os.path.basename(encoded_file_path))
            decrypted_file_name = (
                f"{base_file_name}{self.module_config.file_name_suffix}{extension}"
            )
            decrypted_file_path = os.path.join(
                os.path.dirname(encoded_file_path), decrypted_file_name
            )

            with open(decrypted_file_path, "w") as file:
                file.write(decrypted_message)
            self.logger.info(f"Decrypted File was written to {decrypted_file_path} successfully.")
            return decrypted_file_path
        except Exception as e:
            self.logger.error(f"An error occurred while writing to the file: {e}")
            raise e

    def execute(self) -> List[DataFileInformation]:
        """Executes the file decryptor process.

        Returns:
            List[DataFileInformation]: A list of paths to the decrypted files.
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}.")
        decrypted_file_infos = []
        key_setting = self.load_env_setting(self.module_config.source_system_name)
        private_key_passpharse_decrypted = self.decrypt_base64(key_setting.pass_enc)

        # Set encoded file list based on input types, from config or from previous task
        if self.module_config.source_file_location:
            encoded_file_list = glob(self.module_config.source_file_location)
        else:
            encoded_file_list = self.files_list

        # Perform Decryption
        for encoded_file in encoded_file_list:
            self.logger.info(f"Reading pgp encrypted file: {encoded_file}")
            encoded_message = self.read_pgp_encrypted_message(encoded_file)
            decrypted_message = self.decrypt_pgp_message(
                encoded_message,
                key_setting.key_file_path,
                private_key_passpharse_decrypted,
            )
            decrypted_file_path = self.write_to_txt(decrypted_message, encoded_file)
            decrypted_file_info = generate_data_file_info(decrypted_file_path)
            decrypted_file_infos.append(decrypted_file_info)

        return decrypted_file_infos
