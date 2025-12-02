"""Module for retrtrieving encryption key from HSM service."""
# import: standard
import binascii
import csv
import datetime
import hashlib
import logging
import os
import subprocess
from copy import deepcopy
from glob import glob
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
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    FileOptionConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.data_extractor.odbc_data_extractor import (
    WritePropertyConfigModel,
)
from mdp.framework.mdp_extraction_framework.task.encryption_key_file_generator.base_encryption_key_file_generator import (
    BaseEncryptionKeyFileGeneratorTask,
)
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import render_template

# import: external
import pandas
from Crypto.Cipher import AES
from pandas import read_fwf
from pydantic import BaseModel
from pydantic import validator


class FileNameFormatTaskConfigModel(BaseModel):
    """Configuration model for defining file name format with sequence numbers and other
    file name if any.

    Attributes:
        base_file_name (str): The base name of the file.
        date_suffix (str): The date suffix to be appended to the file name.
    """

    base_file_name: str
    date_suffix: str


class FixedLengthFileReader:
    """Class for managing Fixed Length File Reader.

    Attributes:
        mapping_config (dict): A dictionary representing the mapping configuration
        field_names (list): A list of field names.
        colspecs (list): A list of tuples giving the extents of the fixed-width fields of each line as half-open intervals.
        datatype (dict): A dictionary of column datatypes.
    """

    spark_sql_to_pandas_mapping = {
        "INT": "int",
        "INTEGER": "int",
        "LONG": "int",
        "FLOAT": "float",
        "DOUBLE": "float",
        "STRING": "str",  # 'str' is interchangeable, but 'object' is more general in Pandas
        "BOOLEAN": "bool",
        "DATE": "datetime64[ns]",  # or 'object' if you work with date strings
        "TIMESTAMP": "datetime64[ns]",
        "BINARY": "object",  # For bytes
        "DECIMAL": "float",  # or 'object' if using decimal.Decimal for precision
        "ARRAY": "object",  # For lists
        "MAP": "object",  # For dictionaries
        "STRUCT": "object",  # For custom objects or dictionaries
    }

    def __init__(
        self,
        mapping_config: dict,
        section: str,
    ) -> None:
        """Initiate instance of Fixed Length File Reader.

        Args:
            mapping_config (dict): length mapping config
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mapping_config = mapping_config.get(section)
        self.logger.info(f"Fixed Length Field Mapping Config: {self.mapping_config}")
        self.get_column_mapping()

    def get_column_mapping(self) -> None:
        """Extracts column names, widths, and datatype from a mapping configuration
        dictionary."""
        field_names = []
        colspecs = []
        datatype = {}
        encrypted_columns = []
        spark_sql_datatype = {}

        for field in self.mapping_config:
            field_names.append(field["field_name"])
            # Offset in KBMF files starts with 1, while offset of pandas library starts with 0
            colspec = (field["offset"] - 1, field["offset"] - 1 + field["size"])
            colspecs.append(colspec)
            dtype = self.spark_sql_to_pandas_mapping[field["type"].upper()]
            datatype[field["field_name"]] = dtype
            spark_sql_datatype[field["field_name"]] = field["type"].upper()
            if str(field.get("is_encrypted", "")).lower() == "true":
                encrypted_columns.append(field["field_name"])

        self.field_names = field_names
        self.colspecs = colspecs
        self.datatype = datatype
        self.encrypted_columns = encrypted_columns
        self.spark_sql_datatype = spark_sql_datatype

    def build_df_from_txt_file(
        self,
        files_location: List[str],
        header: int,
        footer: int,
        reader_options: dict = {},
    ) -> pandas.DataFrame:
        """Builds a DataFrame from a fixed-width format (FWF) text file.

        Args:
            files_location List[str]: Text file path.
            header (int): number of header row to skip. Default None to not skip row.
                          For more details: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html
            footer (int): number of footer line to skip. Default 0 to not skip row.
            reader_options (str) : additional reader option

        Returns:
            DataFrame: A pandas DataFrame constructed from the pandas dataframe on FWF text file.
        """
        self.logger.info(f"Building DataFrame from Fixed Length File at: {files_location}")
        pandas_df = read_fwf(
            files_location[0],
            header=header,
            skipfooter=footer,
            names=self.field_names,
            colspecs=self.colspecs,
            dtype=self.datatype,
            skip_blank_lines=False,
            **reader_options,
        )
        pandas_df_all = pandas_df.replace({pandas.NA: None})

        if len(files_location) > 1:
            for file in files_location[1:]:
                pandas_df = read_fwf(
                    file,
                    header=header,
                    skipfooter=footer,
                    names=self.field_names,
                    colspecs=self.colspecs,
                    dtype=self.datatype,
                    skip_blank_lines=False,
                    **reader_options,
                )
                pandas_df = pandas_df.replace({pandas.NA: None})
                pandas_df_all = pandas.concat([pandas_df_all, pandas_df], ignore_index=True)

        self.logger.info("Build DataFrame from Fixed Length File completed.")
        return pandas_df_all


class HSMEncryptionKeyFileGeneratorTaskConfigModel(BaseModel):
    """HSM ecryption key file generator configuration model.

    Args:
        BaseModel: pydantic model for base reader

    Attributes:
        length_mapping_config (str): length mapping config
        number_of_row_header (int, optional): number of header line to skip. Defaults to None to not skip row.
                                For more details: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html
        number_of_row_footer (int, optional): number of footer line to skip. Defaults to 0 to not skip row.
    """

    source_file_location: Optional[str] | None = ""
    length_mapping_config: dict
    number_of_row_header: Optional[int] = 0
    number_of_row_footer: Optional[int] = 0
    reader_options: Optional[dict] = {}
    header_columns: List[str]
    file_name_format: FileNameFormatTaskConfigModel
    full_file_name: str
    file_extension: Optional[str] = "key"
    file_option: Optional[FileOptionConfigModel] = FileOptionConfigModel()
    write_property: WritePropertyConfigModel

    # Validator for the 'number_of_row_header' field
    @validator("number_of_row_header", pre=True, always=True)
    def adjust_header(cls, v):
        # If header is passed and is a number other than 0, subtract 1
        if v is not None and v > 0:
            return v - 1
        # If header is 0 or not passed, set to None
        elif v == 0 or v is None:
            return None
        return v


class HSMEncryptionKeyFileGeneratorTask(BaseEncryptionKeyFileGeneratorTask):
    """Class for generating encryption key file from HSM service."""

    parameter_config_model = HSMEncryptionKeyFileGeneratorTaskConfigModel

    def __init__(
        self,
        module_config: dict,
        job_parameters: JobParameters,
        file_infos: List[DataFileInformation],
    ):
        """Initializes a EncryptionKeyRetriverTask instance.

        Args:
            module_config (dict): A dictionary containing module configuration.
            job_parameters (JobParameters): An object containing job parameters.
            file_infos (List[DataFileInformation]): List of files path to be read.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        super().__init__(module_config, job_parameters)

        # Set source file list based on input types, from config or from previous task
        if self.module_config.source_file_location:
            self.files_list = []
            rendered_file_path = render_template(
                content=self.module_config.source_file_location,
                mapping=vars(self.module_config.file_name_format),
            )
            for file in glob(rendered_file_path):
                if not file.endswith(".key"):
                    self.files_list.append(file)

            # If source file list come from config, generate new DataFileInformation of those files
            self.encrypted_file_infos = [
                generate_data_file_info(file_loc) for file_loc in self.files_list
            ]
        else:
            if not file_infos:
                raise FileNotFoundError(
                    "DataFileInformation class is 'None'. Please check previous process."
                )
            else:
                self.files_list = [file_info.file_location for file_info in file_infos]
                self.encrypted_file_infos = file_infos

        if self.files_list == []:
            raise FileNotFoundError("Please check 'source_file_location' in config")
        else:
            self.logger.info(f"List of file location path = {self.files_list}.")

        self.logger.info(f"List of file location: {self.files_list}")
        if self.files_list == []:
            raise FileNotFoundError

        self.full_file_name = render_template(
            content=self.module_config.full_file_name,
            mapping=vars(self.module_config.file_name_format),
        )
        self.full_file_location = os.path.join(
            os.path.dirname(self.files_list[0]),
            f"{self.full_file_name}.{self.module_config.file_extension}",
        )

    def get_key_by_hsm(self, encrypted_message):
        try:
            java_class_path = os.getenv("HSM_JAVA_CLASS_PATH")
            java_class_name = os.getenv("HSM_JAVA_CLASS_NAME")

            host = os.getenv("HSM_HOST")
            port = os.getenv("HSM_PORT")
            dpk = os.getenv("HSM_DPK")

            # Run the Java class and capture the output
            result = subprocess.run(
                [
                    "java",
                    "-cp",
                    java_class_path,
                    java_class_name,
                    encrypted_message,
                    host,
                    str(port),
                    dpk,
                ],
                stdout=subprocess.PIPE,  # Capture the output
                stderr=subprocess.PIPE,  # Capture errors
                text=True,  # Get output as a string
            )

            # Check if there is any error
            if result.stderr:
                self.logger.error(f"HSM sevice error: {result.stderr}")
                # return None

            # Capture the key by the Java program as standard output
            clear_key = result.stdout.strip()
            return clear_key

        except Exception as e:
            self.logger.error(f"HSM sevice error: {e}")
            # return None

    def hash_sha256(self, data: str) -> str:
        """Method to hash string with sha256.

        Args:
            data (str): string content need to be hashed

        Returns:
            str: hex string results of hashing sha256
        """
        # Encode the data to bytes
        b_data = data.encode("utf-8")

        # Create a SHA-256 hash object
        hash_object = hashlib.sha256()

        # Update the hash object with the data
        hash_object.update(b_data)

        # Get the hexadecimal representation of the digest
        hex_dig = hash_object.hexdigest()
        return hex_dig

    def ccms_encryption(self, plaintext: str, key: str, key_type: str) -> str:
        """Method to encrypt plaintext  with AES 256 as ECB mode.

        Args:
            plaintext (str): plain text or string messages need to be encrypted
            key (str): key used for encryption
            key_type(str): type of key consist of plain_text, hex_string

        Raises:
            ValueError: Wrong type key error handler

        Returns:
            str: encrypted message
        """

        # Decode the key to bytes
        if key_type.lower() == "hex_string":
            b_key = binascii.unhexlify(key)
        elif key_type.lower() == "plain_text":
            b_key = key.encode("utf-8")
        else:
            raise ValueError("Key type is not correct, should be plain_text or hex string")

        # Convert plaintext to bytes if it's not already
        b_plaintext = plaintext.encode() if isinstance(plaintext, str) else plaintext

        # Create a new AES cipher in ECB mode
        cipher = AES.new(b_key, AES.MODE_ECB)

        # Encrypt the plaintext block by block (assuming plaintext is a multiple of block size)
        ciphertext = b""
        for i in range(0, len(b_plaintext), AES.block_size):
            block = b_plaintext[i : i + AES.block_size]
            ciphertext += cipher.encrypt(block)

        # Return the hex-encoded ciphertext
        return binascii.hexlify(ciphertext).decode()

    def generate_key_file_data(self, pos_dt: str, key_list: list, file_name: str, key_section: str):
        """Generates a DataFrame containing the date of the key, the date of the
        generated key, key and data file name.

        Args:
            pos_dt (str): The date of the key.
            key_list (List): The key.
            file_name (str): The data file name.
            key_section (str): Section of key file
        """
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        data = []
        if key_section == "header":
            for keys in key_list:
                row = [pos_dt, current_date, keys[1], file_name]
                data.append(row)
        if key_section == "body":
            for keys in key_list:
                row = [pos_dt, current_date, keys[1], keys[0], file_name]
                data.append(row)

        return data

    def write_to_csv(
        self,
        file_name: str,
        header_col: list,
        data: list,
        write_property: WritePropertyConfigModel,
        file_option: FileOptionConfigModel,
    ):
        """Write data to a CSV file.

        Args:
            file_name (str): The name of the CSV file to create.
            header_col (Sequence[str]): Sequence of strings representing the column headers.
            data (Sequence[tuple]): A list of rows containing the data to be written.
            write_property (WritePropertyConfigModel): Options for CSV writing.
            file_option (FileOptionConfigModel): File option for opening file.
        """
        self.logger.info(f"Writing {file_name}.")
        option = deepcopy(write_property.option)
        quoting = option.get("quoting")
        if quoting:
            option["quoting"] = csv.__getattribute__(quoting)

        with open(file_name, **file_option.model_dump()) as csvfile:
            writer = csv.writer(csvfile, **option)
            if write_property.header:
                writer.writerow(header_col)
            writer.writerows(data)
        self.logger.info(f"Write {file_name} completed.")

    def execute(self) -> list:
        """Executes the HSM ecryption key retrieving process.

        Get key from data file, connect to HSM service, fetches clear key then apply MDP encryption immediately.

        Returns:
            List[str]: List of MDP-encrypyted keys
        """
        key_section = list(self.module_config.length_mapping_config.keys())[0]

        # read fixed-lenght mapping config
        reader = FixedLengthFileReader(
            mapping_config=self.module_config.length_mapping_config,
            section=key_section,
        )
        # build dataframe from fixed-lenght source file
        df = reader.build_df_from_txt_file(
            files_location=self.files_list,
            header=self.module_config.number_of_row_header,
            footer=self.module_config.number_of_row_footer,
            reader_options=self.module_config.reader_options,
        )
        # get key from header
        if key_section == "header":
            encrypted_key_list = df[df.columns[0]].head(1).unique().tolist()

        # get key from body
        if key_section == "body":
            encrypted_key_list = df[df.columns[0]].unique().tolist()

        self.logger.info(f"Number of HSM key = {len(encrypted_key_list)}.")

        # prepare hash key for mdp encryption
        pos_dt = self.job_parameters.pos_dt
        hash_key = self.hash_sha256(pos_dt)

        self.logger.info("Get clear key from HSM service")
        mdp_key_list = []
        for k in encrypted_key_list:
            # get clear key using hsm service
            clear_key = self.get_key_by_hsm(k)
            # MDP encryption
            mdp_encrypted_key = self.ccms_encryption(
                plaintext=clear_key, key=hash_key, key_type="hex_string"
            )

            key_tuple = (k, mdp_encrypted_key)

            mdp_key_list.append(key_tuple)

        data = self.generate_key_file_data(
            pos_dt, mdp_key_list, self.full_file_name, key_section=key_section
        )
        self.write_to_csv(
            file_name=self.full_file_location,
            header_col=self.module_config.header_columns,
            data=data,
            write_property=self.module_config.write_property,
            file_option=self.module_config.file_option,
        )
        self.logger.info("Key file is generated.")

        self.encrypted_file_infos.append(generate_data_file_info(self.full_file_location))
        return self.encrypted_file_infos
