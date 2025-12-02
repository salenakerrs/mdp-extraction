"""Test Module for HSM encrypted key file generator."""

# import: standard
import csv
import logging
import os
from datetime import datetime

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    DataFileInformation,
)
from mdp.framework.mdp_extraction_framework.task.encryption_key_file_generator.hsm_encryption_key_file_generator import (
    FixedLengthFileReader,
)
from mdp.framework.mdp_extraction_framework.task.encryption_key_file_generator.hsm_encryption_key_file_generator import (
    HSMEncryptionKeyFileGeneratorTask,
)
from mdp.framework.mdp_extraction_framework.task.encryption_key_file_generator.hsm_encryption_key_file_generator import (
    HSMEncryptionKeyFileGeneratorTaskConfigModel,
)

# import: external
import pandas as pd
import pytest
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class mock_model(BaseModel, extra="allow"):
    """A mock pydantic model."""

    pass


@pytest.fixture(scope="module", autouse=True)
def setup_environment_variables():
    """Setup environment variable to override the '.env' file for unit testing."""
    # environment variable for ADLS storage
    os.environ["MDP_INBND__ACCOUNT_NAME"] = "testadls001dev"
    os.environ["MDP_INBND__CONTAINER_NAME"] = "test_container"
    os.environ["MDP_INBND__SAS_TOKEN"] = "test_token"
    os.environ["MDP_INBND__filepath"] = "test_adls/filepath"
    # environment variable for local storage
    os.environ["LOCAL_STORAGE__filepath"] = "test_local/filepath"

    # CONNECTION INFO
    os.environ["CONNECTION_INFO__UD__username"] = "test_user_ud"
    os.environ["CONNECTION_INFO__UD__password"] = "test_password_ud"
    os.environ["CONNECTION_INFO__UD__database"] = "test_sid_ud"
    os.environ["CONNECTION_INFO__UD__server"] = "test_server_ud"
    os.environ["CONNECTION_INFO__UD__port"] = "1525"

    os.environ["CONNECTION_INFO__SCF__username"] = "test_user_scf"
    os.environ["CONNECTION_INFO__SCF__password"] = "test_password_scf"
    os.environ["CONNECTION_INFO__SCF__database"] = "test_database_scf"
    os.environ["CONNECTION_INFO__SCF__server"] = "test_server_scf"
    os.environ["CONNECTION_INFO__SCF__port"] = "1433"

    os.environ["CONNECTION_INFO__DGTL_FCTRNG__username"] = "test_user_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__password"] = "test_password_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__database"] = "test_database_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__server"] = "test_server_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__port"] = "1433"

    # kbc_ivr
    os.environ["CONNECTION_INFO__KBC_IVR__username"] = "test_user_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__password"] = "test_password_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__database"] = "test_database_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__server"] = "test_server_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__port"] = "1433"

    # kbc_docsub
    os.environ["CONNECTION_INFO__KBC_DOCSUB__username"] = "test_user_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__password"] = "test_password_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__database"] = "test_database_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__server"] = "test_server_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__port"] = "1433"


parameters = {
    "length_mapping_config": {
        "header": [{"field_name": "sharekey", "size": 32, "offset": 66, "type": "string"}]
    },
    "number_of_row_header": None,
    "number_of_row_footer": 1,
    "reader_options": {"encoding": "cp1252"},
    "header_columns": ["date_of_key", "date_of_generated_key", "encrypted_key", "data_file_name"],
    "file_name_format": {
        "base_file_name": "test_hsm_encrypted_file",
        "date_suffix": "{{ ptn_yyyy }}{{ ptn_mm }}{{ ptn_dd }}",
    },
    "full_file_name": "{{ base_file_name }}_{{ date_suffix }}",
    "file_extension": "key",
    "file_option": {"mode": "a", "newline": "", "encoding": "utf-8"},
    "write_property": {"header": True, "option": {"delimiter": "|"}},
}

HSM_ENCRYPTED_FILE = "test/mdp/unit/mdp_extraction_framework/resources/task/encryption_key_file_generator/test_hsm_encrypted_file.txt"

MODULE_CONFIG = mock_model(
    module_name=HSMEncryptionKeyFileGeneratorTask,
    parameters=HSMEncryptionKeyFileGeneratorTaskConfigModel(**parameters),
)


@pytest.fixture(scope="session", autouse=True)
def teardown_encryption_file(scope="session", autouse=True):
    """Fixture to teardown written files."""

    yield ("Run complete, start tear down resource steps")

    output_path = (
        "test/mdp/unit/mdp_extraction_framework/resources/task/encryption_key_file_generator"
    )
    # Remove written files after testing
    hsm_encrypted_file_header_path = os.path.join(output_path, "test_hsm_encrypted_file_header.key")
    hsm_encrypted_file_body_path = os.path.join(output_path, "test_hsm_encrypted_file_body.key")

    os.remove(hsm_encrypted_file_header_path)
    os.remove(hsm_encrypted_file_body_path)


@pytest.fixture
def hsm_encryption_key_file_generator():
    # Test reader
    module_config = mock_model(
        module_name=HSMEncryptionKeyFileGeneratorTask,
        parameters=HSMEncryptionKeyFileGeneratorTaskConfigModel(**parameters),
    )
    job_param = JobParameters(
        pos_dt="2023-10-31",
        config_file_path="",
    )
    file_infos = [
        DataFileInformation(
            file_location=HSM_ENCRYPTED_FILE, file_size=1234, file_created_datetime=datetime.now()
        ),
    ]
    hsm_encryption_key_generator_ins = HSMEncryptionKeyFileGeneratorTask(
        module_config,
        job_param,
        file_infos,
    )

    yield hsm_encryption_key_generator_ins


def test_hash_sha256(hsm_encryption_key_file_generator):
    """Method to test hashing 256 function.

    Args:
        hsm_encryption_key_file_generator (fixture): hsm encrypted key file generator instance
    """
    input_message = "ccms_hashing_test"
    expected_hash_message = "ca825aec882b65ecac95d9714a3cf1f6181fccc53cd985d83b6b5c5cd4782d61"

    actual_message = hsm_encryption_key_file_generator.hash_sha256(input_message)

    assert actual_message == expected_hash_message, "Hashing message is not correct"


@pytest.mark.parametrize(
    "plaintext, input_key, input_key_type, expected_encrypted_message",
    [
        (
            "20240207125139000000000000000000",
            "c3b16720310a8735859e16cc2857aa75b3e7b6ac46174cf4a61447ca0cb25de1",
            "hex_string",
            "25eb494fc0b43f5c30d414a95ad09114f19f184d624ce372b38d2948318426df",
        ),
        (
            "4124660900004114",
            "20240207125139000000000000000000",
            "plain_text",
            "5d7a0d73e5e769d5651ea51ab0038dce",
        ),
    ],
    ids=["hex_string_key", "plain_text_key"],
)
def test_ccms_encryption(
    hsm_encryption_key_file_generator,
    plaintext,
    input_key,
    input_key_type,
    expected_encrypted_message,
):
    """Method to test function encryption of ccms source.

    Args:
        hsm_encryption_key_file_generator (fixture): hsm encrypted key file generator instance
        plaintext (fixture): plaintext value for testing
        input_key (fixture): key for encryption
        input_key_type (fixture): key type
        expected_decrypted_message (fixture): expected encryption message from function test
    """
    actual_encrypted_message = hsm_encryption_key_file_generator.ccms_encryption(
        plaintext, input_key, key_type=input_key_type
    )

    assert (
        actual_encrypted_message == expected_encrypted_message
    ), "Encrypted message is not correct"


@pytest.mark.parametrize(
    "pos_dt, key_list, file_name, key_section, expected_key_data",
    [
        (
            "2023-10-31",
            [
                (
                    "6cc8292e42624659a51abaf1148eddcb",
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                )
            ],
            "test_hsm_encrypted_file",
            "header",
            [
                [
                    "2023-10-31",
                    datetime.now().strftime("%Y-%m-%d"),
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                    "test_hsm_encrypted_file",
                ]
            ],
        ),
        (
            "2023-10-31",
            [
                (
                    "6cc8292e42624659a51abaf1148eddcb",
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                ),
                (
                    "f4cd12967fc7b124d9b9ea7e41da6324",
                    "2aaab189b1bd0aaa13cc0d352712932795db2972da082cdcd54f6b05406d6a2e",
                ),
            ],
            "test_hsm_encrypted_file",
            "body",
            [
                [
                    "2023-10-31",
                    datetime.now().strftime("%Y-%m-%d"),
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                    "6cc8292e42624659a51abaf1148eddcb",
                    "test_hsm_encrypted_file",
                ],
                [
                    "2023-10-31",
                    datetime.now().strftime("%Y-%m-%d"),
                    "2aaab189b1bd0aaa13cc0d352712932795db2972da082cdcd54f6b05406d6a2e",
                    "f4cd12967fc7b124d9b9ea7e41da6324",
                    "test_hsm_encrypted_file",
                ],
            ],
        ),
    ],
    ids=["key_in_header", "key_in_body"],
)
def test_generate_key_file_data(
    hsm_encryption_key_file_generator,
    pos_dt,
    key_list,
    file_name,
    key_section,
    expected_key_data,
):
    """Method to test function encryption of ccms source.

    Args:
        hsm_encryption_key_file_generator (fixture): hsm encrypted key file generator instance
        pos_dt (fixture): date of key
        key_list (fixture): list of key for encryption
        file_name (fixture): data file name
        key_section (fixture): section of key in data file
        expected_key_data (fixture): expected encryption key data
    """
    actual_key_data = hsm_encryption_key_file_generator.generate_key_file_data(
        pos_dt, key_list, file_name, key_section
    )

    assert actual_key_data == expected_key_data, "Encryption key data is not correct"


@pytest.mark.parametrize(
    "file_name, header_col, data, expected_content",
    [
        (
            "test_hsm_encrypted_file_header.key",
            ["date_of_key", "date_of_generated_key", "encrypted_key", "data_file_name"],
            [
                [
                    "2023-10-31",
                    datetime.now().strftime("%Y-%m-%d"),
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                    "test_hsm_encrypted_file",
                ]
            ],
            [
                ["date_of_key", "date_of_generated_key", "encrypted_key", "data_file_name"],
                [
                    "2023-10-31",
                    datetime.now().strftime("%Y-%m-%d"),
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                    "test_hsm_encrypted_file",
                ],
            ],
        ),
        (
            "test_hsm_encrypted_file_body.key",
            ["date_of_key", "date_of_generated_key", "encrypted_key", "hsm_key", "data_file_name"],
            [
                [
                    "2023-10-31",
                    datetime.now().strftime("%Y-%m-%d"),
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                    "6cc8292e42624659a51abaf1148eddcb",
                    "test_hsm_encrypted_file",
                ]
            ],
            [
                [
                    "date_of_key",
                    "date_of_generated_key",
                    "encrypted_key",
                    "hsm_key",
                    "data_file_name",
                ],
                [
                    "2023-10-31",
                    datetime.now().strftime("%Y-%m-%d"),
                    "93de6f63782b2d62d109f1313ac67dc14bbff5e755f2911b6688c975476604a8",
                    "6cc8292e42624659a51abaf1148eddcb",
                    "test_hsm_encrypted_file",
                ],
            ],
        ),
    ],
    ids=["Key in header", "Key in body"],
)
def test_write_to_csv(
    hsm_encryption_key_file_generator, file_name, header_col, data, expected_content
):
    """Method to test writing .key file.

    Args:
        hsm_encryption_key_file_generator (fixture): hsm encrypted key file generator instance
        file_name (fixture): data file name
        header_col (fixture): list of header column
        data (fixture): data of key file
        expected_content (fixture): expected content of key data
    """
    full_file_name = (
        "test/mdp/unit/mdp_extraction_framework/resources/task/encryption_key_file_generator"
        + f"/{file_name}"
    )
    hsm_encryption_key_file_generator.write_to_csv(
        file_name=full_file_name,
        header_col=header_col,
        data=data,
        write_property=MODULE_CONFIG.parameters.write_property,
        file_option=MODULE_CONFIG.parameters.file_option,
    )

    with open(full_file_name, "r", newline="") as csvfile:
        csv_reader = csv.reader(
            csvfile,
            delimiter="|",
        )
        data = [row for row in csv_reader]

    assert data == expected_content, "File content does not match with expected."


def test_get_column_mapping():
    """Test method to extracts column names and widths from a mapping configuration
    dictionary."""

    reader = FixedLengthFileReader(
        mapping_config=MODULE_CONFIG.parameters.length_mapping_config,
        section="header",
    )
    assert reader.field_names == ["sharekey"], "Column names does not match expected from config."
    assert reader.colspecs == [(65, 97)], "Column spec does not match expected from config."
    assert reader.datatype == {"sharekey": "str"}, "datatype does not match expected from config."
    assert (
        reader.encrypted_columns == []
    ), "List of encrypt columns does not match expected from config."
    assert reader.spark_sql_datatype == {
        "sharekey": "STRING"
    }, "spark sql datatype does not match expected from config."


def test_build_df_from_txt_file():
    """Test method to builds a DataFrame from a fixed-width format (FWF) text file."""
    # Expected DataFrame
    expected_df = pd.DataFrame({"sharekey": ["4EA701648809CB719A1811927453D6B2", None, None, None]})

    # Test reader
    reader = FixedLengthFileReader(
        mapping_config=MODULE_CONFIG.parameters.length_mapping_config,
        section="header",
    )

    df = reader.build_df_from_txt_file(
        files_location=[HSM_ENCRYPTED_FILE],
        header=MODULE_CONFIG.parameters.number_of_row_header,
        footer=MODULE_CONFIG.parameters.number_of_row_footer,
        reader_options=MODULE_CONFIG.parameters.reader_options,
    )
    assert df.equals(expected_df), "Expected DataFrame does not match with actual."
