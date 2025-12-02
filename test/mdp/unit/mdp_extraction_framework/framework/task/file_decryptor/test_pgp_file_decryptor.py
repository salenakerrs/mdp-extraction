"""Test PGP File Decryptor."""
# import: standard
import os
from datetime import datetime

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.job_parameters import JobParameters
from mdp.framework.mdp_extraction_framework.task.data_extractor.base_extractor import (
    DataFileInformation,
)
from mdp.framework.mdp_extraction_framework.task.file_decryptor.pgp_file_decryptor import (
    PgpFileDecryptorFileError,
)
from mdp.framework.mdp_extraction_framework.task.file_decryptor.pgp_file_decryptor import (
    PgpFileDecryptorTask,
)
from mdp.framework.mdp_extraction_framework.task.file_decryptor.pgp_file_decryptor import (
    PgpFileDecryptorTaskConfigModel,
)

# import: external
import pytest
from pgpy import PGPMessage
from pydantic import BaseModel

JOB_PARAMS = JobParameters(
    pos_dt="2023-10-31",
    config_file_path="",
)


class mock_model(BaseModel, extra="allow"):
    """A mock pydantic model."""

    pass


PRIVATE_KEY_FILE = (
    "test/mdp/unit/mdp_extraction_framework/resources/task/file_decryptor/private_key.asc"
)
ENCRYPTED_FILE = (
    "test/mdp/unit/mdp_extraction_framework/resources/task/file_decryptor/encrypted_file.txt"
)


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
    # ud
    os.environ["CONNECTION_INFO__UD__dbtype"] = "oracledb"
    os.environ["CONNECTION_INFO__UD__username"] = "test_user_ud"
    os.environ["CONNECTION_INFO__UD__password"] = "test_password_ud"
    os.environ["CONNECTION_INFO__UD__database"] = "test_sid_ud"
    os.environ["CONNECTION_INFO__UD__server"] = "test_server_ud"
    os.environ["CONNECTION_INFO__UD__port"] = "1525"

    # scf
    os.environ["CONNECTION_INFO__SCF__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__SCF__username"] = "test_user_scf"
    os.environ["CONNECTION_INFO__SCF__password"] = "test_password_scf"
    os.environ["CONNECTION_INFO__SCF__database"] = "test_database_scf"
    os.environ["CONNECTION_INFO__SCF__server"] = "test_server_scf"
    os.environ["CONNECTION_INFO__SCF__port"] = "1433"

    # dgtl_fctrng
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__username"] = "test_user_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__password"] = "test_password_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__database"] = "test_database_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__server"] = "test_server_dgtl_fctrng"
    os.environ["CONNECTION_INFO__DGTL_FCTRNG__port"] = "1433"

    # kbc_ivr
    os.environ["CONNECTION_INFO__KBC_IVR__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__KBC_IVR__username"] = "test_user_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__password"] = "test_password_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__database"] = "test_database_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__server"] = "test_server_kbc_ivr"
    os.environ["CONNECTION_INFO__KBC_IVR__port"] = "1433"

    # kbc_docsub
    os.environ["CONNECTION_INFO__KBC_DOCSUB__dbtype"] = "sqlserver"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__username"] = "test_user_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__password"] = "test_password_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__database"] = "test_database_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__server"] = "test_server_kbc_docsub"
    os.environ["CONNECTION_INFO__KBC_DOCSUB__port"] = "1433"

    # smartserve
    os.environ["CONNECTION_INFO__SMARTSERVE__dbtype"] = "db2"
    os.environ["CONNECTION_INFO__SMARTSERVE__username"] = "test_user_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__password"] = "test_password_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__database"] = "test_database_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__server"] = "test_server_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__port"] = "50000"
    os.environ["CONNECTION_INFO__SMARTSERVE__schemaname"] = "test_schema_smartserve"
    os.environ["CONNECTION_INFO__SMARTSERVE__securitymechanism"] = "13"

    # mockmariadb
    os.environ["CONNECTION_INFO__MOCKMARIADB__dbtype"] = "mariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__username"] = "test_user_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__password"] = "test_password_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__database"] = "test_database_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__server"] = "test_server_mockmariadb"
    os.environ["CONNECTION_INFO__MOCKMARIADB__port"] = "3306"

    # mockmysql
    os.environ["CONNECTION_INFO__MOCKMYSQL__dbtype"] = "mysql"
    os.environ["CONNECTION_INFO__MOCKMYSQL__username"] = "test_user_mockmysql"
    os.environ["CONNECTION_INFO__MOCKMYSQL__password"] = "test_password_mockmysql"
    os.environ["CONNECTION_INFO__MOCKMYSQL__database"] = "test_database_mockmysql"
    os.environ["CONNECTION_INFO__MOCKMYSQL__server"] = "test_server_mockmysql"
    os.environ["CONNECTION_INFO__MOCKMYSQL__port"] = "3306"

    # PGP Key
    os.environ["PGP_PRIVATE_KEY__KS__PASS_ENC"] = "QUJDRFRFU1Q="
    os.environ["PGP_PRIVATE_KEY__KS__KEY_FILE_PATH"] = PRIVATE_KEY_FILE


@pytest.fixture(scope="session", autouse=True)
def teardown_decrypted_file(scope="session", autouse=True):
    """Fixture to teardown written files."""

    yield ("Run complete, start tear down resource steps")

    output_path = "test/mdp/unit/mdp_extraction_framework/resources/task/file_decryptor"

    # Remove written files after testing
    decrypted_file_path = os.path.join(output_path, "encrypted_file_decrypted.txt")

    os.remove(decrypted_file_path)


@pytest.fixture
def mock_task():
    """Fixture to set up a PgpFileDecryptorTask instance."""
    param = {"source_system_name": "KS", "source_file_location": ENCRYPTED_FILE}
    module_config = mock_model(
        module_name=PgpFileDecryptorTask, parameters=PgpFileDecryptorTaskConfigModel(**param)
    )
    file_infos = [
        DataFileInformation(
            file_location=ENCRYPTED_FILE, file_size=1234, file_created_datetime=datetime.now()
        ),
    ]
    return PgpFileDecryptorTask(module_config, JOB_PARAMS, file_infos)


def test_read_pgp_key(mock_task):
    """Test reading the PGP private key from file."""
    key = mock_task.read_pgp_key(PRIVATE_KEY_FILE)
    assert key is not None, "Failed to load PGP private key"


@pytest.mark.parametrize(
    "file , expected_pass",
    [
        (ENCRYPTED_FILE, True),
        (
            "test/mdp/unit/mdp_extraction_framework/resources/task/encryption_key_file_generator/test_hsm_encrypted_file.txt",
            False,
        ),
    ],
    ids=["PGP File", "Not PGP File"],
)
def test_read_pgp_encrypted_message(mock_task, file, expected_pass):
    """Test reading a PGP encrypted message."""
    if expected_pass:
        message = mock_task.read_pgp_encrypted_message(file)
        assert message is not None, "Failed to load PGP encrypted message"
    else:
        with pytest.raises(PgpFileDecryptorFileError):
            message = mock_task.read_pgp_encrypted_message(file)


def test_decrypt_base64(mock_task):
    """Test decoding a base64-encoded message."""
    encoded_message = (
        "U29tZSBiYXNlNjQtZW5jb2RlZCBtZXNzYWdl"  # base64 for "Some base64-encoded message"
    )
    decoded_message = mock_task.decrypt_base64(encoded_message)
    assert decoded_message == "Some base64-encoded message", "Base64 decoding failed"


def test_decrypt_pgp_message(mock_task):
    """Test the decryption of a PGP message using a private key and passphrase."""
    passphrase = "ABCDTEST"
    encoded_message = PGPMessage.from_file(ENCRYPTED_FILE)
    decrypted_message = mock_task.decrypt_pgp_message(encoded_message, PRIVATE_KEY_FILE, passphrase)

    assert decrypted_message is not None, "PGP message decryption failed"


def test_write_to_txt(mock_task, tmp_path):
    """Test writing the decrypted message to a text file."""
    decrypted_message = "This is a test decrypted message."
    encoded_file_path = str(tmp_path / "test_file.pgp")

    decrypted_file_path = mock_task.write_to_txt(decrypted_message, encoded_file_path)

    assert os.path.exists(decrypted_file_path), "Failed to write decrypted message to file"
    assert decrypted_file_path.endswith("_decrypted.pgp"), "File name suffix is incorrect"

    with open(decrypted_file_path, "r") as file:
        content = file.read()
        assert content == decrypted_message, "File content mismatch"


def test_execute(mock_task):
    """Test the full execution of PgpFileDecryptorTask."""
    decrypted_files = mock_task.execute()

    assert len(decrypted_files) == 1, "Decrypted files count mismatch"
    for decrypted_file_info in decrypted_files:
        assert os.path.exists(decrypted_file_info.file_location), "Decrypted file not found"
