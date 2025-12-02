"""config_reader tests."""
# import: standard
import json
import os

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import ConfigMapping
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import JSONReader
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import YAMLFileReader
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import (
    read_and_render_config,
)
from mdp.framework.mdp_extraction_framework.utility.file_reader.config_reader import render_template

# import: external
import pytest

INPUT_CONFIG = {
    "job_name": "test_job",
    "pipeline_name": "IngestionPipeline",
    "tasks": {
        "read_source_file_task": {
            "module_name": "ParquetSourceFileReaderTask",
            "bypass_flag": "False",
            "parameters": {
                "filter_cond": "ptn_yyyy = '{{ptn_yyyy}}'",
                "date": "{{ptn_dd}}",
                "month": "{{ptn_mm}}",
                "year": "{{ptn_yyyy}}",
                "year_be": "{{ptn_yyyy_be}}",
                "quarter": "{{ptn_qtr}}",
            },
        }
    },
}


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


def test_json_reader():
    """Test reading json files."""
    json_path = "test/oih/unit/mdp_extraction_framework/resources/conf/pipeline.json"
    json_cls = JSONReader(config_file_path=json_path)
    json_txt = json_cls.read_file()

    expected_txt = json.dumps(INPUT_CONFIG, indent=4)
    assert json_txt.strip() == expected_txt.strip()


def test_yaml_reader(tmp_path):
    """Test reading yaml files."""
    yaml_content = """
    tasks:
      - name: Task 1
        value: 42
      - name: Task 2
        value: 24
    """
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(yaml_content)

    yaml_reader = YAMLFileReader(config_file_path=yaml_file)
    config = yaml_reader.read_file()

    assert isinstance(config, dict)
    assert config["tasks"][0]["name"] == "Task 1"
    assert config["tasks"][0]["value"] == 42
    assert config["tasks"][1]["name"] == "Task 2"
    assert config["tasks"][1]["value"] == 24


def test_render_jinja_template():
    """Test render jinja template."""
    json_path = "test/oih/unit/mdp_extraction_framework/resources/conf/pipeline.json"
    input_txt = json.dumps(INPUT_CONFIG, indent=4)

    mapping = ConfigMapping(pos_dt="2023-10-31")

    json_cls = JSONReader(config_file_path=json_path)
    config = json_cls.render_jinja_template(input_txt, mapping)

    expected_config = {
        "job_name": "test_job",
        "pipeline_name": "IngestionPipeline",
        "tasks": {
            "read_source_file_task": {
                "module_name": "ParquetSourceFileReaderTask",
                "bypass_flag": "False",
                "parameters": {
                    "filter_cond": "ptn_yyyy = '2023'",
                    "date": "31",
                    "month": "10",
                    "year": "2023",
                    "year_be": "2566",
                    "quarter": "04",
                },
            }
        },
    }
    assert config == expected_config


def test_overwrite_config():
    """Test overwriting config."""
    overwrite_dict = {
        "job_name": "overwritten_job_name",
        "tasks": {
            "read_source_file_task": {
                "bypass_flag": "True",
                "parameters": {"filter_cond": "ptn_mm = '{{ptn_mm}}'"},
            }
        },
    }
    json_path = "test/oih/unit/framework/mdp_control_framework/resources/conf/pipeline.json"
    input_txt = json.dumps(INPUT_CONFIG, indent=4)

    json_cls = JSONReader(config_file_path=json_path)
    config = json_cls.overwrite_config(input_txt, overwrite_dict)

    expected_config = {
        "job_name": "overwritten_job_name",
        "pipeline_name": "IngestionPipeline",
        "tasks": {
            "read_source_file_task": {
                "module_name": "ParquetSourceFileReaderTask",
                "bypass_flag": "True",
                "parameters": {
                    "filter_cond": "ptn_mm = '{{ptn_mm}}'",
                    "date": "{{ptn_dd}}",
                    "month": "{{ptn_mm}}",
                    "year": "{{ptn_yyyy}}",
                    "year_be": "{{ptn_yyyy_be}}",
                    "quarter": "{{ptn_qtr}}",
                },
            }
        },
    }

    expected_txt = json.dumps(expected_config, indent=4)
    assert config.strip() == expected_txt.strip()


def test_read_and_render_config():
    """Test read and render configuration file util."""
    file_path = "test/oih/unit/mdp_extraction_framework/resources/conf/sample_config.json"
    mapping = {"pos_dt": "1999-01-01"}

    result_config = read_and_render_config(file_path=file_path, config_mapping=mapping)
    expected_config = {
        "job_name": "test_job",
        "pipeline_name": "TestPipeline",
        "tasks": {
            "sample_task": {
                "module_name": "SampleTask",
                "bypass_flag": False,
                "parameters": {
                    "pos_dt": "1999-01-01",
                    "date": "01",
                    "month": "01",
                    "year": "1999",
                    "year_be": "2542",
                    "quarter": "01",
                },
            }
        },
    }

    assert (
        result_config == expected_config
    ), f"The result config {result_config} does not match with expected config {expected_config}."


@pytest.mark.parametrize(
    "keep_undefined, expected_result",
    [
        (True, "Hello, John! {{ undefined_var }}"),
        (False, "Hello, John! "),
    ],
    ids=["keep undefined variable", "discard undefined variable"],
)
def test_render_template(keep_undefined, expected_result):
    """Test render_template with different keep_undefined values."""
    template_content = "Hello, {{ name }}! {{ undefined_var }}"
    mapping = {"name": "John"}

    result = render_template(template_content, mapping, keep_undefined=keep_undefined)

    assert (
        result == expected_result
    ), f"Result rendered content `{result}` does not match the expected content `{expected_result}`"


# @pytest.mark.parametrize(
#     "keep_undefined, expected_result",
#     [
#         (True, "Hello, John! {{ undefined_var }}"),
#         (False, "Hello, John! "),
#     ],
#     ids=["keep undefined variable", "discard undefined variable"],
# )ACCOUNT_NAME
def test_EnvSettings():
    """Test the pydantic class 'ENVSettings' which loads the parameters from the
    environment variables."""
    os.environ["LOCAL_STORAGE__filepath"] = "test_local/filepath"

    env_settings = EnvSettings()

    # assertion for local storage attributes
    assert env_settings.local_storage.filepath == "test_local/filepath"


def test_render_with_env_variable():
    """Test render_template with different keep_undefined values."""

    file_path = "test/oih/unit/mdp_extraction_framework/resources/conf/extraction_config.json"
    mapping = {"pos_dt": "2024-01-01"}

    result_config = read_and_render_config(file_path=file_path, config_mapping=mapping)

    expected_config = {
        "job_name": "test_job",
        "pipeline_name": "TestPipeline",
        "tasks": {
            "sample_task": {
                "module_name": "SampleTask",
                "bypass_flag": False,
                "parameters": {
                    "azcopy_command": "cp",
                    "source": {
                        "type": "LocalLocation",
                        "filepath": "test_local/filepath/test_area/extrct_sit_table_d_20240101.txt",
                    },
                    "target": {
                        "type": "ADLSLocation",
                        "storage_account": "testadls001dev",
                        "storage_container": "test_container",
                        "sas_token": "test_token",
                        "filepath": "test_adls/filepath/fw_test/azcopy/extrct_sit_table_d_20240101.txt",
                    },
                },
            }
        },
    }

    assert (
        result_config == expected_config
    ), f"The result config {result_config} does not match with expected config {expected_config}."
