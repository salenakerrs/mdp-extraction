"""Abstract base class for reading config files."""

# import: standard
import json
import pathlib
from abc import ABC
from abc import abstractmethod
from copy import deepcopy
from dataclasses import asdict
from typing import Any
from typing import Union

# import: internal
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import ConfigMapping
from mdp.framework.mdp_extraction_framework.config_validator.config_mapping import EnvSettings

# import: external
import yaml
from jinja2 import BaseLoader
from jinja2 import DebugUndefined
from jinja2 import Environment
from jinja2 import Undefined


def get_object_attribute_mapping(env_settings: EnvSettings) -> dict[str, str]:
    """Create a mapping from each attribute and its value from an object."""
    return {
        attr: getattr(env_settings, attr) for attr in dir(env_settings) if not attr.startswith("_")
    }


def render_template(content: str, mapping, keep_undefined: bool = True) -> str:
    """Render a Jinja2 template configuration string.

    If `keep_undefined` is set to False, undefined variables in the template will be replaced
    with an empty string in the rendered content. Otherwise, the rendered content remains
    unchanged for undefined variables.

    Args:
        content (str): The content of the Jinja2 template configuration.
        mapping: The mapping for key values used in the template.
        keep_undefined (bool): Flag to keep or discard undefined variables. Defaults to True.

    Returns:
        str: The rendered configuration.
    """

    jinja_env = Environment(
        loader=BaseLoader, undefined=DebugUndefined if keep_undefined else Undefined
    )
    template = jinja_env.from_string(content)

    env_settings = EnvSettings()
    env_settings_mapping = get_object_attribute_mapping(env_settings)
    mappings = {**env_settings_mapping, **mapping}

    rendered_content = template.render(**mappings)
    return rendered_content


class ConfigFileReader(ABC):
    """Abstract base class for reading config files."""

    def __init__(self, config_file_path: str) -> None:
        """__init__ function of FileReader.

        Args:
            config_file_path (str):  Input file path.
        """
        self.config_file_path = config_file_path

    @abstractmethod
    def read_file(self) -> Any:
        """Base method for reading config files."""
        pass

    def update_dict(self, config: dict, input_overwrite: dict) -> None:
        """Recursively updates the provided 'config' dictionary with values from
        'input_overwrite' for specified keys, without affecting other keys.

        Args:
            config (dict): The original dictionary to be updated.
            input_overwrite (dict): The dictionary containing key-value pairs to update 'config'.
        """
        for key, value in input_overwrite.items():
            if isinstance(value, dict) and key in config and isinstance(config[key], dict):
                self.update_dict(config[key], value)
            else:
                config[key] = value

    def overwrite_config(self, content: str, input_overwrite: dict) -> str:
        """Method for overwriting config with input dictionary.

        Args:
            content (str): string of configuration
            input_overwrite (dict): input dictionary to overwrite config

        Returns:
            str: overwritten configuration
        """
        config_dict = json.loads(content)
        overwrite_config = deepcopy(config_dict)
        self.update_dict(overwrite_config, input_overwrite)
        return json.dumps(overwrite_config, indent=4)

    def render_jinja_template(self, content: str, mapping: ConfigMapping) -> Any:
        """Method for rendering Jinja2 template configuration string.

        Args:
            content (str): content of the Jinja2 template configuration
            mapping (ConfigMapping): mapping for key values used only in J2Reader.

        Returns:
            Any: output rendered config
        """
        rendered_config = render_template(content, asdict(mapping))
        return json.loads(rendered_config)


class JSONReader(ConfigFileReader):
    """Subclass for reading JSON Jinja2 template config files."""

    def read_file(self) -> str:
        """Read a Json config file and return a string of configuration.

        Returns:
            str: A string of configuration parameters.
        """
        return pathlib.Path(self.config_file_path).read_text()


class YAMLFileReader(ConfigFileReader):
    """Subclass for reading YAML Jinja2 template config files."""

    def read_file(self) -> Any:
        """Read a YAML config file and return a dictionary of configuration.

        Returns:
            dict: A dictionary of configuration parameters.
        """
        content = pathlib.Path(self.config_file_path).read_text()
        return yaml.safe_load(content)


def read_and_render_config(file_path: str, config_mapping: dict) -> Union[dict, list]:
    """Read configuration from the specified path and render a Jinja2 template using the
    provided `config_mapping`.

    Args:
        file_path (str): The path to the configuration file.
        config_mapping (dict): A dictionary representing key-value pairs for rendering the configuration template.

    Returns:
        Union[dict, list]: The completed configuration with the rendered template.
    """
    json_reader = JSONReader(config_file_path=file_path)
    template_config = json_reader.read_file()
    config = json_reader.render_jinja_template(template_config, ConfigMapping(**config_mapping))
    return config
