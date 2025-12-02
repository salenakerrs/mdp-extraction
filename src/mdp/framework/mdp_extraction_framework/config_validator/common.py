"""PipelineConfigModel Class."""

# import: standard
from typing import Optional

# import: external
from pydantic import BaseModel


class TaskConfigModel(BaseModel):
    """Task config base model.

    Args:
        BaseModel: pydantic basemodel
    """

    module_name: str
    bypass_flag: Optional[bool] = False
    parameters: Optional[dict] = {}


class PipelineConfigModel(BaseModel):
    """Pipeline config base model.

    Args:
        BaseModel: pydantic basemodel
    """

    job_name: str
    pipeline_name: str
    job_info: dict
    tasks: dict[str, TaskConfigModel]


class PipelineTaskModel(BaseModel):
    """Pipeline Task base model."""

    pass


class ParametersConfigModel(BaseModel):
    """Parameter config base model."""

    pass
