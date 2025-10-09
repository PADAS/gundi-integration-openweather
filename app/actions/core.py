import importlib
import inspect
from typing import Optional

from pydantic import BaseModel
from app.services.utils import UISchemaModelMixin


class ActionConfiguration(UISchemaModelMixin, BaseModel):
    pass


class InternalActionConfiguration(BaseModel):
    pass


class ExecutableActionMixin:
    pass


class PullActionConfiguration(ExecutableActionMixin, ActionConfiguration):
    pass


class PushActionConfiguration(ActionConfiguration):
    pass


class AuthActionConfiguration(ActionConfiguration):
    pass


class GenericActionConfiguration(ActionConfiguration):
    pass


def discover_actions(module_name, prefix):
    action_handlers = {}
    # Import the module using importlib
    module = importlib.import_module(module_name)
    all_members = inspect.getmembers(module)

    # Iterate through the members and filter functions by prefix
    for name, func in all_members:
        if name.startswith(prefix) and inspect.isfunction(func):
            signature = inspect.signature(func)
            key = name[len(prefix):]  # Remove prefix
            if (config_annotation := signature.parameters.get("action_config").annotation) != inspect._empty:
                config_model = config_annotation
            else:
                config_model = GenericActionConfiguration
            if issubclass(config_model, PushActionConfiguration):  # Push actions
                if data_param := signature.parameters.get("data"):
                    if (data_annotation := data_param.annotation) != inspect._empty:
                        data_model = data_annotation
                    else:
                        raise ValueError(f"The 'data' parameter in action '{key}' must be annotated with a data model.")
                else:
                    raise ValueError(f"Push action '{key}' must accept a 'data' parameter.")
                if not signature.parameters.get("metadata"):
                    raise ValueError(f"Push action '{key}' must accept a 'metadata' parameter.")
            else:
                data_model = None
            action_handlers[key] = (func, config_model, data_model)

    return action_handlers



def get_actions():
    return list(discover_actions(module_name="app.actions.handlers", prefix="action_").keys())

def setup_action_handlers():
    return discover_actions(module_name="app.actions.handlers", prefix="action_")

def get_action_handler_by_data_type(type_name: str):
    for action_id, value in action_handlers.items():
        func, config_model, data_model = value
        if data_model and data_model.__name__ == type_name.strip():
            return action_id, func, config_model, data_model
    else:
        raise ValueError(f"No action handler found for data type '{type_name}'.")

action_handlers = setup_action_handlers()
