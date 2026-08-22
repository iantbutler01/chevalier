import inspect
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Type

from pydantic import BaseModel, ConfigDict, create_model
from typing_extensions import get_type_hints


PreparedTool = Tuple[str, str, Dict[str, Any], Callable[[Any], Awaitable[str]]]


def _annotations(handler: Callable[..., Awaitable[Optional[str]]]) -> Dict[str, Any]:
    try:
        return get_type_hints(handler, include_extras=True)
    except Exception as error:
        raise TypeError(f"could not resolve tool annotations: {error}") from error


def prepare_tool(
    handler: Callable[..., Awaitable[Optional[str]]],
    name: Optional[str],
    description: Optional[str],
) -> PreparedTool:
    if not inspect.iscoroutinefunction(handler):
        raise TypeError("tool handler must be an async function")

    signature = inspect.signature(handler)
    annotations = _annotations(handler)
    fields: Dict[str, Any] = {}

    for parameter in signature.parameters.values():
        if parameter.kind in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            raise TypeError(
                f"tool parameter {parameter.name!r} must be a named parameter"
            )
        annotation = annotations.get(parameter.name, parameter.annotation)
        if annotation is inspect.Parameter.empty:
            raise TypeError(f"tool parameter {parameter.name!r} requires a type annotation")
        default = ... if parameter.default is inspect.Parameter.empty else parameter.default
        fields[parameter.name] = (annotation, default)

    tool_name = name if name is not None else getattr(handler, "__name__", None)
    if not isinstance(tool_name, str):
        raise TypeError("tool handler requires a name")
    arguments_model: Type[BaseModel] = create_model(
        f"{tool_name}Arguments",
        __config__=ConfigDict(extra="forbid"),
        **fields,
    )
    schema = arguments_model.model_json_schema()
    tool_description = (
        description if description is not None else inspect.getdoc(handler) or ""
    )

    async def invoke(arguments: Any) -> str:
        values = arguments_model.model_validate(arguments)
        kwargs = {
            parameter_name: getattr(values, parameter_name)
            for parameter_name in signature.parameters
        }
        result = await handler(**kwargs)
        if result is None:
            return ""
        if not isinstance(result, str):
            raise TypeError("tool handler must return a string or None")
        return result

    return tool_name, tool_description, schema, invoke
