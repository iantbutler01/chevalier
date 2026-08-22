import ast
import re
from pathlib import Path

import chevalier
import chevalier_sandbox


ROOT = Path(__file__).parents[1]

CORE_TYPE_ALIASES = {
    "ToolCallJs": "ToolCall",
    "ToolSchemaJs": "ToolSchema",
}

SANDBOX_TYPE_ALIASES = {
    "DurableVolumeInfoJs": "DurableVolumeInfo",
    "ExecEventJs": "ExecEvent",
    "HostPciDeviceJs": "HostPciDevice",
    "HostPciFunctionJs": "HostPciFunction",
    "HostPciInventoryJs": "HostPciInventory",
    "PciDeviceActionJs": "PciDeviceAction",
    "SessionCheckpointJs": "SessionCheckpoint",
    "SessionDirectoryEntryJs": "SessionDirectoryEntry",
    "SessionInfoJs": "SessionInfo",
    "SessionSnapshotJs": "SessionSnapshot",
    "ShellEventJs": "ShellEvent",
}


def snake_case(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def split_typescript_parameters(parameters):
    parts = []
    start = 0
    depth = 0
    for index, character in enumerate(parameters):
        if character in "({[<":
            depth += 1
        elif character in ")}]>":
            depth -= 1
        elif character == "," and depth == 0:
            parts.append(parameters[start:index])
            start = index + 1
    if parameters.strip():
        parts.append(parameters[start:])

    names = []
    for part in parts:
        match = re.match(r"\s*(\w+)\??\s*:", part)
        if match:
            name = snake_case(match.group(1))
            names.append("from_" if name == "from" else name)
    return names


def typescript_classes(path):
    source = path.read_text()
    classes = {}
    for match in re.finditer(r"export declare class (\w+) \{(.*?)\n\}", source, re.S):
        methods = {}
        for line in match.group(2).splitlines():
            method = re.match(r"\s*(?:static )?(?:get )?(\w+)\s*\((.*)\):", line)
            if method and method.group(1) != "constructor":
                methods[snake_case(method.group(1))] = split_typescript_parameters(
                    method.group(2)
                )
        classes[match.group(1)] = methods
    return classes


def typescript_functions(path):
    source = path.read_text()
    return {
        snake_case(name)
        for name in re.findall(r"export declare function (\w+)\s*\(", source)
    }


def typescript_interfaces(path):
    source = path.read_text()
    interfaces = {}
    for match in re.finditer(r"export interface (\w+) \{(.*?)\n\}", source, re.S):
        fields = set()
        required = set()
        for line in match.group(2).splitlines():
            field = re.match(r"\s*(\w+)(\?)?:\s", line)
            if field:
                name = snake_case(field.group(1))
                fields.add(name)
                if field.group(2) is None:
                    required.add(name)
        interfaces[match.group(1)] = (fields, required)
    return interfaces


def python_typed_dicts(path):
    typed_dicts = {}
    for node in ast.parse(path.read_text()).body:
        if not isinstance(node, ast.ClassDef) or not any(
            isinstance(base, ast.Name) and base.id == "TypedDict"
            for base in node.bases
        ):
            continue

        total = True
        for keyword in node.keywords:
            if keyword.arg == "total" and isinstance(keyword.value, ast.Constant):
                total = keyword.value.value

        fields = set()
        required = set()
        for child in node.body:
            if not isinstance(child, ast.AnnAssign) or not isinstance(
                child.target, ast.Name
            ):
                continue
            name = child.target.id
            fields.add(name)
            wrapper = (
                child.annotation.value.id
                if isinstance(child.annotation, ast.Subscript)
                and isinstance(child.annotation.value, ast.Name)
                else None
            )
            if wrapper == "Required" or (total and wrapper != "NotRequired"):
                required.add(name)
        typed_dicts[node.name] = (fields, required)
    return typed_dicts


def python_class_methods(path):
    classes = {}
    for node in ast.parse(path.read_text()).body:
        if not isinstance(node, ast.ClassDef):
            continue
        methods = {}
        for child in node.body:
            if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if child.name.startswith("_"):
                continue
            parameters = [
                argument.arg
                for argument in (
                    child.args.posonlyargs + child.args.args + child.args.kwonlyargs
                )
            ]
            if parameters and parameters[0] in {"self", "cls"}:
                parameters = parameters[1:]
            methods[child.name] = parameters
        classes[node.name] = methods
    return classes


def assert_binding_parity(
    module,
    declaration_path,
    stub_path,
    aliases,
    extra_exports=(),
    method_parameter_overrides=None,
    replaced_interfaces=(),
):
    method_parameter_overrides = method_parameter_overrides or {}
    classes = typescript_classes(declaration_path)
    functions = typescript_functions(declaration_path)
    assert set(module.__all__) == set(classes) | functions | set(extra_exports)

    stub_methods = python_class_methods(stub_path)
    for name, expected_methods in classes.items():
        actual_methods = {
            method for method in dir(getattr(module, name)) if not method.startswith("_")
        }
        assert actual_methods == set(expected_methods)
        for method, parameters in expected_methods.items():
            expected_parameters = method_parameter_overrides.get(
                (name, method), parameters
            )
            assert stub_methods[name][method] == expected_parameters

    interfaces = typescript_interfaces(declaration_path)
    typed_dicts = python_typed_dicts(stub_path)
    for name, expected_shape in interfaces.items():
        if name in replaced_interfaces:
            continue
        python_name = aliases.get(name, name)
        assert python_name in typed_dicts
        assert typed_dicts[python_name] == expected_shape


def test_core_binding_matches_typescript_capabilities_with_typed_rust_responses():
    typed_response_exports = {
        "AssistantResponse",
        "CompleteStreamEvent",
        "OutputStreamEvent",
        "ProviderRateLimit",
        "RateLimitsStreamEvent",
        "ReasoningResponsePart",
        "ResponsePart",
        "ResponseStreamEvent",
        "SignatureResponsePart",
        "TextResponsePart",
        "TokenUsage",
        "ToolCall",
        "ToolPartialStreamEvent",
        "ToolResponsePart",
        "UsageStreamEvent",
    }
    assert_binding_parity(
        chevalier,
        ROOT / "ts/native.d.ts",
        ROOT / "py/python/chevalier/__init__.pyi",
        CORE_TYPE_ALIASES,
        extra_exports={"ChevalierError"} | typed_response_exports,
        method_parameter_overrides={
            ("Runtime", "tool"): ["handler", "name", "description"]
        },
        replaced_interfaces={"RunResult", "StreamEvent", "ToolCallJs"},
    )


def test_sandbox_binding_matches_native_typescript_surface():
    assert_binding_parity(
        chevalier_sandbox,
        ROOT / "ts-sandbox/index.d.ts",
        ROOT / "py-sandbox/python/chevalier_sandbox/__init__.pyi",
        SANDBOX_TYPE_ALIASES,
    )
