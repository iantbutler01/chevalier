from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Union
from typing_extensions import Literal, Required, TypeAlias, TypedDict, final

Json = Any
AsyncToolHandler = Callable[..., Awaitable[Optional[str]]]
__all__: List[str]

class RuntimeOptions(TypedDict, total=False):
    model: str
    api_key: str

class MediaPartInput(TypedDict, total=False):
    type: Required[str]
    text: str
    image_base64: str
    mime_type: str
    image_url: str

class ToolCallInput(TypedDict, total=False):
    tool_use_id: Required[str]
    tool_name: Required[str]
    args: Required[str]

class Message(TypedDict, total=False):
    type: Required[str]
    role: str
    content: str
    tool_use_id: str
    tool_name: str
    is_error: bool
    parts: List[MediaPartInput]
    tool_calls: List[ToolCallInput]

class RunOptions(TypedDict, total=False):
    prompt: str
    system: str
    temperature: float
    top_p: float
    max_tokens: int
    model: str
    api_key: str
    output_schema: Json
    output_type: str
    history: List[Message]
    timeout_ms: float

class AnthropicCacheConfig(TypedDict, total=False):
    automatic_prompt_caching: str
    tool_definitions_cache_breakpoint: str

class CodexSubscriptionConfigInput(TypedDict, total=False):
    token: Required[str]
    account_id: str
    base_url: str
    transport: str
    sse_header_timeout_ms: float
    websocket_connect_timeout_ms: float
    reasoning_effort: str
    reasoning_summary: str
    text_verbosity: str
    service_tier: str

class KimiCodingConfigInput(TypedDict, total=False):
    token: Required[str]
    auth_kind: Required[str]
    base_url: str
    user_agent: str

class ProviderConfigInput(TypedDict, total=False):
    anthropic: AnthropicCacheConfig
    codex_subscription: CodexSubscriptionConfigInput
    kimi_coding: KimiCodingConfigInput

@final
class ToolCall:
    @property
    def tool_use_id(self) -> str: ...
    @property
    def tool_name(self) -> str: ...
    @property
    def args(self) -> Json: ...
    @property
    def raw_arguments(self) -> Optional[str]: ...
    @property
    def signature(self) -> Optional[str]: ...
    @property
    def tool_obj(self) -> Optional[Json]: ...

@final
class TextResponsePart:
    @property
    def text(self) -> str: ...

@final
class ReasoningResponsePart:
    @property
    def text(self) -> str: ...

@final
class ToolResponsePart:
    @property
    def call(self) -> ToolCall: ...

@final
class SignatureResponsePart:
    @property
    def value(self) -> str: ...

ResponsePart: TypeAlias = Union[
    TextResponsePart,
    ReasoningResponsePart,
    ToolResponsePart,
    SignatureResponsePart,
]

@final
class AssistantResponse:
    @property
    def output(self) -> List[ResponsePart]: ...
    def text(self) -> str: ...
    def as_str(self) -> Optional[str]: ...
    def reasoning(self) -> str: ...
    def tool_calls(self) -> List[ToolCall]: ...
    def signatures(self) -> List[str]: ...
    def has_tool_calls(self) -> bool: ...

@final
class TokenUsage:
    @property
    def input_tokens(self) -> int: ...
    @property
    def output_tokens(self) -> int: ...
    @property
    def cached_tokens(self) -> int: ...
    @property
    def cache_write_input_tokens(self) -> int: ...
    def total_tokens(self) -> int: ...

@final
class ProviderRateLimit:
    @property
    def scope(self) -> Literal["session", "subscription"]: ...
    @property
    def used_percent(self) -> int: ...
    @property
    def window_minutes(self) -> int: ...
    @property
    def resets_at_epoch_sec(self) -> int: ...

class ToolSchema(TypedDict):
    name: str
    description: str
    parameters: Json

@final
class OutputStreamEvent:
    @property
    def type(self) -> Literal["output"]: ...
    @property
    def output(self) -> ResponsePart: ...

@final
class ToolPartialStreamEvent:
    @property
    def type(self) -> Literal["toolPartial"]: ...
    @property
    def data(self) -> Json: ...

@final
class UsageStreamEvent:
    @property
    def type(self) -> Literal["usage"]: ...
    @property
    def usage(self) -> TokenUsage: ...

@final
class RateLimitsStreamEvent:
    @property
    def type(self) -> Literal["rateLimits"]: ...
    @property
    def rate_limits(self) -> List[ProviderRateLimit]: ...

@final
class CompleteStreamEvent:
    @property
    def type(self) -> Literal["complete"]: ...
    @property
    def response(self) -> AssistantResponse: ...

ResponseStreamEvent: TypeAlias = Union[
    OutputStreamEvent,
    ToolPartialStreamEvent,
    UsageStreamEvent,
    RateLimitsStreamEvent,
    CompleteStreamEvent,
]

class ChevalierError(Exception):
    code: str
    retryable: bool
    output: Optional[str]
    def __init__(self, message: str, code: str = ..., retryable: bool = ..., output: Optional[str] = ...) -> None: ...

@final
class StreamHandle:
    def next(self) -> Awaitable[Optional[ResponseStreamEvent]]: ...
    def close(self) -> None: ...

@final
class Runtime:
    def __new__(cls, options: Optional[RuntimeOptions] = ...) -> Runtime: ...
    def run(self, options: RunOptions) -> Awaitable[AssistantResponse]: ...
    def tool(self, handler: AsyncToolHandler, *, name: Optional[str] = ..., description: Optional[str] = ...) -> Awaitable[None]: ...
    def register_tool_schema(self, name: str, description: str, schema: Json) -> Awaitable[None]: ...
    def execute_tool_call(self, tool_name: str, args: Json) -> Awaitable[str]: ...
    def run_stream(self, options: RunOptions) -> Awaitable[StreamHandle]: ...
    def get_tool_schemas(self) -> Awaitable[List[ToolSchema]]: ...
    def set_system_messages(self, messages: Sequence[Message]) -> Awaitable[None]: ...
    def set_default_prompt(self, prompt: str) -> Awaitable[None]: ...
    def set_provider_config(self, config: ProviderConfigInput) -> Awaitable[None]: ...
    def raw_response(self) -> Awaitable[str]: ...
    def reasoning(self) -> Awaitable[str]: ...
    def reasoning_segments(self) -> Awaitable[Json]: ...
    def mcp(self, uri: str) -> Awaitable[None]: ...
    def mcp_as(self, uri: str, label: str) -> Awaitable[None]: ...
    def dispose(self) -> Awaitable[None]: ...

class McpHttpConfig(TypedDict, total=False):
    transport: Required[Literal["http", "websocket"]]
    url: Required[str]
    headers: Dict[str, str]

class McpStdioConfig(TypedDict, total=False):
    transport: Required[Literal["stdio"]]
    command: Required[str]
    args: List[str]
    env: Dict[str, str]
    cwd: str

McpClientConfig = Union[McpHttpConfig, McpStdioConfig]

class McpServerOptions(TypedDict, total=False):
    version: str
    description: str

@final
class McpClient:
    @staticmethod
    def connect(config: McpClientConfig) -> Awaitable[McpClient]: ...
    @staticmethod
    def http(url: str) -> Awaitable[McpClient]: ...
    @staticmethod
    def websocket(url: str) -> Awaitable[McpClient]: ...
    @staticmethod
    def stdio(command_line: str) -> Awaitable[McpClient]: ...
    def list_tools(self) -> Awaitable[Json]: ...
    def call_tool(self, name: str, args: Json) -> Awaitable[Json]: ...
    def list_resources(self) -> Awaitable[Json]: ...
    def read_resource(self, uri: str) -> Awaitable[Json]: ...

@final
class McpServer:
    def __new__(cls, name: str, options: Optional[McpServerOptions] = ...) -> McpServer: ...
    def tool(self, name: str, description: str, schema: Json, handler: AsyncToolHandler) -> Awaitable[None]: ...
    def serve(self, transport: str, addr: Optional[str] = ...) -> Awaitable[None]: ...

class GatewayOptions(TypedDict, total=False):
    endpoint: Required[str]
    auth_token: str
    scope_path: str
    component: str
    mutation_reason: str

class VfsWriteOptions(TypedDict, total=False):
    if_match: Optional[str]
    expected_file_id: Optional[str]
    executable: bool
    mode: int

class VfsMetadataOptions(TypedDict, total=False):
    max_hash_bytes: Optional[int]

class VfsPrefetchOptions(TypedDict, total=False):
    include_small_file_bytes: bool
    max_entries: int
    max_pack_bytes: int

class VfsObjectState(TypedDict):
    size_bytes: int
    pack_key: str
    pack_slot_offset: int
    pack_slot_length: int
    pack_slot_compression: int

class VfsMetadata(TypedDict, total=False):
    path: Required[str]
    kind: Required[Literal["File", "Directory", "Symlink", "Special", "Unknown"]]
    size_bytes: Required[int]
    file_id: Optional[str]
    link_count: int
    link_target: Optional[str]
    mode: Optional[int]
    executable: bool
    content_hash: Optional[str]
    token_count: Optional[int]
    version: Optional[str]
    updated_at: Optional[str]
    mtime_ns: Optional[int]
    ctime_ns: Optional[int]
    object_state: Optional[VfsObjectState]

class VfsHardLinkResult(TypedDict):
    source: VfsMetadata
    destination: VfsMetadata

class VfsPrefetchFileBytes(TypedDict):
    path: str
    body: bytes

class VfsSeededHash(TypedDict):
    path: str
    size_bytes: str
    mtime_ns: str
    ctime_ns: str
    content_hash: str

class VfsMkdirOptions(TypedDict, total=False):
    mode: Optional[int]

class VfsRemoveOptions(TypedDict, total=False):
    if_match: Optional[str]

@final
class VfsContentHasher:
    def __new__(cls) -> VfsContentHasher: ...
    def update(self, chunk: bytes) -> None: ...
    def digest(self) -> str: ...

@final
class VfsStorage:
    @staticmethod
    def local(root: str) -> VfsStorage: ...
    @staticmethod
    def gateway(options: GatewayOptions) -> VfsStorage: ...
    def read(self, path: str) -> Awaitable[bytes]: ...
    def read_range(self, path: str, offset: int, length: int) -> Awaitable[bytes]: ...
    def write(self, path: str, data: bytes, options: Optional[VfsWriteOptions] = ...) -> Awaitable[Json]: ...
    def write_from_file(self, path: str, source_path: str, expected_content_hash: str, options: Optional[VfsWriteOptions] = ...) -> Awaitable[Json]: ...
    def seed_hash_cache(self, entries: Sequence[VfsSeededHash]) -> int: ...
    def stat(self, path: str, options: Optional[VfsMetadataOptions] = ...) -> Awaitable[Optional[VfsMetadata]]: ...
    def list_dir(self, path: str, options: Optional[VfsMetadataOptions] = ...) -> Awaitable[List[VfsMetadata]]: ...
    def metadata_many(self, paths: Sequence[str]) -> Awaitable[List[Optional[VfsMetadata]]]: ...
    def prefetch_subtree(self, prefix: str, options: Optional[VfsPrefetchOptions] = ...) -> Awaitable[List[VfsPrefetchFileBytes]]: ...
    def mkdir(self, path: str, options: Optional[VfsMkdirOptions] = ...) -> Awaitable[None]: ...
    def create_symlink(self, path: str, target: str) -> Awaitable[None]: ...
    def create_hard_link(self, source: str, destination: str) -> Awaitable[VfsHardLinkResult]: ...
    def find_hard_link_alias(self, file_id: str, excluding_path: str) -> Awaitable[Optional[str]]: ...
    def remove(self, path: str, options: Optional[VfsRemoveOptions] = ...) -> Awaitable[Json]: ...
    def rmdir(self, path: str) -> Awaitable[None]: ...
    def rename(self, from_: str, to: str) -> Awaitable[Json]: ...
    def apply_namespace_batch(self, mutations: Json) -> Awaitable[None]: ...
    def write_many(self, writes: Json) -> Awaitable[Json]: ...

def vfs_content_hash(bytes: bytes) -> str: ...
def vfs_content_hash_algorithm() -> str: ...
def version() -> str: ...

__version__: str
