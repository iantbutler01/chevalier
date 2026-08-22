from typing import Union

from .chevalier import (
    AssistantResponse,
    ChevalierError,
    CompleteStreamEvent,
    McpClient,
    McpServer,
    OutputStreamEvent,
    ProviderRateLimit,
    RateLimitsStreamEvent,
    ReasoningResponsePart,
    Runtime,
    SignatureResponsePart,
    StreamHandle,
    TextResponsePart,
    TokenUsage,
    ToolCall,
    ToolPartialStreamEvent,
    ToolResponsePart,
    UsageStreamEvent,
    VfsContentHasher,
    VfsStorage,
    version,
    vfs_content_hash,
    vfs_content_hash_algorithm,
)

ResponsePart = Union[
    TextResponsePart,
    ReasoningResponsePart,
    ToolResponsePart,
    SignatureResponsePart,
]
ResponseStreamEvent = Union[
    OutputStreamEvent,
    ToolPartialStreamEvent,
    UsageStreamEvent,
    RateLimitsStreamEvent,
    CompleteStreamEvent,
]


def _initialize_chevalier_error(
    self, message, code="ERROR", retryable=False, output=None
):
    Exception.__init__(self, message)
    self.code = code
    self.retryable = retryable
    self.output = output


ChevalierError.__init__ = _initialize_chevalier_error
__version__ = version()
__all__ = [
    "AssistantResponse",
    "ChevalierError",
    "CompleteStreamEvent",
    "McpClient",
    "McpServer",
    "OutputStreamEvent",
    "ProviderRateLimit",
    "RateLimitsStreamEvent",
    "ReasoningResponsePart",
    "ResponsePart",
    "ResponseStreamEvent",
    "Runtime",
    "SignatureResponsePart",
    "StreamHandle",
    "TextResponsePart",
    "TokenUsage",
    "ToolCall",
    "ToolPartialStreamEvent",
    "ToolResponsePart",
    "UsageStreamEvent",
    "VfsContentHasher",
    "VfsStorage",
    "version",
    "vfs_content_hash",
    "vfs_content_hash_algorithm",
]
