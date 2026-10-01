from __future__ import annotations

from typing import Literal, Union

from .chevalier import (
    AssistantResponse,
    ClaudeSession,
    claude_subscription_status,
    ChevalierError,
    CompleteStreamEvent,
    McpClient,
    McpServer,
    OutputStreamEvent,
    ProviderRateLimit,
    RateLimitsStreamEvent,
    ReasoningResponsePart,
    Runtime,
    ProgrammaticExecution,
    programmatic_description,
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


class _ClaudeEvent(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as error:
            raise AttributeError(name) from error


class ClaudeInitEvent(_ClaudeEvent):
    type: Literal["init"]
    sessionId: str
    cwd: str
    model: str
    cliVersion: str


class ClaudeTextDeltaEvent(_ClaudeEvent):
    type: Literal["textDelta"]
    text: str


class ClaudeThinkingDeltaEvent(_ClaudeEvent):
    type: Literal["thinkingDelta"]
    text: str


class ClaudeAssistantMessageEvent(_ClaudeEvent):
    type: Literal["assistantMessage"]
    text: str


class ClaudeToolCallEvent(_ClaudeEvent):
    type: Literal["toolCall"]
    callId: str
    call: dict


class ClaudeToolExecutedEvent(_ClaudeEvent):
    type: Literal["toolExecuted"]
    call: dict
    output: dict


class ClaudeToolCancelledEvent(_ClaudeEvent):
    type: Literal["toolCancelled"]
    callId: str


class ClaudeRateLimitsEvent(_ClaudeEvent):
    type: Literal["rateLimits"]
    data: list


class ClaudeApiRetryEvent(_ClaudeEvent):
    type: Literal["apiRetry"]
    attempt: int
    delayMs: int
    error: str


class ClaudeTurnCompleteEvent(_ClaudeEvent):
    type: Literal["turnComplete"]
    usage: dict
    listPriceUsd: float | None
    numTurns: int
    isError: bool
    subtype: str
    result: str | None


ClaudeSessionEvent = Union[
    ClaudeInitEvent, ClaudeTextDeltaEvent, ClaudeThinkingDeltaEvent,
    ClaudeAssistantMessageEvent, ClaudeToolCallEvent, ClaudeToolExecutedEvent,
    ClaudeToolCancelledEvent, ClaudeRateLimitsEvent, ClaudeApiRetryEvent,
    ClaudeTurnCompleteEvent,
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
    "ClaudeSession",
    "claude_subscription_status",
    "ClaudeInitEvent",
    "ClaudeTextDeltaEvent",
    "ClaudeThinkingDeltaEvent",
    "ClaudeAssistantMessageEvent",
    "ClaudeToolCallEvent",
    "ClaudeToolExecutedEvent",
    "ClaudeToolCancelledEvent",
    "ClaudeRateLimitsEvent",
    "ClaudeApiRetryEvent",
    "ClaudeTurnCompleteEvent",
    "ClaudeSessionEvent",
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
    "ProgrammaticExecution",
    "programmatic_description",
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
