"""Tool errors and how exceptions map onto ToolResult.error_type."""

from __future__ import annotations

from typing import Literal

import httpx
import psycopg
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError, OperationalError

ErrorType = Literal[
    "validation",
    "timeout",
    "rate_limit",
    "not_found",
    "permission",
    "upstream",
    "approval_required",
    "unknown",
]

MAX_MESSAGE = 300


class ToolError(Exception):
    error_type: ErrorType = "unknown"
    retryable: bool = False

    def __init__(
        self, message: str, *, error_type: ErrorType | None = None, retryable: bool | None = None
    ) -> None:
        super().__init__(message)
        if error_type is not None:
            self.error_type = error_type
        if retryable is not None:
            self.retryable = retryable


class InvalidInput(ToolError):
    error_type = "validation"


class NotFound(ToolError):
    error_type = "not_found"


class PermissionDenied(ToolError):
    error_type = "permission"


class ApprovalRequired(ToolError):
    """A tier-2 tool was called without a matching, granted approval."""

    error_type = "approval_required"


class UpstreamError(ToolError):
    error_type = "upstream"
    retryable = True


class ToolTimeout(ToolError):
    error_type = "timeout"
    retryable = True


def _short(text: str) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= MAX_MESSAGE else text[: MAX_MESSAGE - 3] + "..."


def classify(exc: BaseException) -> tuple[ErrorType, bool, str]:
    """(error_type, retryable, short message) for any exception. Never a stack trace."""
    if isinstance(exc, ToolError):
        return exc.error_type, exc.retryable, _short(str(exc))
    if isinstance(exc, ValidationError):
        problems = "; ".join(
            f"{'.'.join(map(str, e['loc'])) or 'args'}: {e['msg']}" for e in exc.errors()[:5]
        )
        return "validation", False, _short(problems)
    if isinstance(exc, TimeoutError | httpx.TimeoutException):
        return "timeout", True, _short(f"timed out: {exc}" if str(exc) else "timed out")
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        msg = _short(f"HTTP {code} from {exc.request.url.path}")
        if code == 429:
            return "rate_limit", True, msg
        if code == 404:
            return "not_found", False, msg
        if code in (401, 403):
            return "permission", False, msg
        if code >= 500:
            return "upstream", True, msg
        return "unknown", False, msg
    if isinstance(exc, httpx.TransportError):
        return "upstream", True, _short(f"{type(exc).__name__}: {exc}")

    orig = exc.orig if isinstance(exc, DBAPIError) else exc
    if isinstance(orig, psycopg.errors.InsufficientPrivilege):
        return "permission", False, _short(str(orig).splitlines()[0])
    if isinstance(orig, psycopg.errors.QueryCanceled):
        return "timeout", False, _short("query cancelled by statement_timeout")
    if isinstance(orig, psycopg.errors.UndefinedTable | psycopg.errors.UndefinedColumn):
        return "not_found", False, _short(str(orig).splitlines()[0])
    if isinstance(orig, psycopg.errors.SyntaxError):
        return "validation", False, _short(str(orig).splitlines()[0])
    if isinstance(exc, OperationalError) or isinstance(orig, psycopg.OperationalError):
        return "upstream", True, _short("database unavailable: " + str(orig).splitlines()[0])
    if isinstance(orig, psycopg.Error):
        return "unknown", False, _short(str(orig).splitlines()[0])
    return "unknown", False, _short(f"{type(exc).__name__}: {exc}")
