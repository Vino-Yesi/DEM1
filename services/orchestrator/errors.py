from __future__ import annotations

from typing import Any


class WMSError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


class NotFoundError(WMSError):
    def __init__(
        self, resource: str, key: Any, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(
            "NOT_FOUND",
            f"{resource} was not found",
            status_code=404,
            details={"key": str(key), **(details or {})},
        )


class ConflictError(WMSError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__("CONFLICT", message, status_code=409, details=details)


class ValidationError(WMSError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__("VALIDATION_ERROR", message, status_code=422, details=details)


class PermissionDeniedError(WMSError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__("PERMISSION_DENIED", message, status_code=403, details=details)


class InsufficientStockError(WMSError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(
            "INSUFFICIENT_STOCK", message, status_code=409, details=details
        )


class ConcurrencyRetryExhausted(WMSError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(
            "CONCURRENCY_RETRY_EXHAUSTED", message, status_code=409, details=details
        )
