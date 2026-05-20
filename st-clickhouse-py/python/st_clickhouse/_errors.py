from __future__ import annotations


class ClickHouseError(Exception):
    """Base exception for all ClickHouse errors."""


class ProtocolError(ClickHouseError):
    """Protocol violation — unexpected packet, invalid data, desync."""


class ConnectionError(ClickHouseError):
    """Network or I/O errors — broken pipe, reset, unreachable."""


class AuthenticationError(ClickHouseError):
    """Authentication failure — invalid credentials."""


class QueryError(ClickHouseError):
    """Server returned an exception for the query."""


class TimeoutError(ClickHouseError):
    """Operation timed out (connect, query, receive)."""


class CompressionError(ClickHouseError):
    """Compression/decompression failure."""


class ConfigError(ClickHouseError):
    """Configuration error — invalid address, missing feature."""


def map_error(exc: Exception) -> ClickHouseError:
    """Map a native exception to the proper Python error type."""
    if isinstance(exc, ClickHouseError):
        return exc
    msg = str(exc)
    if "authentication" in msg.lower():
        return AuthenticationError(msg)
    if isinstance(exc, ConnectionError):
        return ConnectionError(msg)
    if isinstance(exc, ValueError):
        if "compression" in msg.lower():
            return CompressionError(msg)
        if "protocol" in msg.lower():
            return ProtocolError(msg)
        return QueryError(msg)
    if isinstance(exc, TimeoutError):
        return TimeoutError(msg)
    if "config" in msg.lower():
        return ConfigError(msg)
    if "I/O" in msg or "io" in msg.lower() or "connection" in msg.lower():
        return ConnectionError(msg)
    if "server error" in msg.lower():
        return QueryError(msg)
    return ClickHouseError(msg)
