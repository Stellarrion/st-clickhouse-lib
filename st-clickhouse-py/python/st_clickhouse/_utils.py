from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from ._errors import ConfigError


def parse_connect_args(addr: str, kwargs: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Parse clickhouse:// URLs into the plain host:port native client shape."""
    if not (addr.startswith("clickhouse://") or addr.startswith("clickhouses://")):
        return addr, kwargs

    parsed = urlparse(addr)
    if parsed.scheme not in {"clickhouse", "clickhouses"} or not parsed.hostname:
        raise ConfigError(f"invalid ClickHouse URL: {addr!r}")

    out = dict(kwargs)
    port = parsed.port or (9440 if parsed.scheme == "clickhouses" else 9000)
    host = parsed.hostname
    native_addr = (
        f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"
    )

    if parsed.username and "user" not in out:
        out["user"] = unquote(parsed.username)
    if parsed.password and "password" not in out:
        out["password"] = unquote(parsed.password)
    if parsed.path and parsed.path != "/" and "database" not in out:
        out["database"] = unquote(parsed.path.lstrip("/"))
    if parsed.scheme == "clickhouses" and "tls" not in out:
        out["tls"] = True

    return native_addr, out


def merge_query_params(
    params: Optional[Dict[str, Any]],
    kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    if params is None:
        return dict(kwargs)
    merged = dict(params)
    merged.update(kwargs)
    return merged


def format_values(rows: List[Dict[str, Any]], col_names: List[str]) -> str:
    """Format rows as VALUES clause for INSERT."""

    def _escape(val: Any) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "1" if val else "0"
        if isinstance(val, (int, float)):
            return str(val)
        if isinstance(val, str):
            escaped = val.replace("'", "\\'").replace("\\", "\\\\")
            return f"'{escaped}'"
        if isinstance(val, bytes):
            return f"'{val.decode('utf-8', errors='replace')}'"
        return f"'{str(val)}'"

    values = ", ".join(
        "(" + ", ".join(_escape(row.get(c, None)) for c in col_names) + ")"
        for row in rows
    )
    return f"VALUES {values}"
