from urllib.parse import urlsplit, urlunsplit


def format_size(size_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def redact_url(url: str) -> str:
    """Return the URL with any embedded userinfo (user:pass) masked.

    Prevents basic-auth credentials from leaking into logs, error messages,
    or structured JSON output when the user supplies URLs of the form
    ``https://user:pass@host/path``.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return url

    if "@" not in (parts.netloc or ""):
        return url

    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"

    netloc = f"***:***@{host}" if host else "***:***@"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
