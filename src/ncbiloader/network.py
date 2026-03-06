# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import email.utils
import math
import random
import time
import typing
from collections.abc import AsyncIterator
from typing import TypedDict, Unpack

import httpx
from aiolimiter import AsyncLimiter
from httpx._client import UseClientDefault
from httpx._types import (
    AuthTypes,
    CookieTypes,
    HeaderTypes,
    QueryParamTypes,
    RequestContent,
    RequestData,
    RequestExtensions,
    RequestFiles,
    TimeoutTypes,
)

from ncbiloader.monitor import ProgressMonitor


class RequestOptions(TypedDict, total=False):
    content: RequestContent | None
    data: RequestData | None
    files: RequestFiles | None
    json: typing.Any | None
    params: QueryParamTypes | None
    headers: HeaderTypes | None
    cookies: CookieTypes | None
    auth: AuthTypes | UseClientDefault | None
    follow_redirects: bool | UseClientDefault
    timeout: TimeoutTypes | UseClientDefault
    extensions: RequestExtensions | None


class NetworkClient:
    """
    Asynchronous HTTP client wrapper providing rate limiting, connection pooling,
    and robust error handling with exponential backoff and full jitter.
    """

    def __init__(
        self, threads: int, monitor: ProgressMonitor, timeout: float = 10.0, http2: bool = True
    ) -> None:
        self.monitor = monitor
        # +10% buffer for max_connections to prevent TIME_WAIT bottlenecks
        limits = httpx.Limits(max_connections=math.ceil(threads * 1.1), max_keepalive_connections=threads)
        self.limiter = AsyncLimiter(threads * 2, 1)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, read=5.0),
            limits=limits,
            http2=http2,
            follow_redirects=True,
            headers={"Accept-Encoding": "identity", "User-Agent": "NCBILoader/1.0"},
        )

    async def safe_request(
        self, method: str, url: str, **kwargs: Unpack[RequestOptions]
    ) -> httpx.Response | None:
        """
        Executes an HTTP request with built-in retry logic.

        Handles rate limits (429) using the 'Retry-After' header and applies
        exponential backoff with full jitter for server-side errors (5xx).

        Args:
            method: HTTP method (e.g., "GET", "HEAD").
            url: Target URL.
            **kwargs: Additional options passed to httpx.request.

        Returns:
            httpx.Response on success, or None if all retry attempts fail.
        """
        retry_codes = {408, 429, 500, 502, 503, 504}

        for attempt in range(1, 4):  # 3 attempts total
            async with self.limiter:
                try:
                    resp = await self.client.request(method, url, **kwargs)

                    if resp.status_code < 400:
                        return resp

                    if resp.status_code in retry_codes:
                        # Smart delay: Server instruction OR Exponential Jitter
                        server_delay = self._get_retry_after(resp)
                        delay = server_delay if server_delay is not None else random.uniform(0, 2**attempt)

                        self.monitor.log(
                            f"Attempt {attempt} failed ({resp.status_code}) for {url}."
                            f"Retrying in {delay:.2f}s...",
                            status="WARNING",
                        )
                        await asyncio.sleep(delay)
                        continue

                    # Fatal errors (404, 403, 401, etc.)
                    self.monitor.log(f"Fatal HTTP error {resp.status_code} for {url}", status="ERROR")
                    return None

                except (httpx.ConnectError, httpx.TimeoutException) as exc:
                    delay = random.uniform(0, 2**attempt)
                    self.monitor.log(
                        f"Network issue ({type(exc).__name__}) on {url}. Retrying in {delay:.2f}s...",
                        status="WARNING",
                    )
                    await asyncio.sleep(delay)
                except httpx.RequestError as exc:
                    self.monitor.log(f"Unrecoverable request error for {url}: {exc}", status="ERROR")
                    break

        return None

    @contextlib.asynccontextmanager
    async def stream_chunk(
        self, url: str, headers: dict[str, str], timeout: int
    ) -> AsyncIterator[httpx.Response]:
        """
        Context manager for streaming large file chunks asynchronously.

        Args:
            url: Target URL to download.
            headers: HTTP headers (usually containing 'Range' bytes).
            timeout: Absolute timeout for the entire chunk download process.

        Yields:
            httpx.Response object configured for async byte streaming.

        Raises:
            httpx.HTTPStatusError: If server responds with 400+ status code.
            TimeoutError: If the chunk download exceeds the specified absolute timeout.
        """
        async with asyncio.timeout(timeout):
            async with self.client.stream("GET", url, headers=headers) as response:
                if response.status_code >= 400:
                    self.monitor.log(f"HTTP Error: {response.status_code} for {url}", "ERROR")
                    raise httpx.HTTPStatusError(
                        f"Error {response.status_code}", request=response.request, response=response
                    )

                yield response

    def _get_retry_after(self, response: httpx.Response) -> float | None:
        """Parses the 'Retry-After' header into seconds."""
        header = response.headers.get("Retry-After")
        if not header:
            return None
        if header.isdigit():
            return float(header)
        try:
            parsed_date = email.utils.parsedate_tz(header)
            if parsed_date:
                return max(0, email.utils.mktime_tz(parsed_date) - time.time())
        except ValueError, TypeError:
            pass
        return None

    async def close(self) -> None:
        """Safely closes the underlying HTTP connection pool."""
        await self.client.aclose()
