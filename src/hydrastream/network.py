# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import email.utils
import math
import mimetypes
import random
import re
import ssl
import time
import typing
from collections.abc import AsyncIterator
from typing import Any, TypedDict, Unpack, cast
from urllib.parse import unquote

import httpx
from aiolimiter import AsyncLimiter
from httpx._client import UseClientDefault
from httpx._types import (
    AuthTypes,
    CookieTypes,
    HeaderTypes,
    ProxyTypes,
    QueryParamTypes,
    RequestContent,
    RequestData,
    RequestExtensions,
    RequestFiles,
    TimeoutTypes,
)

from hydrastream.monitor import ProgressMonitor


class HttpxClientOptions(TypedDict, total=False):
    headers: HeaderTypes | None
    cookies: CookieTypes | None
    auth: AuthTypes | None
    proxy: ProxyTypes | None
    timeout: TimeoutTypes
    verify: ssl.SSLContext | str | bool
    follow_redirects: bool
    http2: bool
    http1: bool


DEFAULT_OPTIONS: HttpxClientOptions = {
    "timeout": httpx.Timeout(10.0, read=5.0),
    "http2": True,
    "follow_redirects": True,
}


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


class DynamicRateLimiter:
    """
    Advanced rate limiter implementing AIMD (Additive Increase/Multiplicative Decrease)
    and Circuit Breaker patterns to dynamically adapt to server constraints.

    - Multiplicative Decrease: Halves the allowed RPS upon encountering HTTP 429 errors.
    - Additive Increase: Slowly probes for higher RPS limits during successful requests.
    - Circuit Breaker: Halts all outgoing requests globally if the server demands a long pause.
    """

    def __init__(
        self,
        initial_rps: int,
        min_rps: int = 1,
        cooldown_seconds: int = 30,
        break_duration: int = 300,
    ) -> None:
        self.max_rps = initial_rps
        self.current_rps = initial_rps
        self.min_rps = min_rps

        self._limiter = AsyncLimiter(initial_rps, 1)
        self._lock = asyncio.Lock()

        # Timestamp of the last 429 error
        self._last_429_time: float = 0.0
        self.default_break = break_duration

        self._circuit_broken_until: float = 0.0
        self.cooldown_seconds = cooldown_seconds

    async def report_429(self, monitor: ProgressMonitor, retry_after: float | None = None) -> None:
        """Throttles or Breaks the circuit."""
        async with self._lock:
            now = time.time()
            self._last_429_time = now

            break_duration = retry_after if retry_after is not None else self.default_break

            # If we are already at MIN speed and still getting 429s -> BREAK CIRCUIT
            if self.current_rps <= self.min_rps or retry_after is not None:
                self._circuit_broken_until = now + break_duration
                await monitor.log(
                    f"!!! CIRCUIT BREAKER !!!"
                    f"Server requested {break_duration:.0f}s pause. All workers sleeping...",
                    status="WARNING",
                )
                return

            # Otherwise, just scale down
            new_rps = max(self.min_rps, int(self.current_rps * 0.5))
            if new_rps < self.current_rps:
                self.current_rps = new_rps
                self._limiter = AsyncLimiter(new_rps, 1)
                await monitor.log(f"429 detected. Throttling to {new_rps} RPS", status="WARNING")

    async def try_scale_up(self) -> bool:
        """
        Attempts to increase RPS.
        Returns True if increased, False if in cooldown or at max.
        """
        async with self._lock:
            # 1. Check if we are still in the "penalty box"
            if time.time() - self._last_429_time < self.cooldown_seconds:
                return False

            # 2. Check if we are already at the ceiling
            if self.current_rps >= self.max_rps:
                return False

            # 3. Scale up safely
            self.current_rps += 1
            self._limiter = AsyncLimiter(self.current_rps, 1)
            return True

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        while True:
            now = time.time()
            if now < self._circuit_broken_until:
                wait_time = self._circuit_broken_until - now
                await asyncio.sleep(wait_time)
                continue
            break

        async with self._lock:
            current_limiter = self._limiter

        await current_limiter.__aenter__()
        try:
            yield
        finally:
            await current_limiter.__aexit__(None, None, None)


class NetworkClient:
    """
    Asynchronous HTTP client wrapper providing rate limiting, connection pooling,
    and robust error handling with exponential backoff and full jitter.
    """

    def __init__(
        self,
        threads: int,
        monitor: ProgressMonitor,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:

        self.monitor = monitor
        self.rate_limiter = DynamicRateLimiter(threads * 2)

        options = cast(dict[str, Any], {**DEFAULT_OPTIONS, **(client_kwargs or {})})

        user_headers = options.pop("headers", None)
        headers_obj = httpx.Headers(user_headers)
        headers_obj.setdefault("Accept-Encoding", "identity")
        headers_obj.setdefault("User-Agent", "HydraStream/1.0")

        calc_limits = httpx.Limits(
            max_connections=math.ceil(threads * 1.1), max_keepalive_connections=threads, keepalive_expiry=4.5
        )
        final_limits = options.pop("limits", calc_limits)

        self.client = httpx.AsyncClient(headers=headers_obj, limits=final_limits, **options)

    async def _evaluate_failure(
        self, url: str, attempt: int, response: httpx.Response | None, exc: Exception | None
    ) -> float | None:

        retry_codes = {408, 429, 500, 502, 503, 504}

        if response is not None:
            if response.status_code not in retry_codes:
                await self.monitor.log(f"Fatal HTTP error {response.status_code} for {url}", status="ERROR")
                return None

            server_delay = self._get_retry_after(response)
            if response.status_code == 429:
                await self.rate_limiter.report_429(self.monitor, server_delay)

            delay = server_delay if server_delay is not None else random.uniform(0, 2**attempt)
            await self.monitor.log(
                f"Attempt {attempt} failed ({response.status_code}) for {url}. Retrying in {delay:.2f}s...",
                status="WARNING",
            )
            return delay

        if exc is not None:
            if isinstance(exc, (httpx.RequestError, TimeoutError, asyncio.TimeoutError)):
                delay = random.uniform(0, 2**attempt)
                await self.monitor.log(
                    f"Network issue ({type(exc).__name__}) on {url}. Retrying in {delay:.2f}s...",
                    status="WARNING",
                )
                return delay

            await self.monitor.log(f"Unrecoverable request error for {url}: {exc}", status="ERROR")
            return None

        return None

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
        for attempt in range(1, 4):
            async with self.rate_limiter.acquire():
                try:
                    resp = await self.client.request(method, url, **kwargs)
                    if resp.status_code < 400:
                        if random.random() < 0.1:
                            await self.rate_limiter.try_scale_up()
                        return resp

                    delay = await self._evaluate_failure(url, attempt, response=resp, exc=None)
                except Exception as exc:
                    delay = await self._evaluate_failure(url, attempt, response=None, exc=exc)

            if delay is None:
                break
            await asyncio.sleep(delay)

        return None

    @contextlib.asynccontextmanager
    async def stream_chunk(
        self, url: str, headers: dict[str, str], chunk_timeout: int
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

        for attempt in range(1, 4):
            response = None
            async with self.rate_limiter.acquire():
                try:
                    async with asyncio.timeout(chunk_timeout):
                        async with self.client.stream("GET", url, headers=headers) as response:
                            if response.status_code < 400:
                                yield response
                                return

                            delay = await self._evaluate_failure(url, attempt, response=response, exc=None)

                except Exception as exc:
                    delay = await self._evaluate_failure(url, attempt, response=None, exc=exc)

            if delay is None:
                if response is not None:
                    raise httpx.HTTPStatusError(
                        f"Stream failed on {url}", request=response.request, response=response
                    )
                else:
                    raise RuntimeError(f"Stream failed on {url} before response was received")

            await asyncio.sleep(delay)

        raise Exception(f"Failed to establish stream for {url} after 3 attempts.")

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
        except (ValueError, TypeError):
            pass
        return None

    def extract_filename(self, url: str, headers: httpx.Headers) -> str:
        """
        Extracts and cleans a filename from Content-Disposition or URL.
        Handles Cyrillic, special chars, and missing extensions.
        """
        filename = None
        cd = headers.get("Content-Disposition", "")

        # 1. Try RFC 5987 (Modern standard for UTF-8 names: filename*=UTF-8''...)
        # Example: filename*=UTF-8''%D1%84%D0%B0%D0%B9%D0%BB.txt
        match_utf8 = re.search(r"filename\*=\s*([^']+)''([^;]+)", cd)
        if match_utf8:
            filename = unquote(match_utf8.group(2))

        # 2. Try Standard filename (filename="name.txt")
        if not filename:
            match_std = re.search(r'filename="?([^";]+)"?', cd)
            if match_std:
                filename = unquote(match_std.group(1))

        # 3. Fallback to URL (strip params and anchors)
        if not filename:
            # Remove ?query=... and #anchor
            clean_url = url.split("?")[0].split("#")[0].rstrip("/")
            filename = unquote(clean_url.rsplit("/", 1)[-1])

        # 4. Final Safety Net Name
        if not filename or filename in [".", ""]:
            filename = "downloaded_file"

        # 5. Sanitize (Remove forbidden OS characters: / \ : * ? " < > |)
        # Replaces them with an underscore
        filename = re.sub(r'[\\/*?:"<>|]', "_", filename)

        # 6. Logic for Missing Extension (via Content-Type)
        if "." not in filename:
            content_type = headers.get("Content-Type", "").split(";")[0]
            ext = mimetypes.guess_extension(content_type)
            if ext:
                filename += ext
            elif not filename.endswith(".bin"):
                filename += ".bin"

        return filename

    async def close(self) -> None:
        """Safely closes the underlying HTTP connection pool."""
        await self.client.aclose()
