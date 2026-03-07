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

    async def scale_down(self) -> None:
        """Called when we hit 429"""
        async with self._lock:
            self._last_429_time = time.time()  # Start the clock
            new_rps = max(self.min_rps, int(self.current_rps * 0.5))

            if new_rps != self.current_rps:
                self.current_rps = new_rps
                # Re-initialize the internal limiter with new rate
                self._limiter = AsyncLimiter(new_rps, 1)

    async def report_429(self, monitor: ProgressMonitor, retry_after: float | None = None) -> None:
        """Throttles or Breaks the circuit."""
        async with self._lock:
            now = time.time()
            self._last_429_time = now

            break_duration = retry_after if retry_after is not None else self.default_break

            # If we are already at MIN speed and still getting 429s -> BREAK CIRCUIT
            if self.current_rps <= self.min_rps or retry_after is not None:
                self._circuit_broken_until = now + break_duration
                monitor.log(
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
                monitor.log(f"429 detected. Throttling to {new_rps} RPS", status="WARNING")

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
        self, threads: int, monitor: ProgressMonitor, timeout: float = 10.0, http2: bool = True
    ) -> None:
        self.monitor = monitor
        # +10% buffer for max_connections to prevent TIME_WAIT bottlenecks
        limits = httpx.Limits(max_connections=math.ceil(threads * 1.1), max_keepalive_connections=threads)

        self.rate_limiter = DynamicRateLimiter(threads * 2)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, read=5.0),
            limits=limits,
            http2=http2,
            follow_redirects=True,
            headers={"Accept-Encoding": "identity", "User-Agent": "NCBILoader/1.0"},
        )

    async def _evaluate_failure(
        self, url: str, attempt: int, response: httpx.Response | None, exc: Exception | None
    ) -> float | None:

        retry_codes = {408, 429, 500, 502, 503, 504}

        if response is not None:
            if response.status_code not in retry_codes:
                self.monitor.log(f"Fatal HTTP error {response.status_code} for {url}", status="ERROR")
                return None

            server_delay = self._get_retry_after(response)
            if response.status_code == 429:
                await self.rate_limiter.report_429(self.monitor, server_delay)

            delay = server_delay if server_delay is not None else random.uniform(0, 2**attempt)
            self.monitor.log(
                f"Attempt {attempt} failed ({response.status_code}) for {url}. Retrying in {delay:.2f}s...",
                status="WARNING",
            )
            return delay

        if exc is not None:
            if isinstance(exc, (httpx.ConnectError, httpx.TimeoutException)):
                delay = random.uniform(0, 2**attempt)
                self.monitor.log(
                    f"Network issue ({type(exc).__name__}) on {url}. Retrying in {delay:.2f}s...",
                    status="WARNING",
                )
                return delay

            self.monitor.log(f"Unrecoverable request error for {url}: {exc}", status="ERROR")
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
        for attempt in range(1, 4):
            response = None
            async with self.rate_limiter.acquire():
                try:
                    async with asyncio.timeout(timeout):
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
        except ValueError, TypeError:
            pass
        return None

    async def close(self) -> None:
        """Safely closes the underlying HTTP connection pool."""
        await self.client.aclose()
