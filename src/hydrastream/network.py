# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import email.utils
import mimetypes
import random
import re
import time
import typing
from collections.abc import AsyncIterator
from typing import TypedDict, Unpack
from urllib.parse import unquote

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

from hydrastream.models import AMIDState, NetworkState
from hydrastream.monitor import log


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


async def report_429(ctx: AMIDState, retry_after: float | None = None) -> None:
    """Throttles or Breaks the circuit."""
    async with ctx.lock:
        now = time.time()
        ctx.last_429_time = now

        break_duration = retry_after if retry_after is not None else ctx.initial_rps

        # If we are already at MIN speed and still getting 429s -> BREAK CIRCUIT
        if ctx.current_rps <= ctx.min_rps or retry_after is not None:
            ctx.circuit_broken_until = now + break_duration
            await log(
                ctx.monitor,
                f"!!! CIRCUIT BREAKER !!!"
                f"Server requested {break_duration:.0f}s pause. "
                f"All workers sleeping...",
                status="WARNING",
            )
            return

        # Otherwise, just scale down
        new_rps = max(ctx.min_rps, int(ctx.current_rps * 0.5))
        if new_rps < ctx.current_rps:
            ctx.current_rps = new_rps
            ctx.limiter = AsyncLimiter(new_rps, 1)
            await log(
                ctx.monitor,
                f"429 detected. Throttling to {new_rps} RPS",
                status="WARNING",
            )


async def try_scale_up(ctx: AMIDState) -> bool:
    """
    Attempts to increase RPS.
    Returns True if increased, False if in cooldown or at max.
    """
    async with ctx.lock:
        # 1. Check if we are still in the "penalty box"
        if time.time() - ctx.last_429_time < ctx.cooldown_seconds:
            return False

        # 2. Check if we are already at the ceiling
        if ctx.current_rps >= ctx.max_rps:
            return False

        # 3. Scale up safely
        ctx.current_rps += 1
        ctx.limiter = AsyncLimiter(ctx.current_rps, 1)
        return True


@contextlib.asynccontextmanager
async def acquire(
    ctx: AMIDState,
) -> AsyncIterator[None]:
    while True:
        now = time.time()
        if now < ctx.circuit_broken_until:
            wait_time = ctx.circuit_broken_until - now
            await asyncio.sleep(wait_time)
            continue
        break

    async with ctx.lock:
        current_limiter = ctx.limiter

    # Элегантный и безопасный способ!
    async with current_limiter:
        yield


async def _evaluate_failure(
    ctx: NetworkState,
    url: str,
    attempt: int,
    response: httpx.Response | None,
    exc: Exception | None,
) -> float | None:
    retry_codes = {408, 429, 500, 502, 503, 504}

    if response is not None:
        if response.status_code not in retry_codes:
            await log(
                ctx.monitor,
                f"Fatal HTTP error {response.status_code} for {url}",
                status="ERROR",
            )
            return None

        server_delay = _get_retry_after(response)
        if response.status_code == 429:
            await report_429(ctx.rate_limiter, server_delay)

        delay = (
            server_delay if server_delay is not None else random.uniform(0, 2**attempt)
        )
        await log(
            ctx.monitor,
            f"Attempt {attempt} failed ({response.status_code}) for {url}. "
            f"Retrying in {delay:.2f}s...",
            status="WARNING",
            throttle_key="net_slow",
        )
        return delay

    if exc is not None:
        if isinstance(exc, httpx.RequestError | TimeoutError | asyncio.TimeoutError):
            delay = random.uniform(0, 2**attempt)
            await log(
                ctx.monitor,
                f"Network issue ({type(exc).__name__}) on {url}. "
                f"Retrying in {delay:.2f}s...",
                status="WARNING",
                throttle_key="net_drop",
            )
            return delay

        await log(
            ctx.monitor, f"Unrecoverable request error for {url}: {exc}", status="ERROR"
        )
        return None

    return None


async def safe_request(
    ctx: NetworkState, method: str, url: str, **kwargs: Unpack[RequestOptions]
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
    for attempt in range(1, ctx.max_retries + 1):
        async with acquire(ctx.rate_limiter):
            try:
                resp = await ctx.client.request(method, url, **kwargs)
                if resp.status_code < 400:
                    if random.random() < 0.1:
                        await try_scale_up(ctx.rate_limiter)
                    return resp

                delay = await _evaluate_failure(
                    ctx, url, attempt, response=resp, exc=None
                )
            except Exception as exc:
                delay = await _evaluate_failure(
                    ctx, url, attempt, response=None, exc=exc
                )

        if delay is None:
            return None
        await asyncio.sleep(delay)

    return None


@contextlib.asynccontextmanager
async def stream_chunk(
    ctx: NetworkState, url: str, headers: dict[str, str], chunk_timeout: int
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

    for attempt in range(1, ctx.max_retries + 1):
        response = None
        yielded = False
        async with acquire(ctx.rate_limiter):
            try:
                async with asyncio.timeout(chunk_timeout):
                    async with ctx.client.stream(
                        "GET", url, headers=headers
                    ) as response:
                        if response.status_code < 400:
                            if random.random() < 0.1:
                                await try_scale_up(ctx.rate_limiter)
                            yielded = True
                            yield response
                            return

                        delay = await _evaluate_failure(
                            ctx, url, attempt, response=response, exc=None
                        )

            except Exception as exc:
                if yielded:
                    raise

                delay = await _evaluate_failure(
                    ctx, url, attempt, response=None, exc=exc
                )

        if delay is None:
            if response is not None:
                raise httpx.HTTPStatusError(
                    f"Stream failed on {url}",
                    request=response.request,
                    response=response,
                )
            else:
                raise httpx.RequestError(
                    f"Stream failed on {url} before response was received"
                )

        await asyncio.sleep(delay)

    raise httpx.RequestError(f"Failed to establish stream for {url} after 3 attempts.")


def _get_retry_after(response: httpx.Response) -> float | None:
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


def extract_filename(url: str, headers: httpx.Headers) -> str:
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


async def close(ctx: NetworkState) -> None:
    """Safely closes the underlying HTTP connection pool."""
    await ctx.client.aclose()
