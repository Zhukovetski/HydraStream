# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import email.utils
import mimetypes
import random
import re
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Unpack
from urllib.parse import unquote

from aiolimiter import AsyncLimiter
from curl_cffi import CurlError, Headers, Response
from curl_cffi.requests import RequestsError
from curl_cffi.requests.session import HttpMethod, RequestParams

from .models import AMIDState, NetworkState
from .monitor import log


async def report_429(ctx: AMIDState, retry_after: float | None = None) -> None:
    async with ctx.lock:
        now = time.time()
        ctx.last_429_time = now

        break_duration = retry_after if retry_after is not None else ctx.initial_rps

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
    async with ctx.lock:
        if time.time() - ctx.last_429_time < ctx.cooldown_seconds:
            return False

        if ctx.current_rps >= ctx.max_rps:
            return False

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

    async with current_limiter:
        yield


async def _evaluate_failure(
    ctx: NetworkState,
    url: str,
    attempt: int,
    response: Response | None,
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
        if isinstance(
            exc, RequestsError | CurlError | TimeoutError | asyncio.TimeoutError
        ):
            err_name = type(exc).__name__
            if isinstance(exc, CurlError):
                err_name = f"CurlError({exc.code})"

            delay = random.uniform(0, 2**attempt)
            await log(
                ctx.monitor,
                f"Network issue ({err_name}) on {url}. Retrying in {delay:.2f}s...",
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
    ctx: NetworkState, method: HttpMethod, url: str, **kwargs: Unpack[RequestParams]
) -> Response | None:
    for attempt in range(1, ctx.max_retries + 1):
        response = None
        async with acquire(ctx.rate_limiter):
            try:
                resp = await ctx.client.request(method, url, **kwargs)  # type: ignore
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
            if response is not None:
                raise RequestsError(
                    f"Request failed on {url}",
                    request=response.request,
                    response=response,
                )
            raise RequestsError(f"Request failed on {url} before response was received")
        await asyncio.sleep(delay)

    raise RequestsError(
        f"Failed to establish request for {url} after {ctx.max_retries} attempts."
    )


@contextlib.asynccontextmanager
async def stream_chunk(
    ctx: NetworkState,
    url: str,
    headers: dict[str, str] | None = None,
) -> AsyncIterator[Response]:
    for attempt in range(1, ctx.max_retries + 1):
        response = None
        yielded = False
        async with acquire(ctx.rate_limiter):
            try:
                async with ctx.client.stream("GET", url, headers=headers) as response:
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
                raise RequestsError(
                    f"Stream failed on {url}",
                    request=response.request,
                    response=response,
                )
            raise RequestsError(f"Stream failed on {url} before response was received")
        await asyncio.sleep(delay)

    raise RequestsError(
        f"Failed to establish stream for {url} after {ctx.max_retries} attempts."
    )


def _get_retry_after(response: Response) -> float | None:
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


def extract_filename(url: str, headers: Headers) -> str:
    filename = None
    cd = headers.get("Content-Disposition", "")

    match_utf8 = re.search(r"filename\*=\s*([^']+)''([^;]+)", cd)
    if match_utf8:
        filename = unquote(match_utf8.group(2))

    if not filename:
        match_std = re.search(r'filename="?([^";]+)"?', cd)
        if match_std:
            filename = unquote(match_std.group(1))

    if not filename:
        clean_url = url.rstrip("/")
        clean_url = clean_url.split("?")[0].split("#")[0]
        clean_url, name = clean_url.rsplit("/", 1)
        if "/" in clean_url and not clean_url.endswith(":/"):
            filename = unquote(name)

    if not filename or filename in [".", ""]:
        filename = "downloaded_file"

    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    filename = Path(filename).name

    if "." not in filename:
        content_type = headers.get("Content-Type", "").split(";")[0]
        ext = mimetypes.guess_extension(content_type)
        if ext:
            filename += ext
        elif not filename.endswith(".bin"):
            filename += ".bin"

    return filename


async def close(ctx: NetworkState) -> None:
    if hasattr(ctx, "client"):
        with contextlib.suppress(TypeError, AttributeError):
            await ctx.client.close()
