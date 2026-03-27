# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import os
import random
import sys
from typing import cast

from curl_cffi import Response
from curl_cffi.requests import RequestsError

from hydrastream.constants import STREAM_CHUNK_SIZE
from hydrastream.models import Chunk, HydraContext
from hydrastream.monitor import done, log, update
from hydrastream.network import stream_chunk
from hydrastream.storage import (
    delete_state,
    open_file,
    verify_file_hash,
    verify_size,
    write_chunk_data,
)


async def file_done(ctx: HydraContext, chunk: Chunk) -> None:
    if ctx.stream:
        return

    filename = chunk.filename
    file_obj = ctx.files[filename]
    if not await verify_file_hash(ctx.fs, file_obj):
        return
    file_obj.close_fd()
    verify_size(ctx.fs, file_obj)
    delete_state(ctx.fs, filename)
    await done(ctx.ui, filename)
    del ctx.files[filename]

    if not ctx.files:
        async with ctx.condition:
            ctx.condition.notify_all()


async def get_chunk(ctx: HydraContext) -> Chunk | None:
    _, chunk = await ctx.chunk_queue.get()
    file_obj = ctx.files.get(chunk.filename)
    if not file_obj or file_obj.is_failed:
        return None

    if ctx.stream:
        if ctx.current_file != chunk.filename:
            async with ctx.condition:
                await ctx.condition.wait_for(
                    lambda c=chunk.filename: ctx.current_file == c or not ctx.is_running
                )
        max_ahead_bytes = ctx.heap_size * STREAM_CHUNK_SIZE

        # Если чанк из "слишком далекого будущего" - ждем!
        if chunk.current_pos > ctx.next_offset + max_ahead_bytes:
            async with ctx.condition:
                await ctx.condition.wait_for(
                    lambda: (
                        (chunk.current_pos <= ctx.next_offset + max_ahead_bytes)
                        or not ctx.is_running
                    )
                )
    return chunk


async def download_worker(ctx: HydraContext) -> None:
    while ctx.is_running:
        chunk = None
        try:
            chunk = await get_chunk(ctx)
            if chunk is None:
                continue
            await process_chunk(ctx, chunk)
            await file_done(ctx, chunk)

        except asyncio.CancelledError:
            break

        except RequestsError as e:
            response = e.response  # type: ignore

            if isinstance(response, Response):
                # Теперь Pyright ВИДИТ, что это Response, и даст автодополнение
                status = response.status_code

                if chunk and status in {400, 401, 403, 404, 410, 416}:
                    await log(
                        ctx.ui,
                        f"Chunk for {chunk.filename} failed permanently "
                        f"(HTTP {status}).",
                        status="ERROR",
                    )
                    ctx.files[chunk.filename].is_failed = True

                # Остальные ошибки сервера (5xx, 429) — пробуем перекинуть чанк
                # в очередь
                else:
                    await requeue_chunk(ctx, chunk, delay_range=(0.5, 2.0))
            # 2. Если ответа нет (Сетевая ошибка / CurlError / Timeout)
            else:
                await requeue_chunk(ctx, chunk)

        except TimeoutError:
            await requeue_chunk(ctx, chunk)

        except Exception as e:
            await log(ctx.ui, f"Critical Worker Exception: {e!r}", status="CRITICAL")
            raise
        finally:
            ctx.chunk_queue.task_done()


async def requeue_chunk(
    ctx: HydraContext,
    chunk: Chunk | None,
    delay_range: tuple[float, float] = (1.0, 3.0),
) -> None:
    if not (ctx.is_running and chunk):
        return

    file_obj = ctx.files[chunk.filename]
    supports_ranges = file_obj.meta.supports_ranges

    if not supports_ranges:
        if ctx.stream:
            err_msg = (
                f"Stream interrupted for {chunk.filename}. "
                f"Server does not support partial downloads (Range requests). "
                f"Cannot resume stream. Aborting."
            )
            await log(ctx.ui, err_msg, status="CRITICAL")

            file_obj.is_failed = True
            return

        await log(
            ctx.ui,
            f"Connection dropped for {chunk.filename}. "
            f"Server does not support resume. Restarting download from 0 bytes.",
            status="WARNING",
        )

        downloaded_so_far = chunk.current_pos - chunk.start
        if downloaded_so_far > 0:
            update(ctx.ui, chunk.filename, -downloaded_so_far)

        chunk.current_pos = chunk.start

        fd = file_obj.fd
        if fd is not None:
            loop = asyncio.get_running_loop()
            # truncate(0) обрезает файл до 0 байт
            await loop.run_in_executor(None, os.ftruncate, fd, 0)

            # Если изначально размер был известен, снова выделяем место
            if file_obj.meta.content_length > 0:
                await loop.run_in_executor(
                    None, os.ftruncate, fd, file_obj.meta.content_length
                )
    await ctx.chunk_queue.put((-1, chunk))
    delay = random.uniform(*delay_range)
    await asyncio.sleep(delay)


async def disk_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str] | None
) -> None:
    buffer = bytearray()
    fd = ctx.files[chunk.filename].fd
    if fd is None:
        fd = open_file(ctx.fs, chunk.filename)
    buffer_size = 1_048_576
    chunk_timeout = ctx.config.chunk_timeout if headers else sys.maxsize
    async with stream_chunk(
        ctx.net,
        ctx.files[chunk.filename].meta.url,
        headers=headers,
        chunk_timeout=chunk_timeout,
    ) as r:
        try:
            async for data in r.aiter_content(chunk_size=131072):  # type: ignore
                data = cast(bytes, data)
                buffer.extend(data)
                update(ctx.ui, chunk.filename, len(data))
                if len(buffer) >= buffer_size:
                    await write_chunk_data(fd, buffer, chunk.current_pos)
                    chunk.current_pos += len(buffer)
                    buffer = bytearray()

        finally:
            if buffer:
                await write_chunk_data(fd, buffer, chunk.current_pos)
                chunk.current_pos += len(buffer)


async def stream_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str] | None
) -> None:
    buffer = bytearray()
    chunk_timeout = ctx.config.chunk_timeout if headers else sys.maxsize
    async with stream_chunk(
        ctx.net,
        ctx.files[chunk.filename].meta.url,
        chunk_timeout=chunk_timeout,
        headers=headers,
    ) as r:
        try:
            async for data in r.aiter_content(chunk_size=131072):  # type: ignore
                data = cast(bytes, data)
                buffer.extend(data)
                update(ctx.ui, chunk.filename, len(data))
                if len(ctx.heap) >= ctx.heap_size and chunk.start < ctx.next_offset:
                    async with ctx.condition:
                        await ctx.condition.wait_for(
                            lambda: len(ctx.heap) < ctx.heap_size
                        )
                if len(buffer) > STREAM_CHUNK_SIZE:
                    await ctx.stream_queue.put((chunk.current_pos, buffer))
                    chunk.current_pos = chunk.current_pos + len(buffer)
                    buffer = bytearray()
            await ctx.stream_queue.put((chunk.current_pos, buffer))
            chunk.current_pos = chunk.current_pos + len(buffer)
            buffer = bytearray()
        except asyncio.CancelledError:
            raise
        except Exception:
            if ctx.stream and buffer:
                with contextlib.suppress(asyncio.QueueFull):
                    ctx.stream_queue.put_nowait((chunk.current_pos, buffer))
                    chunk.current_pos = chunk.current_pos + len(buffer)
            raise


async def process_chunk(ctx: HydraContext, chunk: Chunk) -> None:
    if chunk.current_pos > chunk.end:
        return
    if ctx.files[chunk.filename].meta.supports_ranges:
        headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
    else:
        headers = None
    if not ctx.stream:
        await disk_process_chunk(ctx, chunk, headers)
    else:
        await stream_process_chunk(ctx, chunk, headers)
