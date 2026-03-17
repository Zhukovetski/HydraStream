import asyncio
import contextlib
import random

import httpx

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


async def run_dispatch_loop(ctx: HydraContext) -> None:
    """
    Background worker that consumes chunks from the queue, downloads data,
    and triggers integrity checks upon file completion.
    """
    while ctx.is_running:
        chunk = None
        try:
            _, chunk = await ctx.chunk_queue.get()
            file_obj = ctx.files.get(chunk.filename)
            if not file_obj or file_obj.is_failed:
                ctx.chunk_queue.task_done()
                continue

            # Stream backpressure: wait for the current file to be completely consumed
            if ctx.stream and ctx.current_file != chunk.filename:
                async with ctx.condition:
                    await ctx.condition.wait_for(
                        lambda c=chunk.filename: (
                            ctx.current_file == c or not ctx.is_running
                        )
                    )

            await _process_chunk(ctx, chunk)

            # Integrity check trigger for disk mode
            if not ctx.stream:
                filename = chunk.filename
                file_obj = ctx.files.get(filename)

                if file_obj and file_obj.is_complete:
                    if file_obj.meta.expected_md5 and not file_obj.verified:
                        file_obj.verified = True
                        await log(
                            ctx.ui,
                            f"Verifying MD5 checksum for {filename}...",
                            status="INFO",
                        )
                        try:
                            loop = asyncio.get_running_loop()
                            await loop.run_in_executor(
                                None, verify_file_hash, ctx.fs, file_obj
                            )
                            await log(
                                ctx.ui,
                                f"Integrity confirmed: {filename}",
                                status="SUCCESS",
                            )
                        except ValueError as ve:
                            await log(ctx.ui, str(ve), status="ERROR")

                    file_obj.close_fd()
                    verify_size(ctx.fs, file_obj)
                    delete_state(ctx.fs, filename)
                    await done(ctx.ui, filename)
                    ctx.chunk_queue.task_done()
                    del ctx.files[filename]

                    if not ctx.files:
                        async with ctx.condition:
                            ctx.condition.notify_all()
            # ------------------------------------
        except asyncio.CancelledError:
            break  # Worker shutdown requested

        except httpx.HTTPStatusError as e:
            if chunk:
                if e.response.status_code in {400, 401, 403, 404, 410, 416}:
                    await log(
                        ctx.ui,
                        f"Chunk for {chunk.filename} failed permanently "
                        f"(HTTP {e.response.status_code}).",
                        status="ERROR",
                    )
                    ctx.files[chunk.filename].is_failed = True
                else:
                    if ctx.is_running:
                        # Exponential or fixed backoff for chunk retry
                        await asyncio.sleep(random.uniform(0.5, 2.0))
                        # Re-queue with high priority (-1)
                        await ctx.chunk_queue.put((-1, chunk))
        except (httpx.RequestError, TimeoutError):
            if ctx.is_running and chunk:
                await asyncio.sleep(random.uniform(1.0, 3.0))
                await ctx.chunk_queue.put((-1, chunk))

        except Exception as e:
            await log(ctx.ui, f"Critical Worker Exception: {e!r}", status="CRITICAL")
            raise
        finally:
            ctx.chunk_queue.task_done()


async def _process_chunk(ctx: HydraContext, chunk: Chunk) -> None:
    if chunk.current_pos > chunk.end:
        return
    downloaded_in_this_attempt = 0
    headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
    buffer = bytearray()
    try:
        async with stream_chunk(
            ctx.net,
            ctx.files[chunk.filename].meta.url,
            headers=headers,
            chunk_timeout=ctx.config.chunk_timeout,
        ) as r:
            if not ctx.stream:
                fd = ctx.files[chunk.filename].fd
                if fd is None:
                    fd = open_file(ctx.fs, chunk.filename)
                buffer_size = 1024 * 1024
                try:
                    async for data in r.aiter_bytes():
                        downloaded_in_this_attempt += len(data)
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
            else:
                async for data in r.aiter_bytes():
                    downloaded_in_this_attempt += len(data)
                    buffer.extend(data)
                    update(ctx.ui, chunk.filename, len(data))
                if len(ctx.heap) >= ctx.heap_size:
                    async with ctx.condition:
                        await ctx.condition.wait_for(
                            lambda: len(ctx.heap) <= ctx.heap_size
                        )
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
