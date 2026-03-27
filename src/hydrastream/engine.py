# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import hashlib
import heapq
import math
import random
import signal
from collections.abc import AsyncGenerator, Iterable

from hydrastream.dispatcher import download_worker
from hydrastream.models import HydraContext, TypeHash
from hydrastream.monitor import done, log, ui_start, ui_stop
from hydrastream.network import close
from hydrastream.producer import chunk_producer
from hydrastream.storage import autosave, save_all_states, verify_stream


async def delayed_worker(ctx: HydraContext, delay: float) -> None:
    await asyncio.sleep(delay)
    await download_worker(ctx)


async def stop(ctx: HydraContext, complete: bool = False) -> None:
    if not ctx.is_running:
        return

    ctx.is_running = False

    if not complete:
        await log(
            ctx.ui,
            "Interrupt signal received. Initiating graceful shutdown...",
            status="INTERRUPT",
        )

    if ctx.task_creator:
        ctx.task_creator.cancel()
    if ctx.autosave_task:
        ctx.autosave_task.cancel()

    await close(ctx.net)
    if ctx.workers:
        for worker in ctx.workers:
            if worker and not worker.done():
                worker.cancel()

    if ctx.stream:
        with contextlib.suppress(asyncio.QueueFull):
            ctx.stream_queue.put_nowait((-1, bytearray()))

    async with ctx.condition:
        ctx.condition.notify_all()


async def teardown_engine(ctx: HydraContext, loop: asyncio.AbstractEventLoop) -> None:
    """Универсальная глушилка завода. Защищает от копипасты."""
    if (
        ctx.is_running
        and ctx.ui.total_files > 0
        and ctx.ui.total_files == ctx.ui.files_completed
    ):
        await log(
            ctx.ui,
            "All downloads completed successfully!",
            status="SUCCESS",
            progress=True,
        )
    await stop(ctx, complete=True)
    # Гасим задачи
    tasks_to_cancel = [
        t
        for t in [ctx.task_creator, ctx.autosave_task, *(ctx.workers or [])]
        if (t and asyncio.iscoroutine(t)) or isinstance(t, asyncio.Task)
    ]
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    # Проверка на успех

    # Закрываем ресурсы
    if not ctx.stream:
        save_all_states(ctx.fs, ctx.files)
        for file_obj in ctx.files.values():
            file_obj.close_fd()

    await ui_stop(ctx.ui)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)


async def _stream_one(ctx: HydraContext, filename: str) -> AsyncGenerator[bytes]:
    ctx.current_file = filename

    async with ctx.condition:
        ctx.condition.notify_all()

    file_obj = ctx.files[filename]
    total_size = file_obj.meta.content_length

    checksum = file_obj.meta.expected_checksum
    hasher = hashlib.new(checksum.algorithm) if checksum else None

    ctx.next_offset = 0
    await log(ctx.ui, f"Streaming: {filename}", status="INFO")
    try:
        while ctx.next_offset < total_size:
            if not ctx.is_running:
                break

            if ctx.heap and ctx.heap[0][0] == ctx.next_offset:
                _, chunk_data = heapq.heappop(ctx.heap)
                chunk_bytes = bytes(chunk_data)

                async with ctx.condition:
                    ctx.condition.notify_all()

                if hasher:
                    hasher.update(chunk_bytes)

                yield chunk_bytes

                length = len(chunk_bytes)
                ctx.next_offset += length
                continue

            chunk_start, chunk_data = await ctx.stream_queue.get()
            if chunk_start == -1:
                break

            if chunk_start == ctx.next_offset:
                chunk_bytes = bytes(chunk_data)

                if hasher:
                    hasher.update(chunk_bytes)

                yield chunk_bytes

                length = len(chunk_bytes)
                ctx.next_offset += length

            else:
                heapq.heappush(ctx.heap, (chunk_start, chunk_data))
        else:
            await done(ctx.ui, filename)

            if hasher and checksum:
                try:
                    verify_stream(hasher, checksum.value, ctx.next_offset, total_size)
                    await log(ctx.ui, "Hash Verified", status="SUCCESS", progress=True)
                except Exception as e:
                    await log(ctx.ui, str(e), status="ERROR")
                    raise

    finally:
        ctx.heap.clear()
        del ctx.files[filename]


async def stream_all(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str]] | None,
) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
    await ui_start(ctx.ui)
    ctx.stream = True
    links = [links] if isinstance(links, str) else list(links)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(stop(ctx)))

    ctx.task_creator = asyncio.create_task(
        chunk_producer(ctx, links, expected_checksums), name="Task creator"
    )
    ctx.workers = [
        asyncio.create_task(
            delayed_worker(ctx, random.uniform(0, 0.5)), name=f"Worker: {i}"
        )
        for i in range(ctx.config.threads)
    ]

    try:
        for _ in links:
            if not ctx.is_running:
                break

            filename = await ctx.file_discovery_queue.get()

            if filename is None:
                continue

            file_gen = _stream_one(ctx, filename)

            yield filename, file_gen
            async with ctx.condition:
                await ctx.condition.wait_for(
                    lambda f=filename: f not in ctx.files or not ctx.is_running
                )

    except asyncio.CancelledError:
        pass

    except Exception as e:
        await log(ctx.ui, f"Runtime Exception in run(): {e}", status="CRITICAL")
        raise

    finally:
        await teardown_engine(ctx, loop)


async def run_downloads(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str]] | None,
) -> None:
    await ui_start(ctx.ui)
    ctx.stream = False

    links = [links] if isinstance(links, str) else list(links)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(stop(ctx)))
    ctx.task_creator = asyncio.create_task(
        chunk_producer(ctx, links, expected_checksums), name="Task creator"
    )
    ctx.autosave_task = asyncio.create_task(
        autosave(ctx, interval=60), name="Autosaver"
    )
    num_workers = (
        math.ceil(ctx.config.threads * 1.2)
        if ctx.config.threads > 1
        else ctx.config.threads
    )
    ctx.workers = [
        asyncio.create_task(
            delayed_worker(ctx, random.uniform(0, 0.5)), name=f"Worker: {i}"
        )
        for i in range(num_workers)
    ]

    try:
        await ctx.task_creator
        async with ctx.condition:
            await ctx.condition.wait_for(lambda: not (ctx.files and ctx.is_running))
    except asyncio.CancelledError:
        pass

    except Exception as e:
        await log(ctx.ui, f"Runtime Exception in run(): {e}", status="CRITICAL")
        raise

    finally:
        await teardown_engine(ctx, loop)
