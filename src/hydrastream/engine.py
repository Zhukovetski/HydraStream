# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import hashlib
import heapq
import math
import random
import signal
from _hashlib import HASH
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from typing import TypeVarTuple, Unpack

from .dispatcher import download_worker
from .models import Checksum, File, HydraContext, TypeHash
from .monitor import done, log, ui_start, ui_stop
from .network import close
from .producer import chunk_producer, dispatch_chunks

Ts = TypeVarTuple("Ts")


async def delayed_task(
    ctx: HydraContext,
    task: Callable[[HydraContext, Unpack[Ts]], Awaitable[None]],
    *args: Unpack[Ts],
    delay: float = random.uniform(0, 0.5),
) -> None:
    await asyncio.sleep(delay)
    await task(ctx, *args)


async def autosave(ctx: HydraContext, interval: float) -> None:
    loop = asyncio.get_running_loop()
    while ctx.is_running:
        try:
            await asyncio.sleep(interval)
            await loop.run_in_executor(None, save_all_states, ctx, ctx.files)
        except asyncio.CancelledError:
            break
        except Exception as e:
            await log(ctx.ui, f"Auto-save operation failed: {e}", status="ERROR")


async def cancel_tasks(ctx: HydraContext) -> None:

    for task in ctx.task_creators or []:
        if not task.done():
            task.cancel()

    if ctx.autosave_task:
        ctx.autosave_task.cancel()

    if ctx.dispatcher:
        ctx.dispatcher.cancel()

    await close(ctx.net)
    for worker in ctx.workers or []:
        if not worker.done():
            worker.cancel()


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
    await cancel_tasks(ctx)

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
        for t in [
            *(ctx.task_creators or []),  # Распаковываем список продюсеров
            ctx.autosave_task,  # Единичный таск
            *(ctx.workers or []),  # Распаковываем список воркеров
            ctx.dispatcher,
        ]
        if isinstance(t, asyncio.Task)  # iscoroutine убираем, мы создаем только Tasks!
    ]
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    # Проверка на успех

    # Закрываем ресурсы
    if not ctx.stream:
        save_all_states(ctx, ctx.files)
        for file_obj in ctx.files.values():
            ctx.fs.close_file(file_obj.fd)

    await ui_stop(ctx.ui)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)


async def _stream_one(ctx: HydraContext, filename: str) -> AsyncGenerator[bytes]:
    id = ctx.current_file_id[0]
    file_obj = ctx.files[id]
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
        del ctx.files[id]
        ctx.current_file_id.remove(id)


async def create_tasks(
    ctx: HydraContext,
    links: list[str],
) -> None:
    num_task_creator = math.ceil(len(links) ** 0.4) if len(links) > 1 else 1
    num_task_creator = min(num_task_creator, 20)
    ctx.task_creators = [
        asyncio.create_task(
            delayed_task(ctx, chunk_producer),
            name=f"Task creator: {i}",
        )
        for i in range(num_task_creator)
    ]

    if ctx.stream:
        num_workers = ctx.config.threads
    else:
        num_workers = (
            math.ceil(ctx.config.threads * 1.2)
            if ctx.config.threads > 1
            else ctx.config.threads
        )

        ctx.autosave_task = asyncio.create_task(
            autosave(ctx, interval=60), name="Autosaver"
        )
    if ctx.task_creators:
        for task in ctx.task_creators:
            await task

    ctx.dispatcher = asyncio.create_task(dispatch_chunks(ctx), name="Dispather")

    ctx.workers = [
        asyncio.create_task(
            delayed_task(ctx, download_worker),
            name=f"Worker: {i}",
        )
        for i in range(num_workers)
    ]


async def dispatch_links(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str]] | None,
    loop: asyncio.AbstractEventLoop,
) -> None:

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(stop(ctx)))

    checksums = None
    for i, link in enumerate(links):
        if expected_checksums is not None:
            if checksums := expected_checksums.get(link):
                checksums = Checksum(algorithm=checksums[0], value=checksums[1])
        else:
            expected_checksums = None
        await ctx.links_queue.put((i, link, checksums))


async def stream_all(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str]] | None,
) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:

    await ui_start(ctx.ui)

    ctx.stream = True

    loop = asyncio.get_running_loop()

    links = [links] if isinstance(links, str) else list(links)

    await dispatch_links(ctx, links, expected_checksums, loop)

    await create_tasks(ctx, links)

    try:
        while ctx.files and ctx.is_running:
            if not ctx.is_running:
                break
            id = ctx.current_file_id[0]
            filename = ctx.files[id].meta.filename
            file_gen = _stream_one(ctx, filename)

            yield filename, file_gen
            async with ctx.condition:
                await ctx.condition.wait_for(
                    lambda id=id: id not in ctx.files or not ctx.is_running
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

    loop = asyncio.get_running_loop()

    links = [links] if isinstance(links, str) else list(links)

    await dispatch_links(ctx, links, expected_checksums, loop)

    await create_tasks(ctx, links)

    try:
        async with ctx.condition:
            await ctx.condition.wait_for(lambda: not (ctx.files and ctx.is_running))
    except asyncio.CancelledError:
        pass

    except Exception as e:
        await log(ctx.ui, f"Runtime Exception in run(): {e}", status="CRITICAL")
        raise

    finally:
        await teardown_engine(ctx, loop)


def save_all_states(ctx: HydraContext, files: dict[int, File]) -> None:
    for file in list(files.values()):
        if file.chunks and not all(c.current_pos > c.end for c in (file.chunks or [])):
            ctx.fs.save_state(file)


def verify_stream(
    md5_hasher: HASH, expected_checksum: str, next_offset: int, total_size: int
) -> None:
    calculated = md5_hasher.hexdigest()
    if calculated != expected_checksum:
        err_msg = (
            f"CRITICAL: Stream Integrity Check Failed!\n"
            f"Expected MD5: {expected_checksum}\n"
            f"Got MD5:      {calculated}"
        )
        raise ValueError(err_msg)

    if next_offset != total_size:
        raise ValueError(
            f"Incomplete stream data! Yielded {next_offset} bytes,"
            f" but expected {total_size} bytes."
        )
