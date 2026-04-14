# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import hashlib
import heapq
import math
import random
import signal
import sys
from _hashlib import HASH
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVarTuple, Unpack

from hydrastream.exceptions import FileSizeMismatchError, HashMismatchError, LogStatus

from .dispatcher import download_worker, telemetry_worker
from .models import Checksum, File, HydraContext, TypeHash
from .monitor import done, log, print_dry_run_report, ui_start, ui_stop
from .producer import chunk_producer, dispatch_chunks

Ts = TypeVarTuple("Ts")


async def delayed_task(
    ctx: HydraContext,
    task: Callable[[HydraContext, Unpack[Ts]], Awaitable[None]],
    *args: *Ts,
    delay: tuple[float, float] = (0, 0.3),
) -> None:
    await asyncio.sleep(random.uniform(*delay))
    await task(ctx, *args)


async def autosave(ctx: HydraContext, interval: float) -> None:
    loop = asyncio.get_running_loop()

    while not ctx.sync.all_complete.is_set():
        try:
            async with asyncio.timeout(interval):
                await ctx.sync.all_complete.wait()

            break
        except TimeoutError:
            try:
                await loop.run_in_executor(None, save_all_states, ctx, ctx.files)
            except Exception as e:
                if ctx.config.debug:
                    raise
                await log(
                    ctx.ui, f"Auto-save operation failed: {e}", status=LogStatus.ERROR
                )

        except asyncio.CancelledError:
            break


async def teardown_engine(ctx: HydraContext, loop: asyncio.AbstractEventLoop) -> None:
    if (
        not ctx.is_stopping
        and ctx.ui.progress.total_files > 0
        and ctx.ui.progress.total_files == ctx.ui.progress.files_completed
    ):
        await log(
            ctx.ui,
            "All downloads completed successfully!",
            status=LogStatus.SUCCESS,
            progress=True,
        )

    await stop(ctx, complete=True)

    if not ctx.stream:
        save_all_states(ctx, ctx.files)
        for file_obj in ctx.files.values():
            if file_obj.fd:
                ctx.fs.close_file(file_obj.fd)

    await ui_stop(ctx.ui)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)

    await loop.shutdown_default_executor()


async def stop(ctx: HydraContext, complete: bool = False) -> None:
    if ctx.is_stopping:
        return
    ctx.is_stopping = True

    if not complete:
        await log(
            ctx.ui,
            "Interrupt signal received. Initiating graceful shutdown...",
            status=LogStatus.INTERRUPT,
        )

    if ctx.stream:
        with contextlib.suppress(asyncio.QueueFull):
            ctx.queues.stream.put_nowait((-1, bytearray()))
            ctx.queues.file_discovery.put_nowait(-1)


async def _stream_one(ctx: HydraContext, file_id: int) -> AsyncGenerator[memoryview]:

    file_obj = ctx.files[file_id]
    total_size = file_obj.meta.content_length

    checksum = file_obj.meta.expected_checksum
    hasher = hashlib.new(checksum.algorithm) if checksum else None

    ctx.next_offset = 0
    await log(ctx.ui, f"Streaming: {file_obj.meta.filename}", status=LogStatus.INFO)
    try:
        while ctx.next_offset < total_size:
            if ctx.heap and ctx.heap[0][0] == ctx.next_offset:
                _, chunk_data = heapq.heappop(ctx.heap)
                view = memoryview(chunk_data)

                if hasher:
                    hasher.update(view)

                yield view

                length = len(view)
                ctx.next_offset += length

                async with ctx.sync.chunk_from_future:
                    ctx.sync.chunk_from_future.notify_all()

                continue

            chunk_start, chunk_data = await ctx.queues.stream.get()

            if chunk_start == -1:
                break

            if chunk_start == ctx.next_offset:
                view = memoryview(chunk_data)
                if hasher:
                    hasher.update(view)

                yield view

                length = len(view)
                ctx.next_offset += length
                async with ctx.sync.chunk_from_future:
                    ctx.sync.chunk_from_future.notify_all()

            else:
                heapq.heappush(ctx.heap, (chunk_start, chunk_data))
        else:
            await done(ctx.ui, file_obj.meta.filename)

            if hasher and checksum:
                try:
                    verify_stream(
                        hasher,
                        file_obj.meta.filename,
                        checksum,
                        ctx.next_offset,
                        total_size,
                    )
                    await log(
                        ctx.ui, "Hash Verified", status=LogStatus.SUCCESS, progress=True
                    )
                except Exception as e:
                    await log(ctx.ui, str(e), status=LogStatus.ERROR)
                    raise

    finally:
        ctx.heap.clear()
        del ctx.files[file_id]
        ctx.current_files_id.remove(file_id)
        async with ctx.sync.current_files:
            ctx.sync.current_files.notify()
        if not ctx.files and ctx.discovery_completed:
            await ctx.queues.file_discovery.put(-1)
            ctx.sync.all_complete.set()
            ctx.ui.speed.checkpoint_event.set()
            ctx.dynamic_limit = sys.maxsize
            async with ctx.sync.dynamic_limit:
                ctx.sync.dynamic_limit.notify_all()


async def create_tasks(
    ctx: HydraContext,
    tg: asyncio.TaskGroup,
    loop: asyncio.AbstractEventLoop,
    links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:

    optimal_threads = max(20, ctx.config.threads + 10)
    max_safe_threads = min(optimal_threads, 64)
    custom_pool = ThreadPoolExecutor(
        max_workers=max_safe_threads, thread_name_prefix="HydraIO"
    )
    loop.set_default_executor(custom_pool)

    await ui_start(ctx.ui)
    main_task = asyncio.current_task()

    def handle_signal() -> None:
        if main_task and not main_task.done():
            main_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    ctx.tasks.creators = math.ceil(len(links) ** 0.4) if len(links) > 1 else 1
    ctx.tasks.creators = min(ctx.tasks.creators, 20)

    if ctx.stream:
        ctx.tasks.workers = ctx.config.threads
    else:
        ctx.tasks.workers = (
            math.ceil(ctx.config.threads * 1.2)
            if ctx.config.threads > 1
            else ctx.config.threads
        )

    if not ctx.config.dry_run:
        for i in range(ctx.tasks.workers):
            tg.create_task(
                delayed_task(ctx, download_worker, i),
                name=f"Worker: {i}",
            )
    for i in range(ctx.tasks.creators):
        tg.create_task(
            delayed_task(ctx, chunk_producer),
            name=f"Task creator: {i}",
        )
    if not ctx.config.dry_run:
        if ctx.config.threads > 1:
            tg.create_task(telemetry_worker(ctx), name="Telemetry")
            ctx.tasks.telemetry = 1
        if not ctx.stream:
            tg.create_task(autosave(ctx, interval=60), name="Autosaver")
            ctx.tasks.autosave = 1

        tg.create_task(dispatch_chunks(ctx), name="Dispather")
        ctx.tasks.dispatcher = 1
    tg.create_task(dispatch_links(ctx, links, expected_checksums))
    ctx.tasks.dispatch_links = 1


async def dispatch_links(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:

    checksums = None
    for i, link in enumerate(links):
        if expected_checksums is not None:
            checksums = expected_checksums.get(link)
            if checksums and not isinstance(checksums, Checksum):
                checksums = Checksum(algorithm=checksums[0], value=checksums[1])
        else:
            expected_checksums = None
        await ctx.queues.links.put((i, link, checksums))

    for i in range(ctx.tasks.creators - 1, -1, -1):
        await ctx.queues.links.put((sys.maxsize - i, "", None))


async def session_killer(ctx: HydraContext) -> None:
    try:
        await ctx.sync.all_complete.wait()
    except asyncio.CancelledError:
        await ctx.net.close()
        raise


async def stream_all(
    ctx: HydraContext,
    links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> AsyncGenerator[tuple[str, AsyncGenerator[memoryview]]]:

    ctx.stream = True
    loop = asyncio.get_running_loop()

    try:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(session_killer(ctx), name="SessionKiller")
                await create_tasks(ctx, tg, loop, links, expected_checksums)
                file_gen = None
                while True:
                    file_id = await ctx.queues.file_discovery.get()

                    if file_id == -1:
                        break
                    filename = ctx.files[file_id].meta.filename

                    file_gen = _stream_one(ctx, file_id)

                    yield filename, file_gen

        except* Exception as eg:
            for e in eg.exceptions:
                await log(
                    ctx.ui, f"Critical System Failure: {e!r}", status=LogStatus.CRITICAL
                )
            raise
    except (asyncio.CancelledError, GeneratorExit):
        if ctx.config.debug:
            raise
        # Логируем через твой статус и останавливаем контекст
        await log(ctx.ui, "Operation cancelled by user.", status=LogStatus.INTERRUPT)
        await stop(ctx)

    finally:
        await teardown_engine(ctx, loop)


async def run_downloads(
    ctx: HydraContext,
    links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:
    ctx.stream = False

    loop = asyncio.get_running_loop()
    try:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(session_killer(ctx), name="SessionKiller")
                await create_tasks(ctx, tg, loop, links, expected_checksums)
            if ctx.config.dry_run:
                await print_dry_run_report(
                    ctx.ui, ctx.files, ctx.stream, ctx.config.output_dir
                )
        except* Exception as eg:
            await log(
                ctx.ui,
                f"Critical failure in TaskGroup: {eg.exceptions}",
                status=LogStatus.CRITICAL,
            )
            raise

    except asyncio.CancelledError:
        await stop(ctx)
        raise

    finally:
        await teardown_engine(ctx, loop)


def save_all_states(ctx: HydraContext, files: dict[int, File]) -> None:
    for file in list(files.values()):
        if file.chunks and not all(c.current_pos > c.end for c in (file.chunks or [])):
            ctx.fs.save_state(file)


def verify_stream(
    hasher: HASH,
    filename: str,
    expected_checksum: Checksum,
    next_offset: int,
    total_size: int,
) -> None:
    if next_offset != total_size:
        raise FileSizeMismatchError(
            filename=filename,
            expected=total_size,
            actual=next_offset,
            message_tpl="Incomplete stream data! Yielded {actual} of {expected} bytes.",
        )

    calculated = hasher.hexdigest()
    if calculated != expected_checksum.value:
        raise HashMismatchError(
            filename=filename,
            algorithm=expected_checksum.algorithm,
            expected=expected_checksum.value,
            actual=calculated,
        )
