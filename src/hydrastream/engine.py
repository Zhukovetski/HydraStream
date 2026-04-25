# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import math
import random
import signal
import sys
from collections.abc import AsyncGenerator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVarTuple, Unpack

from hydrastream.actors.autosaver import autosaver, save_all_states
from hydrastream.actors.controller import AdaptiveEngine
from hydrastream.actors.dispatcher import chunk_dispatcher
from hydrastream.actors.feeder import link_feeder
from hydrastream.actors.resolver import metadata_resolver
from hydrastream.actors.streamer import streamer
from hydrastream.actors.throttler import throttle_controller
from hydrastream.actors.worker import DownloadWorker
from hydrastream.actors.writer import disk_writer
from hydrastream.exceptions import (
    LogStatus,
)
from hydrastream.models import Checksum, Envelope, HydraContext, TypeHash
from hydrastream.monitor import log, print_dry_run_report, ui_start, ui_stop

Ts = TypeVarTuple("Ts")


async def send_poison_pills(
    queue: asyncio.PriorityQueue[Envelope[Any | None]],
    count: int,
    envelope_factory: Callable[[int, bool], Envelope[None]] | None = None,
) -> None:

    factory: Callable[[int, bool], Envelope[None]]

    if envelope_factory is None:

        def _default_factory(i: int, last: bool) -> Envelope[None]:
            return Envelope(
                sort_key=(sys.maxsize - i,), is_poison_pill=True, is_last_survivor=last
            )

        factory = _default_factory
    else:
        factory = envelope_factory

    for i in range(count - 1, 0, -1):
        await queue.put(factory(i, False))

    # Последняя "выжившая" пилюля
    await queue.put(factory(0, True))


async def delayed_task(
    ctx: HydraContext,
    task: Callable[[HydraContext, Unpack[Ts]], Awaitable[None]],
    *args: *Ts,
    delay: tuple[float, float] = (0, 0.3),
) -> None:
    await asyncio.sleep(random.uniform(*delay))
    await task(ctx, *args)


async def teardown_engine(ctx: HydraContext, loop: asyncio.AbstractEventLoop) -> None:
    if not ctx.is_running:
        return

    ctx.is_running = False
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
        ctx.ui.cancelled = True
        await log(
            ctx.ui,
            "Interrupt signal received. Initiating graceful shutdown...",
            status=LogStatus.INTERRUPT,
        )

        if ctx.stream:
            with contextlib.suppress(asyncio.QueueFull):
                ctx.queues.stream.put_nowait(
                    Envelope(sort_key=(-1,), is_poison_pill=True)
                )
                ctx.queues.file_discovery.put_nowait(-1)


async def prepare_runtime(ctx: HydraContext, loop: asyncio.AbstractEventLoop) -> None:
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


async def create_tasks(
    ctx: HydraContext,
    tg: asyncio.TaskGroup,
    links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:
    ctx.tasks.resolvers = math.ceil(len(links) ** 0.4) if len(links) > 1 else 1
    ctx.tasks.resolvers = min(ctx.tasks.resolvers, 20)

    if ctx.stream:
        ctx.tasks.workers = ctx.config.threads
    else:
        tg.create_task(disk_writer(ctx), name="Writer")
        ctx.tasks.workers = (
            math.ceil(ctx.config.threads * 1.2)
            if ctx.config.threads > 1
            else ctx.config.threads
        )
    if not ctx.config.dry_run:
        worker_events = [asyncio.Event() for _ in range(ctx.tasks.workers)]

        for i in range(ctx.tasks.workers):
            worker = DownloadWorker(ctx=ctx, worker_id=i, wakeup_event=worker_events[i])
            tg.create_task(worker.run(), name=f"Worker: {i}")

        tg.create_task(chunk_dispatcher(ctx), name="Dispatcher")
        ctx.tasks.dispatcher = 1

        tg.create_task(throttle_controller(ctx), name="Throttler")
        ctx.tasks.throttler = 1

        if ctx.config.threads > 1:
            controller = AdaptiveEngine(ctx, worker_events=worker_events)
            tg.create_task(controller.run(), name="Controller")
            ctx.tasks.controller = 1

        if not ctx.stream:
            tg.create_task(autosaver(ctx, interval=60), name="Autosaver")
            ctx.tasks.autosaver = 1

    for i in range(ctx.tasks.resolvers):
        tg.create_task(
            delayed_task(ctx, metadata_resolver),
            name=f"Resolver: {i}",
        )

    tg.create_task(link_feeder(ctx, links, expected_checksums), name="Feeder")
    ctx.tasks.feeder = 1


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
) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
    ctx.stream = True
    loop = asyncio.get_running_loop()
    await prepare_runtime(ctx, loop)

    try:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(session_killer(ctx), name="SessionKiller")
                await create_tasks(ctx, tg, links, expected_checksums)

                if not ctx.config.dry_run:
                    file_gen = None
                    while True:
                        file_id = await ctx.queues.file_discovery.get()

                        if file_id == -1:
                            break
                        filename = ctx.files[file_id].meta.filename

                        file_gen = streamer(ctx, file_id)

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
    await prepare_runtime(ctx, loop)

    try:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(session_killer(ctx), name="SessionKiller")
                await create_tasks(ctx, tg, links, expected_checksums)
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
