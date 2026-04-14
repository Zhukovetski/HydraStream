# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import math
import os
import random
import sys
import time
from typing import cast

from curl_cffi import CurlOpt, Response
from curl_cffi.requests import RequestsError

from hydrastream.exceptions import (
    DownloadFailedError,
    LogStatus,
    WorkerScaleDown,
)
from hydrastream.utils import format_size

from .models import Chunk, HydraContext
from .monitor import done, log, update
from .network import stream_chunk, try_scale_up


async def telemetry_worker(ctx: HydraContext) -> None:
    """Фоновый инспектор. Управляет скоростью и Авто-тюнингом воркеров."""

    ctx.ui.speed.last_checkpoint_time = time.monotonic()
    smoothed_speed = 0.0
    prev_speed = 0.0
    current_limit = ctx.dynamic_limit

    # Безопасные границы для адаптивного окна
    tau = 1.0
    min_window = 1024  # 1 КБ (чтобы не сжечь CPU)

    while not ctx.sync.all_complete.is_set():
        try:
            # Ждем пульса от Монитора (или пинка перед смертью)
            await ctx.ui.speed.checkpoint_event.wait()

            # Сразу проверяем: а не нажали ли рубильник пока мы спали?
            if ctx.sync.stop_telemetry.is_set():
                break  # Выходим из цикла! Функция завершается, TaskGroup счастлив.
            await ctx.ui.speed.checkpoint_event.wait()
            ctx.ui.speed.checkpoint_event.clear()

            now = time.monotonic()
            elapsed = min(1, now - ctx.ui.speed.last_checkpoint_time)

            if elapsed <= 0:
                continue  # Защита от деления на ноль при сверхбыстрых всплесках

            # =========================================================
            # ФАЗА 1: ЛОГИКА ОГРАНИЧИТЕЛЯ СКОРОСТИ (Global Valve)
            # =========================================================
            if ctx.ui.speed.speed_limit:
                target_time = ctx.ui.speed.bytes_to_check / ctx.ui.speed.speed_limit

                if elapsed < target_time:
                    sleep_duration = target_time - elapsed

                    # ЗАКРЫВАЕМ ВЕНТИЛЬ
                    for r in ctx.active_stream:
                        if r.curl is not None:
                            r.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, 1)
                    await asyncio.sleep(sleep_duration)
                    # ОТКРЫВАЕМ ВЕНТИЛЬ
                    for r in ctx.active_stream:
                        if r.curl is not None:
                            r.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, 0)
                    # Важно: мы спали! Нужно пересчитать 'now' и 'elapsed'
                    # для Авто-тюнера, чтобы он считал РЕАЛЬНУЮ скорость с учетом паузы.
                    now = time.monotonic()
                    elapsed = now - ctx.ui.speed.last_checkpoint_time

                    continue

            # =========================================================
            # ФАЗА 2: АВТО-ТЮНЕР (AIMD Hill Climbing)
            # =========================================================
            speed_now = ctx.ui.speed.bytes_to_check / elapsed

            if smoothed_speed == 0.0:
                smoothed_speed = speed_now
            else:
                # Магия: вычисляем вес нового замера на основе физического времени
                alpha = 1.0 - math.exp(-elapsed / tau)
                smoothed_speed = (alpha * speed_now) + ((1.0 - alpha) * smoothed_speed)

            coef = 1 / speed_now**0.2
            # Пересчитываем, когда мы хотим проснуться в следующий раз
            new_bytes_to_check = int(ctx.ui.speed.bytes_to_check * (1 - coef + elapsed))

            ctx.ui.speed.bytes_to_check = max(min_window, new_bytes_to_check)
            # Анализируем результат нашего предыдущего шага
            if smoothed_speed > prev_speed * 1.05:
                prev_speed = smoothed_speed
                # Скорость выросла! Добавляем воркеров (если не уперлись в лимит юзера)
                if current_limit < ctx.config.threads:
                    current_limit += 1
                    await log(
                        ctx.ui,
                        f"Speed increased to {format_size(speed_now)}/s. "
                        f"Scaling up to {current_limit} workers.",
                        status=LogStatus.INFO,
                        throttle_key="scale_up",
                        throttle_sec=5.0,
                    )

            elif smoothed_speed < prev_speed * 0.95:
                prev_speed = smoothed_speed
                # Скорость резко упала (сеть забилась). Скидываем воркеров.
                if current_limit > 2:
                    current_limit -= 1
                    await log(
                        ctx.ui,
                        f"Network congested. Scaling down to {current_limit} workers.",
                        status=LogStatus.WARNING,
                        throttle_key="scale_down",
                        throttle_sec=5.0,
                    )

            # Применяем лимит
            if ctx.dynamic_limit != current_limit:
                ctx.dynamic_limit = current_limit
                # Будим диспетчера только если лимит изменился
                async with ctx.sync.dynamic_limit:
                    ctx.sync.dynamic_limit.notify_all()

            # Фиксируем время для следующего круга
            ctx.ui.speed.last_checkpoint_time = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if ctx.config.debug:
                raise
            await log(ctx.ui, f"Telemetry failed: {e}", status=LogStatus.ERROR)


async def file_done(ctx: HydraContext, chunk: Chunk) -> None:
    if ctx.stream:
        return

    filename = chunk.file.meta.filename
    file_obj = chunk.file
    if chunk.file.meta.content_length:
        if chunk.file.verified or not chunk.file.is_complete:
            return
        chunk.file.verified = True
        if not ctx.fs.verify_size(filename, file_obj.meta.content_length):
            return
    if file_obj.meta.expected_checksum:
        await log(
            ctx.ui,
            f"Verifying Hash checksum for {chunk.file.meta.filename}...",
            status=LogStatus.INFO,
        )
        await ctx.fs.verify_file_hash(
            file_obj.meta.filename,
            file_obj.meta.expected_checksum.value,
            file_obj.meta.expected_checksum.algorithm,
        )
        await log(
            ctx.ui,
            f"Integrity confirmed: {chunk.file.meta.filename}",
            status=LogStatus.SUCCESS,
        )
    ctx.fs.close_file(fd_or_conn=file_obj.fd)
    ctx.fs.delete_state(filename)
    await done(ctx.ui, filename)
    del ctx.files[chunk.file.meta.id]
    ctx.current_files_id.remove(chunk.file.meta.id)
    async with ctx.sync.current_files:
        ctx.sync.current_files.notify_all()


async def get_chunk(ctx: HydraContext) -> Chunk | None:
    if ctx.stream:
        id, chunk = await ctx.queues.chunk.get()
    else:
        chunk, id = await ctx.queues.chunk.get()
    chunk = cast(Chunk, chunk)
    id = cast(int, id)

    if sys.maxsize - ctx.tasks.workers < id:
        if not ctx.sync.stop_telemetry.is_set():
            ctx.sync.stop_telemetry.set()
            ctx.ui.speed.checkpoint_event.set()
            ctx.dynamic_limit = sys.maxsize
            async with ctx.sync.dynamic_limit:
                ctx.sync.dynamic_limit.notify_all()
                ctx.sync.all_complete.set()

        if id == sys.maxsize:
            await log(ctx.ui, f"{ctx.tasks.workers}", status=LogStatus.WARNING)
            if ctx.stream:
                await ctx.queues.file_discovery.put(-1)

        raise asyncio.CancelledError

    file_obj = chunk.file
    if not file_obj or file_obj.is_failed:
        return None
    if ctx.stream:
        async with ctx.sync.chunk_from_future:
            await ctx.sync.chunk_from_future.wait_for(
                lambda: (
                    ctx.next_offset + ctx.config.STREAM_BUFFER_SIZE >= chunk.current_pos
                )
            )
    return chunk


async def download_worker(ctx: HydraContext, worker_id: int) -> None:
    while True:
        try:
            if worker_id >= ctx.dynamic_limit:
                async with ctx.sync.dynamic_limit:
                    await ctx.sync.dynamic_limit.wait_for(
                        lambda: worker_id < ctx.dynamic_limit
                    )
            chunk = None
            chunk = await get_chunk(ctx)
            if chunk is None:
                continue
            await process_chunk(ctx, chunk, worker_id)
            await file_done(ctx, chunk)

        except WorkerScaleDown:
            if chunk:
                if ctx.stream:
                    await ctx.queues.chunk.put((-1, chunk))
                else:
                    await ctx.queues.chunk.put((chunk, -1))

        except asyncio.CancelledError:
            break

        except RequestsError as e:
            if chunk:
                response = e.response  # type: ignore

                if isinstance(response, Response):
                    # Теперь Pyright ВИДИТ, что это Response, и даст автодополнение
                    status = response.status_code

                    if status in {400, 401, 403, 404, 410, 416}:
                        await log(
                            ctx.ui,
                            f"Chunk for {chunk.file.meta.filename} failed permanently"
                            f"(HTTP {status}).",
                            status=LogStatus.ERROR,
                        )
                        chunk.file.is_failed = True
                        ctx.fs.delete_file(chunk.file.meta.filename)
                        if ctx.stream:
                            raise DownloadFailedError(
                                url=chunk.file.meta.url,
                                status_code=status,
                                reason=response.reason,
                            ) from None

                    # Остальные ошибки сервера (5xx, 429) — пробуем перекинуть чанк
                    # в очередь
                    else:
                        await requeue_chunk(ctx, chunk, delay_range=(0.5, 2.0))
                # 2. Если ответа нет (Сетевая ошибка / CurlError / Timeout)
                else:
                    await requeue_chunk(ctx, chunk)

            ctx.dynamic_limit = max(ctx.dynamic_limit - 1, 1)
            async with ctx.sync.dynamic_limit:
                ctx.sync.dynamic_limit.notify_all()

        except TimeoutError:
            if chunk:
                await requeue_chunk(ctx, chunk)

        except Exception as e:
            await log(
                ctx.ui, f"Worker internal crash: {e!r}", status=LogStatus.CRITICAL
            )
            raise


async def requeue_chunk(
    ctx: HydraContext,
    chunk: Chunk,
    delay_range: tuple[float, float] = (1.0, 3.0),
) -> None:
    if not chunk:
        return

    file_obj = chunk.file
    supports_ranges = file_obj.meta.supports_ranges

    if not supports_ranges:
        if ctx.stream:
            err_msg = (
                f"Stream interrupted for {chunk.file.meta.filename}. "
                f"Server does not support partial downloads (Range requests). "
                f"Cannot resume stream. Aborting."
            )
            await log(ctx.ui, err_msg, status=LogStatus.CRITICAL)

            file_obj.is_failed = True
            return

        await log(
            ctx.ui,
            f"Connection dropped for {chunk.file.meta.filename}. "
            f"Server does not support resume. Restarting download from 0 bytes.",
            status=LogStatus.WARNING,
        )

        downloaded_so_far = chunk.current_pos - chunk.start
        if downloaded_so_far > 0:
            update(ctx.ui, chunk.file.meta.filename, -downloaded_so_far)

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
    if ctx.stream:
        await ctx.queues.chunk.put((-1, chunk))
    else:
        await ctx.queues.chunk.put((chunk, -1))
    delay = random.uniform(*delay_range)
    await asyncio.sleep(delay)


async def disk_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str] | None, worker_id: int
) -> None:
    buffer = bytearray()
    fd = chunk.file.fd

    if fd is None:
        fd = ctx.fs.open_file(chunk.file.meta.filename)
    buffer_size = 1_048_576
    async with stream_chunk(
        ctx.net,
        chunk.file.meta.url,
        headers=headers,
    ) as r:
        ctx.active_stream.add(r)
        try:
            async for data in r.aiter_content(chunk_size=131072):  # type: ignore
                data = cast(bytes, data)
                buffer.extend(data)
                update(ctx.ui, chunk.file.meta.filename, len(data))
                if len(buffer) >= buffer_size:
                    await ctx.fs.write_chunk_data(fd, buffer, chunk.current_pos)
                    chunk.current_pos += len(buffer)
                    buffer = bytearray()
                    if random.random() < 0.1:
                        await try_scale_up(ctx.net.rate_limiter)
                if ctx.dynamic_limit <= worker_id:
                    raise WorkerScaleDown

        finally:
            ctx.active_stream.remove(r)
            if buffer:
                await ctx.fs.write_chunk_data(fd, buffer, chunk.current_pos)
                chunk.current_pos += len(buffer)


async def stream_process_chunk(
    ctx: HydraContext, chunk: Chunk, headers: dict[str, str] | None, worker_id: int
) -> None:
    buffer = bytearray()
    async with stream_chunk(
        ctx.net,
        chunk.file.meta.url,
        headers=headers,
    ) as r:
        ctx.active_stream.add(r)
        try:
            async for data in r.aiter_content(chunk_size=131072):  # type: ignore
                data = cast(bytes, data)
                buffer.extend(data)
                update(ctx.ui, chunk.file.meta.filename, len(data))
                if len(buffer) > ctx.config.STREAM_CHUNK_SIZE:
                    await ctx.queues.stream.put((chunk.current_pos, buffer))
                    chunk.current_pos = chunk.current_pos + len(buffer)
                    buffer = bytearray()
                if ctx.dynamic_limit <= worker_id:
                    raise WorkerScaleDown
            await ctx.queues.stream.put((chunk.current_pos, buffer))
            chunk.current_pos = chunk.current_pos + len(buffer)
            buffer = bytearray()
        except asyncio.CancelledError:
            raise
        except Exception:
            if ctx.stream and buffer:
                with contextlib.suppress(asyncio.QueueFull):
                    ctx.queues.stream.put_nowait((chunk.current_pos, buffer))
                    chunk.current_pos = chunk.current_pos + len(buffer)
            raise
        finally:
            ctx.active_stream.remove(r)


async def process_chunk(ctx: HydraContext, chunk: Chunk, worker_id: int) -> None:
    if chunk.current_pos > chunk.end:
        return
    if chunk.file.meta.supports_ranges:
        headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
    else:
        headers = None
    if not ctx.stream:
        await disk_process_chunk(ctx, chunk, headers, worker_id)
    else:
        await stream_process_chunk(ctx, chunk, headers, worker_id)
