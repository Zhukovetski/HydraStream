# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import os
import random
import sys
import traceback

from curl_cffi import Response
from curl_cffi.requests import RequestsError

from hydrastream._curl_shim import aiter_bytes, get_error_response
from hydrastream.actors.controller import MaxLimitSignal, NetworkCongestionSignal
from hydrastream.exceptions import (
    DownloadFailedError,
    LogStatus,
    StreamError,
    WorkerScaleDown,
)
from hydrastream.models import (
    Chunk,
    Envelope,
    HydraContext,
    StreamChunk,
    WriteChunk,
    my_dataclass,
)
from hydrastream.monitor import done, log, update
from hydrastream.network import stream_chunk, try_scale_up


@my_dataclass
class DownloadWorker:
    ctx: HydraContext
    controller_outbox: asyncio.Queue[object]
    dynamic_limit_event: asyncio.Event

    async def run(self) -> None:
        while True:
            await self.dynamic_limit_event.wait()
            envelope, chunk = await self.get_chunk()
            if envelope is None:
                break
            if chunk is None:
                continue

            try:
                await self.process_chunk(chunk)

                if not chunk.is_finished:
                    await log(
                        self.ctx.ui,
                        f"Truncated read for {chunk.file.meta.filename}. "
                        f"Requeuing remaining {chunk.remaining} bytes.",
                        status=LogStatus.WARNING,
                        throttle_key="truncated_read",
                        throttle_sec=2.0,
                    )
                    await self.requeue_chunk(envelope, chunk, delay_range=(0.1, 1.0))
                    continue
                await self.file_done(chunk)
            except Exception as e:
                await self.handle_worker_error(envelope, chunk, e)

    async def get_chunk(self) -> tuple[Envelope[Chunk | None] | None, Chunk | None]:
        envelope = await self.ctx.queues.chunk.get()

        if envelope.is_poison_pill:
            if not self.ctx.sync.stop_adaptive_controller.is_set():
                self.ctx.sync.stop_adaptive_controller.set()
                self.ctx.ui.speed.controller_checkpoint_event.set()
                self.ctx.dynamic_limit = sys.maxsize
                await self.controller_outbox.put(MaxLimitSignal())

            if envelope.is_last_survivor:
                if self.ctx.stream:
                    await self.ctx.queues.file_discovery.put(-1)

                self.ctx.sync.all_complete.set()
                self.ctx.ui.speed.throttler_checkpoint_event.set()
                await self.ctx.queues.disk.put(
                    Envelope(
                        sort_key=(sys.maxsize,),
                        is_poison_pill=True,
                        is_last_survivor=True,
                    )
                )

            return None, None

        if not (chunk := envelope.payload):
            return envelope, None

        file_obj = chunk.file
        if not file_obj or file_obj.is_failed:
            return envelope, None

        if self.ctx.stream:
            async with self.ctx.sync.chunk_from_future:
                await self.ctx.sync.chunk_from_future.wait_for(
                    lambda: (
                        self.ctx.next_offset + self.ctx.config.BUFFER_SIZE
                        >= chunk.current_pos
                    )
                )
        return envelope, chunk

    async def handle_worker_error(
        self, envelope: Envelope[Chunk | None], chunk: Chunk, e: Exception
    ) -> None:
        if isinstance(e, WorkerScaleDown):
            await self.ctx.queues.chunk.put(envelope)
            return

        if isinstance(e, RequestsError):
            await self._handle_requests_error(envelope, chunk, e)
            self.ctx.dynamic_limit = max(self.ctx.dynamic_limit - 1, 1)
            await self.controller_outbox.put(NetworkCongestionSignal())
            return

        if isinstance(e, TimeoutError):
            await self.requeue_chunk(envelope, chunk)
            return

        tb_str = traceback.format_exc()

        if self.ctx.config.debug:
            # В дебаге выводим в консоль/лог всю простыню, чтобы сразу найти баг
            await log(
                self.ctx.ui, f"CRITICAL CRASH:\n{tb_str}", status=LogStatus.CRITICAL
            )

        else:
            await log(
                self.ctx.ui,
                f"Worker internal crash: {e!r}",
                status=LogStatus.CRITICAL,
                traceback=tb_str,  # Это поле уйдет в файл download.log!
            )
        raise e

    async def _handle_requests_error(
        self, envelope: Envelope[Chunk | None], chunk: Chunk, e: RequestsError
    ) -> None:
        """Разбирает сетевые ошибки и решает: убить файл или переповторить чанк."""
        response = get_error_response(e)
        if not isinstance(response, Response):
            await self.requeue_chunk(envelope, chunk)
            return

        status = response.status_code

        # Логика "Фатальных" ошибок
        if status in {400, 401, 403, 404, 410, 416}:
            await log(
                self.ctx.ui,
                f"Chunk for {chunk.file.meta.filename} "
                f"failed permanently (HTTP {status}).",
                status=LogStatus.ERROR,
            )
            chunk.file.is_failed = True
            self.ctx.fs.delete_file(chunk.file.meta.filename)

            if self.ctx.stream:
                raise DownloadFailedError(
                    url=chunk.file.meta.url,
                    status_code=status,
                    reason=response.reason,
                )
        else:
            await self.requeue_chunk(envelope, chunk, delay_range=(0.5, 2.0))

    async def requeue_chunk(
        self,
        envelope: Envelope[Chunk | None],
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        file_obj = chunk.file
        supports_ranges = file_obj.meta.supports_ranges

        if not supports_ranges:
            if self.ctx.stream:
                raise StreamError(
                    url=chunk.file.meta.url, filename=chunk.file.meta.filename
                )
            await log(
                self.ctx.ui,
                f"Connection dropped for {chunk.file.meta.filename}. "
                f"Server does not support resume. Restarting download from 0 bytes.",
                status=LogStatus.WARNING,
            )

            downloaded_so_far = chunk.current_pos - chunk.start
            if downloaded_so_far > 0:
                update(self.ctx.ui, chunk.file.meta.filename, -downloaded_so_far)

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
        await self.ctx.queues.chunk.put(envelope)
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def process_chunk(self, chunk: Chunk) -> None:
        if chunk.current_pos > chunk.end:
            return
        if chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
        else:
            headers = None

        if not self.ctx.stream:
            await self.disk_process_chunk(chunk, headers)
        else:
            await self.stream_process_chunk(chunk, headers)

    async def disk_process_chunk(
        self,
        chunk: Chunk,
        headers: dict[str, str] | None,
    ) -> None:
        buffer_list: list[bytes] = []
        current_buffer_size = 0

        fd = chunk.file.fd

        if fd is None:
            fd = self.ctx.fs.open_file(chunk.file.meta.filename)
        buffer_size = 1_048_576
        async with stream_chunk(
            self.ctx.net,
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            try:
                self.ctx.active_stream.add(r)
                bytes_to_read = chunk.end - chunk.current_pos + 1

                async for data in aiter_bytes(r, chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    buffer_list.append(data)
                    current_buffer_size += len(data)

                    bytes_to_read -= len(data)
                    update(self.ctx.ui, chunk.file.meta.filename, len(data))

                    if current_buffer_size >= buffer_size:
                        await self.ctx.queues.disk.put(
                            Envelope(
                                sort_key=(fd, chunk.current_pos),
                                payload=WriteChunk(
                                    fd=fd,
                                    offset=chunk.current_pos,
                                    length=current_buffer_size,
                                    data=buffer_list,
                                ),
                            )
                        )
                        chunk.current_pos += current_buffer_size

                        buffer_list.clear()
                        current_buffer_size = 0
                        if random.random() < 0.1:
                            await try_scale_up(self.ctx.net.rate_limiter)

                    if bytes_to_read <= 0:
                        break

                    if not self.dynamic_limit_event.is_set():
                        raise WorkerScaleDown

            finally:
                self.ctx.active_stream.remove(r)
                if buffer_list:
                    with contextlib.suppress(asyncio.QueueFull):
                        self.ctx.queues.disk.put_nowait(
                            Envelope(
                                sort_key=(fd, chunk.current_pos),
                                payload=WriteChunk(
                                    fd=fd,
                                    offset=chunk.current_pos,
                                    length=current_buffer_size,
                                    data=buffer_list,
                                ),
                            )
                        )

    async def stream_process_chunk(
        self,
        chunk: Chunk,
        headers: dict[str, str] | None,
    ) -> None:
        data = b""
        async with stream_chunk(
            self.ctx.net,
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            try:
                self.ctx.active_stream.add(r)
                bytes_to_read = chunk.end - chunk.current_pos + 1

                async for data in aiter_bytes(r, chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    bytes_to_read -= len(data)
                    update(self.ctx.ui, chunk.file.meta.filename, len(data))

                    await self.ctx.queues.stream.put(
                        Envelope(
                            sort_key=(chunk.current_pos,),
                            payload=StreamChunk(start=chunk.current_pos, data=data),
                        )
                    )
                    chunk.current_pos = chunk.current_pos + len(data)

                    if not self.dynamic_limit_event.is_set():
                        raise WorkerScaleDown

                    if bytes_to_read <= 0:
                        break

            finally:
                self.ctx.active_stream.remove(r)

    async def file_done(
        self,
        chunk: Chunk,
    ) -> None:
        if self.ctx.stream:
            return

        filename = chunk.file.meta.filename
        file_obj = chunk.file
        if chunk.file.meta.content_length:
            if chunk.file.verified or not chunk.file.is_complete:
                return
            chunk.file.verified = True
            if not self.ctx.fs.verify_size(filename, file_obj.meta.content_length):
                return
        if file_obj.meta.expected_checksum:
            await log(
                self.ctx.ui,
                f"Verifying Hash checksum for {chunk.file.meta.filename}...",
                status=LogStatus.INFO,
            )
            await self.ctx.fs.verify_file_hash(
                file_obj.meta.filename,
                file_obj.meta.expected_checksum.value,
                file_obj.meta.expected_checksum.algorithm,
            )
            await log(
                self.ctx.ui,
                f"Integrity confirmed: {chunk.file.meta.filename}",
                status=LogStatus.SUCCESS,
            )
        self.ctx.fs.close_file(fd_or_conn=file_obj.fd)
        self.ctx.fs.delete_state(filename)
        await done(self.ctx.ui, filename)
        del self.ctx.files[chunk.file.meta.id]
        self.ctx.current_files_id.remove(chunk.file.meta.id)
        async with self.ctx.sync.current_files:
            self.ctx.sync.current_files.notify_all()
