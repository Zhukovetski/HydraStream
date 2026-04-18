# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import os
import random
import sys
from dataclasses import dataclass

from curl_cffi import Response
from curl_cffi.requests import RequestsError

from hydrastream.exceptions import (
    DownloadFailedError,
    LogStatus,
    WorkerScaleDown,
)
from hydrastream.models import Chunk, HydraContext
from hydrastream.monitor import done, log, update
from hydrastream.network import stream_chunk, try_scale_up


@dataclass
class DownloadWorker:
    ctx: HydraContext
    worker_id: int
    chunk: Chunk | None = None

    async def run(self) -> None:
        while True:
            try:
                if self.worker_id >= self.ctx.dynamic_limit:
                    async with self.ctx.sync.dynamic_limit:
                        await self.ctx.sync.dynamic_limit.wait_for(
                            lambda: self.worker_id < self.ctx.dynamic_limit
                        )
                await self.get_chunk()
                if self.chunk is None:
                    continue
                await self.process_chunk()

                if not self.chunk.is_finished:
                    await log(
                        self.ctx.ui,
                        f"Truncated read for {self.chunk.file.meta.filename}. "
                        f"Requeuing remaining {self.chunk.remaining} bytes.",
                        status=LogStatus.WARNING,
                        throttle_key="truncated_read",
                        throttle_sec=2.0,
                    )
                    await self.requeue_chunk(delay_range=(0.1, 1.0))
                    continue

                await self.file_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.handle_worker_error(e)
            finally:
                self.chunk = None

    async def get_chunk(self) -> Chunk | None:
        if self.ctx.stream:
            id, chunk = await self.ctx.queues.chunk.get()
        else:
            chunk, id = await self.ctx.queues.chunk.get()

        if not isinstance(chunk, Chunk) or not isinstance(id, int):
            raise ValueError
        self.chunk = chunk

        if sys.maxsize - self.ctx.tasks.workers < id:
            if not self.ctx.sync.stop_adaptive_controller.is_set():
                self.ctx.sync.stop_adaptive_controller.set()
                self.ctx.ui.speed.controller_checkpoint_event.set()
                self.ctx.dynamic_limit = sys.maxsize
                async with self.ctx.sync.dynamic_limit:
                    self.ctx.sync.dynamic_limit.notify_all()

            if id == sys.maxsize:
                if self.ctx.stream:
                    await self.ctx.queues.file_discovery.put(-1)

                self.ctx.sync.all_complete.set()
                self.ctx.ui.speed.throttler_checkpoint_event.set()

            raise asyncio.CancelledError

        file_obj = chunk.file
        if not file_obj or file_obj.is_failed:
            return None

        if self.ctx.stream:
            async with self.ctx.sync.chunk_from_future:
                await self.ctx.sync.chunk_from_future.wait_for(
                    lambda: (
                        self.ctx.next_offset + self.ctx.config.STREAM_BUFFER_SIZE
                        >= chunk.current_pos
                    )
                )

    async def handle_worker_error(self, e: Exception) -> None:
        if isinstance(e, WorkerScaleDown):
            if self.chunk:
                await self.ctx.queues.chunk.put(
                    (-1, self.chunk) if self.ctx.stream else (self.chunk, -1)
                )
            return

        if isinstance(e, RequestsError):
            await self._handle_requests_error(e)
            self.ctx.dynamic_limit = max(self.ctx.dynamic_limit - 1, 1)
            async with self.ctx.sync.dynamic_limit:
                self.ctx.sync.dynamic_limit.notify_all()
            return

        if isinstance(e, TimeoutError):
            if self.chunk:
                await self.requeue_chunk()
            return

        await log(
            self.ctx.ui, f"Worker internal crash: {e!r}", status=LogStatus.CRITICAL
        )
        raise e

    async def _handle_requests_error(self, e: RequestsError) -> None:
        """Разбирает сетевые ошибки и решает: убить файл или переповторить чанк."""
        if not self.chunk:
            return

        response = e.response
        if not isinstance(response, Response):
            await self.requeue_chunk()
            return

        status = response.status_code

        # Логика "Фатальных" ошибок
        if status in {400, 401, 403, 404, 410, 416}:
            await log(
                self.ctx.ui,
                f"Chunk for {self.chunk.file.meta.filename} "
                f"failed permanently (HTTP {status}).",
                status=LogStatus.ERROR,
            )
            self.chunk.file.is_failed = True
            self.ctx.fs.delete_file(self.chunk.file.meta.filename)

            if self.ctx.stream:
                raise DownloadFailedError(
                    url=self.chunk.file.meta.url,
                    status_code=status,
                    reason=response.reason,
                )
        else:
            await self.requeue_chunk(delay_range=(0.5, 2.0))

    async def requeue_chunk(
        self,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        if not self.chunk:
            return

        file_obj = self.chunk.file
        supports_ranges = file_obj.meta.supports_ranges

        if not supports_ranges:
            if self.ctx.stream:
                err_msg = (
                    f"Stream interrupted for {self.chunk.file.meta.filename}. "
                    f"Server does not support partial downloads (Range requests). "
                    f"Cannot resume stream. Aborting."
                )
                await log(self.ctx.ui, err_msg, status=LogStatus.CRITICAL)

                file_obj.is_failed = True
                return

            await log(
                self.ctx.ui,
                f"Connection dropped for {self.chunk.file.meta.filename}. "
                f"Server does not support resume. Restarting download from 0 bytes.",
                status=LogStatus.WARNING,
            )

            downloaded_so_far = self.chunk.current_pos - self.chunk.start
            if downloaded_so_far > 0:
                update(self.ctx.ui, self.chunk.file.meta.filename, -downloaded_so_far)

            self.chunk.current_pos = self.chunk.start

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
        await self.ctx.queues.chunk.put(
            (-1, self.chunk) if self.ctx.stream else (self.chunk, -1)
        )
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def process_chunk(self) -> None:
        if not self.chunk:
            return

        if self.chunk.current_pos > self.chunk.end:
            return
        if self.chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={self.chunk.current_pos}-{self.chunk.end}"}
        else:
            headers = None

        if not self.ctx.stream:
            await self.disk_process_chunk(headers)
        else:
            await self.stream_process_chunk(headers)

    async def disk_process_chunk(
        self,
        headers: dict[str, str] | None,
    ) -> None:
        if not self.chunk:
            return

        buffer = bytearray()
        fd = self.chunk.file.fd

        if fd is None:
            fd = self.ctx.fs.open_file(self.chunk.file.meta.filename)
        buffer_size = 1_048_576
        async with stream_chunk(
            self.ctx.net,
            self.chunk.file.meta.url,
            headers=headers,
        ) as r:
            self.ctx.active_stream.add(r)
            try:
                bytes_to_read = self.chunk.end - self.chunk.current_pos + 1

                async for data in r.aiter_content(chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    buffer.extend(data)
                    bytes_to_read -= len(data)
                    update(self.ctx.ui, self.chunk.file.meta.filename, len(data))
                    if len(buffer) >= buffer_size:
                        await self.ctx.fs.write_chunk_data(
                            fd, buffer, self.chunk.current_pos
                        )
                        self.chunk.current_pos += len(buffer)
                        buffer = bytearray()
                        if random.random() < 0.1:
                            await try_scale_up(self.ctx.net.rate_limiter)

                    if self.ctx.dynamic_limit <= self.worker_id:
                        raise WorkerScaleDown
                    if bytes_to_read <= 0:
                        break

            finally:
                self.ctx.active_stream.remove(r)
                if buffer:
                    await self.ctx.fs.write_chunk_data(
                        fd, buffer, self.chunk.current_pos
                    )
                    self.chunk.current_pos += len(buffer)

    async def stream_process_chunk(
        self,
        headers: dict[str, str] | None,
    ) -> None:
        if not self.chunk:
            return

        buffer = bytearray()
        async with stream_chunk(
            self.ctx.net,
            self.chunk.file.meta.url,
            headers=headers,
        ) as r:
            self.ctx.active_stream.add(r)
            try:
                bytes_to_read = self.chunk.end - self.chunk.current_pos + 1

                async for data in r.aiter_content(chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    buffer.extend(data)
                    bytes_to_read -= len(data)
                    update(self.ctx.ui, self.chunk.file.meta.filename, len(data))
                    if len(buffer) > self.ctx.config.STREAM_CHUNK_SIZE:
                        await self.ctx.queues.stream.put((
                            self.chunk.current_pos,
                            buffer,
                        ))
                        self.chunk.current_pos = self.chunk.current_pos + len(buffer)
                        buffer = bytearray()

                    if self.ctx.dynamic_limit <= self.worker_id:
                        raise WorkerScaleDown

                    if bytes_to_read <= 0:
                        break
                await self.ctx.queues.stream.put((self.chunk.current_pos, buffer))
                self.chunk.current_pos = self.chunk.current_pos + len(buffer)
                buffer = bytearray()
            except asyncio.CancelledError:
                raise
            except Exception:
                if self.ctx.stream and buffer:
                    with contextlib.suppress(asyncio.QueueFull):
                        self.ctx.queues.stream.put_nowait((
                            self.chunk.current_pos,
                            buffer,
                        ))
                        self.chunk.current_pos = self.chunk.current_pos + len(buffer)
                raise
            finally:
                self.ctx.active_stream.remove(r)

    async def file_done(
        self,
    ) -> None:
        if not self.chunk:
            return

        if self.ctx.stream:
            return

        filename = self.chunk.file.meta.filename
        file_obj = self.chunk.file
        if self.chunk.file.meta.content_length:
            if self.chunk.file.verified or not self.chunk.file.is_complete:
                return
            self.chunk.file.verified = True
            if not self.ctx.fs.verify_size(filename, file_obj.meta.content_length):
                return
        if file_obj.meta.expected_checksum:
            await log(
                self.ctx.ui,
                f"Verifying Hash checksum for {self.chunk.file.meta.filename}...",
                status=LogStatus.INFO,
            )
            await self.ctx.fs.verify_file_hash(
                file_obj.meta.filename,
                file_obj.meta.expected_checksum.value,
                file_obj.meta.expected_checksum.algorithm,
            )
            await log(
                self.ctx.ui,
                f"Integrity confirmed: {self.chunk.file.meta.filename}",
                status=LogStatus.SUCCESS,
            )
        self.ctx.fs.close_file(fd_or_conn=file_obj.fd)
        self.ctx.fs.delete_state(filename)
        await done(self.ctx.ui, filename)
        del self.ctx.files[self.chunk.file.meta.id]
        self.ctx.current_files_id.remove(self.chunk.file.meta.id)
        async with self.ctx.sync.current_files:
            self.ctx.sync.current_files.notify_all()
