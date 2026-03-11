# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import hashlib
import heapq
import random
import signal
from collections.abc import AsyncGenerator, Iterable
from types import TracebackType
from typing import Any, Self

import httpx

from .models import Chunk, File
from .monitor import ProgressMonitor
from .network import NetworkClient
from .providers import ProviderRouter
from .storage import StorageManager


class HydraStream:
    """
    High-performance asynchronous manager for downloading and streaming genomics data.

    Implements concurrent chunk downloading, in-memory reordering for sequential streams,
    atomic disk writes, and automatic MD5 integrity verification.
    """

    def __init__(
        self,
        threads: int = 3,
        no_ui: bool = False,
        quiet: bool = False,
        output_dir: str = "download",
        stream_buffer_size: int | None = None,
        chunk_timeout: int = 120,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.is_running = True

        self._monitor = ProgressMonitor(no_ui=no_ui, quiet=quiet, log_file=f"{output_dir}/session.log")
        self.network = NetworkClient(threads=threads, monitor=self._monitor, client_kwargs=client_kwargs)
        self.storage = StorageManager(output_dir=output_dir)
        self.providers = ProviderRouter(self.network)

        self._max_conns = threads
        self.chunk_timeout = chunk_timeout
        self._stream = None
        self._stream_buffer_size = stream_buffer_size
        self.stream_chunk_size = 5 * 1024 * 1024
        self.MIN_CHUNK = 1 * 1024 * 1024
        self._semaphore = asyncio.Semaphore(threads)

        self._queue: asyncio.Queue[tuple[int, Chunk]] = asyncio.PriorityQueue()
        maxsize = stream_buffer_size // self.MIN_CHUNK if stream_buffer_size else 0
        maxsize = maxsize if maxsize > self._max_conns else self._max_conns
        self._stream_queue: asyncio.Queue[tuple[int, bytearray]] = asyncio.Queue(maxsize)
        self._file_discovery_queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.condition = asyncio.Condition()

        self.files: dict[str, File] = {}
        self.heap: list[tuple[int, bytearray]] = []
        self.heap_size = maxsize - self._max_conns
        self.workers: list[asyncio.Task[None]] = []
        self.task_creator = None
        self.autosave_task = None
        self.current_file = ""

    async def _autosave(self) -> None:
        while self.is_running:
            try:
                await asyncio.sleep(60)
                await self.storage.save_all_states(self.files)
            except asyncio.CancelledError:
                break

    async def _add_task_producer(
        self, links: Iterable[str], expected_checksums: dict[str, str] | None = None
    ) -> None:
        """
        Retrieves metadata (HEAD requests) for provided URLs, initializes storage,
        and populates the priority queue with chunk tasks.
        """
        checksums_map = expected_checksums if isinstance(expected_checksums, dict) else {}
        for i, url in enumerate(links):
            if not self.is_running:
                break

            md5_val = checksums_map.get(url)

            try:
                file = None
                total_size = 0

                # Fetch metadata
                response = await self.network.safe_request("Head", url=url)

                if response is None:
                    await self._monitor.log(f"Skipping {url} due to unreachable remote.", status="ERROR")
                    if self._stream:
                        await self._file_discovery_queue.put(None)
                    continue

                total_size = int(response.headers.get("content-length", 0))

                filename = self.network.extract_filename(url, response.headers)

                if total_size <= 0:
                    await self._monitor.log(
                        f"Invalid Content-Length ({total_size}) for {filename}", status="ERROR"
                    )
                    continue

                # Auto-fetch MD5 if missing
                if not md5_val:
                    self._monitor.add_file(filename)
                    md5_val = await self.providers.resolve_hash(url, filename)
                    await self._monitor.done(filename)

                # 1. State Recovery
                if not self._stream:
                    file, num = self.storage.load_state(filename=filename)
                    if num > 1:
                        await self._monitor.log(
                            f"Multiple state files found for {filename}!", status="WARNING"
                        )

                # 2. Chunk geometry calculation
                parts = self._max_conns
                chunk_size = max(total_size // parts, self.MIN_CHUNK)
                if self._stream and chunk_size > self.stream_chunk_size:
                    chunk_size = self.stream_chunk_size

                # 3. File object creation
                if not file:
                    if not self._stream:
                        new_filename = self.storage.create_sparse_file(filename=filename, size=total_size)
                        if new_filename:
                            await self._monitor.log(
                                f"{filename} already exists. The new file will be saved as {new_filename}.",
                                status="WARNING",
                            )
                            filename = new_filename

                    file = File(
                        filename=filename,
                        url=url,
                        content_length=total_size,
                        chunk_size=chunk_size,
                        expected_md5=md5_val,
                    )
                filename = file.filename
                if not self._stream:
                    file.fd = self.storage.open_file(filename=filename)

                # UI Registration
                self.files[filename] = file
                chunks = file.chunks or []

                self._monitor.add_file(filename, total_size)
                if not self._stream:
                    downloaded = sum(c.uploaded for c in chunks)
                    if downloaded - len(chunks) > 0:
                        self._monitor.update(filename, downloaded)
                else:
                    await self._file_discovery_queue.put(filename)

                # 4. Dispatch tasks to queue
                for c in chunks:
                    if not self.is_running:
                        break
                    if c.current_pos <= c.end:
                        await self._queue.put((i, c))

            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._monitor.log(f"Failed to process URL {url}: {e}", status="ERROR")
                if self._stream:
                    await self._file_discovery_queue.put(None)
                # Do not raise here; allow the producer to move to the next URL

    async def _download_worker(self) -> None:
        """
        Background worker that consumes chunks from the queue, downloads data,
        and triggers integrity checks upon file completion.
        """
        while self.is_running:
            chunk = None
            try:
                _, chunk = await self._queue.get()
                file_obj = self.files.get(chunk.filename)
                if not file_obj or file_obj.is_failed:
                    self._queue.task_done()
                    continue

                # Stream backpressure: wait for the current file to be completely consumed
                if self._stream and self.current_file != chunk.filename:
                    async with self.condition:
                        await self.condition.wait_for(
                            lambda c=chunk.filename: (
                                getattr(self, "current_file", None) == c or not self.is_running
                            )
                        )

                await self._process_chunk(chunk)

                # Integrity check trigger for disk mode
                if not self._stream:
                    filename = chunk.filename
                    file_obj = self.files.get(filename)

                    if file_obj and file_obj.is_complete:
                        if file_obj.expected_md5 and not file_obj.verified:
                            file_obj.verified = True
                            await self._monitor.log(
                                f"Verifying MD5 checksum for {filename}...", status="INFO"
                            )
                            try:
                                loop = asyncio.get_event_loop()
                                await loop.run_in_executor(None, self.storage.verify_file_hash, file_obj)
                                await self._monitor.log(f"Integrity confirmed: {filename}", status="SUCCESS")
                            except ValueError as ve:
                                await self._monitor.log(str(ve), status="ERROR")
                                # Optional: Add logic to quarantine/delete corrupted files here

                        file_obj.close_fd()
                        self.storage.verify_size(file_obj)
                        self.storage.delete_state(filename)
                        await self._monitor.done(filename)
                        self._queue.task_done()
                        del self.files[filename]

                        if not self.files:
                            async with self.condition:
                                self.condition.notify_all()
                # ------------------------------------
            except asyncio.CancelledError:
                break  # Worker shutdown requested

            except httpx.HTTPStatusError as e:
                if chunk:
                    if e.response.status_code in {400, 401, 403, 404, 410, 416}:
                        await self._monitor.log(
                            f"Chunk for {chunk.filename} failed permanently (HTTP {e.response.status_code}).",
                            status="ERROR",
                        )
                        self.files[chunk.filename].is_failed = True
                    else:
                        if self.is_running:
                            # Exponential or fixed backoff for chunk retry
                            await asyncio.sleep(random.uniform(0.5, 2.0))
                            # Re-queue with high priority (-1)
                            await self._queue.put((-1, chunk))
            except (httpx.RequestError, TimeoutError):
                if self.is_running and chunk:
                    await asyncio.sleep(random.uniform(1.0, 3.0))
                    await self._queue.put((-1, chunk))

            except Exception as e:
                await self._monitor.log(f"Critical Worker Exception: {e!r}", status="CRITICAL")
                raise
            finally:
                self._queue.task_done()

    async def _process_chunk(self, chunk: Chunk) -> None:
        async with self._semaphore:
            if chunk.current_pos > chunk.end:
                return
            downloaded_in_this_attempt = 0
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
            buffer = bytearray()
            try:
                async with self.network.stream_chunk(
                    self.files[chunk.filename].url, headers=headers, chunk_timeout=self.chunk_timeout
                ) as r:
                    if not self._stream:
                        fd = self.files[chunk.filename].fd
                        if fd is None:
                            fd = self.storage.open_file(chunk.filename)
                        buffer_size = 1024 * 1024
                        try:
                            async for data in r.aiter_bytes():
                                downloaded_in_this_attempt += len(data)
                                buffer.extend(data)

                                self._monitor.update(chunk.filename, len(data))
                                if len(buffer) >= buffer_size:
                                    await self._write_buffer(fd, buffer, chunk)
                                    buffer = bytearray()

                        finally:
                            if buffer:
                                await self._write_buffer(fd, buffer, chunk)
                    else:
                        async for data in r.aiter_bytes():
                            downloaded_in_this_attempt += len(data)
                            buffer.extend(data)
                            self._monitor.update(chunk.filename, len(data))
                        if len(self.heap) > self.heap_size:
                            async with self.condition:
                                await self.condition.wait_for(lambda: len(self.heap) <= self.heap_size)
                        await self._stream_queue.put((chunk.current_pos, buffer))
                        chunk.current_pos = chunk.current_pos + len(buffer)
                        buffer = bytearray()
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._stream and buffer:
                    self._stream_queue.put_nowait((chunk.current_pos, buffer))
                    chunk.current_pos = chunk.current_pos + len(buffer)
                raise

    async def _write_buffer(self, fd: int, data: bytearray, chunk: Chunk) -> None:
        await self.storage.write_chunk_data(fd, data, chunk.current_pos)
        chunk.current_pos += len(data)

    async def _stop(self, complete: bool = False) -> None:
        """
        Executes a graceful shutdown sequence.
        Cancels active tasks and flushes deadlocked queues using poison pills.
        """
        if not self.is_running:
            return

        self.is_running = False

        if not complete:
            await self._monitor.log(
                "Interrupt signal received. Initiating graceful shutdown...", status="INTERRUPT"
            )

        if self.task_creator:
            self.task_creator.cancel()
        if self.autosave_task:
            self.autosave_task.cancel()

        for worker in self.workers:
            if worker and not worker.done():
                worker.cancel()

        # Inject Poison Pill to terminate stream consumer
        if self._stream:
            with contextlib.suppress(asyncio.QueueFull):
                self._stream_queue.put_nowait((-1, bytearray()))

        async with self.condition:
            self.condition.notify_all()

        self._monitor.is_running = False

    async def stream_all(
        self,
        links: Iterable[str] | str,
        expected_checksums: dict[str, str] | None = None,
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
        """
        Orchestrates sequential streaming of multiple files.
        Manages the lifecycle of producers and workers, yielding a separate
        byte generator for each file.
        """

        self._stream = True
        if isinstance(links, str):
            links = [links]
        loop = asyncio.get_running_loop()
        # Graceful shutdown handler
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._stop()))

        self.task_creator = asyncio.create_task(self._add_task_producer(links, expected_checksums))
        self.workers = [asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)]

        try:
            for _ in links:
                if not self.is_running:
                    break

                filename = await self._file_discovery_queue.get()

                if filename is None:
                    continue

                file_gen = self._stream_one(filename)

                yield filename, file_gen
                async with self.condition:
                    await self.condition.wait_for(
                        lambda f=filename: f not in self.files or not self.is_running
                    )

        except asyncio.CancelledError:
            pass

        except Exception as e:
            # Catching unexpected runtime exceptions (e.g. disk full, OS errors)
            await self._monitor.log(f"Runtime Exception in run(): {e}", status="CRITICAL")
            raise

        finally:
            await self._stop(complete=True)

            await asyncio.gather(self.task_creator, *self.workers, return_exceptions=True)

            if self.is_running:
                await self._monitor.log(
                    "All downloads completed successfully!", status="SUCCESS", progress=True
                )

            await self.network.close()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def _stream_one(self, filename: str) -> AsyncGenerator[bytes]:
        """
        Internal generator that reorders downloaded chunks from the queue
        into a sequential byte stream.
        """
        self.current_file = filename

        # WAKE UP WORKERS: Tell them it's time to process this file!
        async with self.condition:
            self.condition.notify_all()

        file_obj = self.files[filename]
        total_size = file_obj.content_length
        expected_checksum = file_obj.expected_md5
        md5_hasher = hashlib.md5() if expected_checksum else None

        next_offset = 0
        await self._monitor.log(f"Streaming: {filename}", status="INFO")
        try:
            while next_offset < total_size:
                # 1. Если в куче уже есть следующий кусок - отдаем его
                if not self.is_running:
                    break

                # 1. Check if the next sequential chunk is already in the heap
                if self.heap and self.heap[0][0] == next_offset:
                    _, chunk_data = heapq.heappop(self.heap)
                    chunk_bytes = bytes(chunk_data)

                    # We freed up heap space, notify the producer
                    async with self.condition:
                        self.condition.notify()

                    if md5_hasher:
                        md5_hasher.update(chunk_bytes)

                    yield chunk_bytes

                    length = len(chunk_bytes)
                    next_offset += length
                    continue

                # 2. Wait for incoming chunks from workers
                chunk_start, chunk_data = await self._stream_queue.get()
                if chunk_start == -1:
                    break

                # 3. Process incoming chunk
                if chunk_start == next_offset:
                    chunk_bytes = bytes(chunk_data)

                    if md5_hasher:
                        md5_hasher.update(chunk_bytes)

                    yield chunk_bytes

                    length = len(chunk_bytes)
                    next_offset += length
                else:
                    # Chunk is out of order, store it in the heap
                    heapq.heappush(self.heap, (chunk_start, chunk_data))
            else:
                await self._monitor.done(filename)

                if md5_hasher and expected_checksum:
                    try:
                        self.storage.verify_stream(md5_hasher, expected_checksum, next_offset, total_size)
                        if self._monitor:
                            await self._monitor.log("MD5 Verified", status="SUCCESS", progress=True)
                    except Exception as e:
                        await self._monitor.log(str(e), status="ERROR")
                        raise

        finally:
            # Cleanup and notify stream_all that we are done
            del self.files[filename]

    async def run(
        self,
        links: str | Iterable[str],
        expected_checksums: dict[str, str] | None = None,
    ) -> None:
        """
        Executes the download pipeline, writing data directly to disk.
        """
        self._stream = False
        if isinstance(links, str):
            links = [links]

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self._stop()))

        self.task_creator = asyncio.create_task(self._add_task_producer(links, expected_checksums))
        self.autosave_task = asyncio.create_task(self._autosave())
        self.workers = [asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)]

        try:
            await self.task_creator
            async with self.condition:
                await self.condition.wait_for(lambda: not (self.files and self.is_running))

            if self.is_running:
                await self._monitor.log(
                    "All downloads completed successfully!", status="SUCCESS", progress=True
                )

        except asyncio.CancelledError:
            pass

        except Exception as e:
            # Catching unexpected runtime exceptions (e.g. disk full, OS errors)
            await self._monitor.log(f"Runtime Exception in run(): {e}", status="CRITICAL")
            raise

        finally:
            await self._stop(complete=True)

            await asyncio.gather(self.task_creator, self.autosave_task, *self.workers, return_exceptions=True)

            await self.storage.save_all_states(self.files)
            await self.network.close()

            for file_obj in self.files.values():
                file_obj.close_fd()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def start(self) -> Self:
        await self._monitor.start()
        return self

    async def stop(self) -> None:
        await self._stop()
        await self._monitor.stop()

    async def __aenter__(self) -> Self:
        await self._monitor.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        await self._monitor.stop()
