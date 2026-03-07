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
from typing import Self

import httpx
from aiolimiter import AsyncLimiter

from .models import Chunk, File
from .monitor import ProgressMonitor
from .network import NetworkClient
from .providers import NCBIProvider
from .storage import StorageManager


class NCBILoader:
    """
    High-performance asynchronous manager for downloading and streaming genomics data.

    Implements concurrent chunk downloading, in-memory reordering for sequential streams,
    atomic disk writes, and automatic MD5 integrity verification.
    """

    _queue: asyncio.Queue[tuple[int, Chunk]]
    _stream_queue: asyncio.Queue[tuple[int, bytearray]]
    workers: list[asyncio.Task[None]]

    def __init__(
        self,
        threads: int = 3,
        silent: bool = False,
        output_dir: str = "download",
        stream_buffer_size: int | None = None,
        timeout: int | float = 10.0,
        chunk_timeout: int | float = 60,
        follow_redirects: bool = True,
        http2: bool = False,
        verify: bool = False,
    ) -> None:
        self._monitor = ProgressMonitor(silent=silent, log_file=f"{output_dir}/session.log")
        self.network = NetworkClient(threads=threads, http2=http2, monitor=self._monitor)
        self.storage = StorageManager(output_dir=output_dir)
        self.ncbi = NCBIProvider(network=self.network)
        self.providers = NCBIProvider(self.network)

        self._max_conns = threads
        self._stream = None
        self._stream_buffer_size = stream_buffer_size
        self.stream_chunk_size = 5 * 1024 * 1024
        self.MIN_CHUNK = 1024 * 1024
        self.chunk_timeout = chunk_timeout
        self._limiter = AsyncLimiter(threads * 2, 1)
        self._semaphore = asyncio.Semaphore(threads)
        self._timeout = httpx.Timeout(timeout, read=5.0)

        self._queue = asyncio.PriorityQueue()
        maxsize = stream_buffer_size // self.MIN_CHUNK if stream_buffer_size else 0
        maxsize = maxsize if maxsize > self._max_conns else self._max_conns
        self._stream_queue = asyncio.Queue(maxsize)
        self.files: dict[str, File] = {}
        self.heap: list[tuple[int, bytearray]] = []
        self.condition = asyncio.Condition()
        self.is_running = True
        self.workers = []
        self.task_creator = None
        self.autosave_task = None
        self.current_file = ""

    async def _autosave(self) -> None:
        while self.is_running:
            try:
                await asyncio.sleep(60)
                self.storage.save_all_states(self.files)
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
                filename = url.rsplit("/", 1)[1]
                file = None
                total_size = 0

                # Fetch metadata
                response = await self.network.safe_request("Head", url=url)

                if response is None:
                    self._monitor.log(f"Skipping {filename} due to unreachable remote.", status="ERROR")
                    continue

                total_size = int(response.headers.get("content-length", 0))

                if total_size <= 0:
                    self._monitor.log(f"Invalid Content-Length ({total_size}) for {filename}", status="ERROR")
                    continue

                # Auto-fetch MD5 if missing
                if not md5_val:
                    self._monitor.add_file(filename)
                    md5_val = await self.providers.get_expected_hash(url, filename)
                    self._monitor.done(filename)

                # 1. State Recovery
                if not self._stream:
                    file = self.storage.load_state(filename=filename)

                # 2. Chunk geometry calculation
                parts = self._max_conns
                chunk_size = max(total_size // parts, self.MIN_CHUNK)
                if self._stream and chunk_size > self.stream_chunk_size:
                    chunk_size = self.stream_chunk_size

                # 3. File object creation
                if not file:
                    if not self._stream:
                        self.storage.create_sparse_file(filename=filename, size=total_size)

                    file = File(
                        filename=filename,
                        url=url,
                        content_length=total_size,
                        chunk_size=chunk_size,
                        expected_md5=md5_val,
                    )

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

                # 4. Dispatch tasks to queue
                for c in chunks:
                    if not self.is_running:
                        break
                    if c.current_pos <= c.end:
                        await self._queue.put((i, c))

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._monitor.log(f"Failed to process URL {url}: {e}", status="ERROR")
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
                            self._monitor.log(f"Verifying MD5 checksum for {filename}...", status="INFO")
                            try:
                                self.storage.verify_file_hash(file_obj)
                                self._monitor.log(f"Integrity confirmed: {filename}", status="SUCCESS")
                            except ValueError as ve:
                                self._monitor.log(str(ve), status="ERROR")
                                # Optional: Add logic to quarantine/delete corrupted files here

                        file_obj.close_fd()
                        self.storage.verify_size(file_obj)
                        self.storage.delete_state(filename)
                        self._monitor.done(filename)
                        self._queue.task_done()
                        del self.files[filename]

                        if not self.files:
                            async with self.condition:
                                self.condition.notify_all()
                # ------------------------------------
            except asyncio.CancelledError:
                self._queue.task_done()
                break  # Worker shutdown requested
            except Exception:
                if self.is_running and chunk:
                    # Exponential or fixed backoff for chunk retry
                    await asyncio.sleep(random.uniform(0, 2))
                    # Re-queue with high priority (-1)
                    await self._queue.put((-1, chunk))
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
                    self.files[chunk.filename].url, headers=headers, timeout=60
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
                        if len(self.heap) > 20:
                            async with self.condition:
                                await self.condition.wait_for(lambda: len(self.heap) <= 20)
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
            self._monitor.log(
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
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        self.task_creator = asyncio.create_task(self._add_task_producer(links, expected_checksums))
        self.workers = [asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)]

        try:
            for url in links:
                if not self.is_running:
                    break
                filename = url.rsplit("/", 1)[1]

                # Wait for the producer to discover and register the file
                while filename not in self.files:
                    if self.task_creator.done():
                        break
                    await asyncio.sleep(0.1)

                if filename not in self.files:
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
            self._monitor.log(f"Runtime Exception in run(): {e}", status="CRITICAL")
            raise

        finally:
            await self._stop(complete=True)

            await asyncio.gather(self.task_creator, *self.workers, return_exceptions=True)

            if self.is_running:
                self._monitor.log("All downloads completed successfully!", status="SUCCESS", progress=True)

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
        self._monitor.log(f"Streaming: {filename}", status="INFO")
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

                    lenght = len(chunk_bytes)
                    next_offset += lenght
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

                    lenght = len(chunk_bytes)
                    next_offset += lenght
                else:
                    # Chunk is out of order, store it in the heap
                    heapq.heappush(self.heap, (chunk_start, chunk_data))
            else:
                self._monitor.done(filename)

                if md5_hasher and expected_checksum:
                    try:
                        self.storage.verify_stream(md5_hasher, expected_checksum, next_offset, total_size)
                        if self._monitor:
                            self._monitor.log("MD5 Verified", status="SUCCESS", progress=True)
                    except Exception as e:
                        self._monitor.log(str(e), status="ERROR")
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
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        self.task_creator = asyncio.create_task(self._add_task_producer(links, expected_checksums))
        self.autosave_task = asyncio.create_task(self._autosave())
        self.workers = [asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)]

        try:
            await self.task_creator
            async with self.condition:
                await self.condition.wait_for(lambda: not (self.files and self.is_running))

            if self.is_running:
                self._monitor.log("All downloads completed successfully!", status="SUCCESS", progress=True)

        except asyncio.CancelledError:
            pass

        except Exception as e:
            # Catching unexpected runtime exceptions (e.g. disk full, OS errors)
            self._monitor.log(f"Runtime Exception in run(): {e}", status="CRITICAL")
            raise

        finally:
            await self._stop(complete=True)

            await asyncio.gather(self.task_creator, self.autosave_task, *self.workers, return_exceptions=True)

            self.storage.save_all_states(self.files)
            await self.network.close()

            for file_obj in self.files.values():
                file_obj.close_fd()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def start(self) -> Self:
        self._monitor.start()
        return self

    async def stop(self) -> None:
        await self._stop()
        self._monitor.stop()

    async def __aenter__(self) -> Self:
        self._monitor.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:

        self._monitor.stop()
