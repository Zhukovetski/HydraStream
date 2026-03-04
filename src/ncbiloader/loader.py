# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import hashlib
import heapq
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

        self.network = NetworkClient(threads=threads, http2=http2)
        self.storage = StorageManager(output_dir=output_dir)
        self.ncbi = NCBIProvider(network=self.network)
        self.providers = NCBIProvider(self.network)

        self._max_conns = threads
        self._stream = None
        self._stream_buffer_size = stream_buffer_size
        self.stream_chunk_size = 5 * 1024 * 1024
        self.MIN_CHUNK = 1024 * 1024  # 1 MB минимум
        self.chunk_timeout = chunk_timeout
        self._limiter = AsyncLimiter(threads * 2, 1)
        self._semaphore = asyncio.Semaphore(threads)
        self._timeout = httpx.Timeout(timeout, read=5.0)

        self._monitor = ProgressMonitor(silent=silent, log_file=f"{output_dir}/session.log")
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
        self.prev_file = ""

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
        """Продюсер: Нарезает файлы и кидает в очередь"""
        checksums_map = expected_checksums if isinstance(expected_checksums, dict) else {}
        for url in links:
            if not self.is_running:
                break

            md5_val = checksums_map.get(url)

            try:
                filename = url.rsplit("/", 1)[1]
                file = None
                total_size = 0
                response = await self.network.safe_request("Head", url=url)

                if response is None:
                    # Если мы исчерпали 3 попытки ИЛИ словили 404, идем к следующему URL
                    continue

                total_size = int(response.headers.get("content-length", 0))

                if total_size <= 0:
                    raise ValueError(f"content-length: {total_size}")

                if not md5_val:
                    md5_val = await self.providers.get_expected_hash(url, filename)

                # 1. Попытка восстановления
                if not self._stream:
                    file = self.storage.load_state(filename=filename)

                # Логика нарезки
                parts = self._max_conns
                chunk_size = max(total_size // parts, self.MIN_CHUNK)

                if self._stream and chunk_size > self.stream_chunk_size:
                    chunk_size = self.stream_chunk_size

                # 2. Если не восстановили - создаем с нуля

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

                # 3. Регистрируем в UI и памяти
                self.files[filename] = file
                chunks = file.chunks or []

                self._monitor.add_file(filename, total_size)
                if not self._stream:
                    downloaded = sum(c.uploaded for c in chunks)
                    if downloaded - len(chunks) > 0:
                        self._monitor.update(filename, downloaded)

                # 4. Кидаем в очередь (самое важное)
                for c in chunks:
                    if not self.is_running:
                        break
                    if c.current_pos <= c.end:
                        await self._queue.put((10, c))

            except Exception as e:
                self._monitor.log(f"[red]Ошибка обработки {url}: {e}[/]")
                raise

    async def _download_worker(self) -> None:
        while self.is_running:
            chunk = None
            try:
                _, chunk = await self._queue.get()
                if self._stream and self.current_file != chunk.filename:
                    async with self.condition:
                        await self.condition.wait_for(lambda: self.current_file not in self.files)
                await self._process_chunk(chunk)
                # --- ЛОГИКА МОМЕНТАЛЬНОЙ ПРОВЕРКИ ---
                if not self._stream:  # Только для режима RUN (диск)
                    filename = chunk.filename
                    file_obj = self.files.get(filename)
                    # Если файл готов И у него есть хеш для проверки И мы его еще не проверяли
                    if file_obj and file_obj.is_complete:
                        if file_obj.expected_md5 and not file_obj.verified:
                            # Ставим флаг, чтобы другой воркер не запустил проверку повторно
                            file_obj.verified = True
                            # Запускаем проверку в фоне!
                            self._monitor.log(f"[*] Проверка MD5 для {filename}...", True)
                            self.storage.verify_file_hash(file_obj)
                            self._monitor.log(f"[green]MD5 Verified: {filename}[/]", True)

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
                break
            except Exception:
                if self.is_running and chunk:
                    # Мягкий ретрай
                    await asyncio.sleep(2)
                    await self._queue.put((1, chunk))
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
                        buffer_size = 1024 * 1024  # 1 MB
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

    async def stop(self, complete: bool = False) -> None:
        """Этот метод вызывается по Ctrl+C"""

        if not self.is_running:
            return  # Защита от двойного вызова
        self.is_running = False
        if not complete:
            self._monitor.log("[yellow]Получен сигнал остановки...[/]", progress=True)
        # 1. Останавливаем Продюсеров (чтобы не кидали новое)
        if self.task_creator:
            self.task_creator.cancel()
        if self.autosave_task:
            self.autosave_task.cancel()

        # 2. Отменяем Воркеров (они выйдут из while и вызовут task_done для своих задач)
        for worker in self.workers:
            if worker and not worker.done():
                worker.cancel()

        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

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

        self._stream = True
        if isinstance(links, str):
            links = [links]
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        self.task_creator = asyncio.create_task(self._add_task_producer(links, expected_checksums))
        self.workers = [asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)]

        try:
            # 2. MAIN LOOP (Обработка файлов по очереди)
            # Мы не итерируемся по links здесь, потому что producer уже накидал их в File state
            # Нам нужно знать порядок. Либо producer сохраняет порядок в отдельный список,
            # либо мы итерируемся по links снова.
            for url in links:
                if not self.is_running:
                    break
                filename = url.rsplit("/", 1)[1]

                # Ждем, пока файл появится в системе (Producer его обработает)
                # Тут нужна синхронизация, если Producer отстает.
                while filename not in self.files:
                    if self.task_creator.done():
                        # Если продюсер умер, а файла нет - беда
                        break
                    await asyncio.sleep(0.1)

                if filename not in self.files:
                    continue

                # Создаем внутренний генератор для ОДНОГО файла
                # Он предполагает, что воркеры уже работают
                file_gen = self._stream_one(filename)

                yield filename, file_gen
                async with self.condition:
                    await self.condition.wait_for(
                        lambda f=filename: f not in self.files or not self.is_running
                    )

        except asyncio.CancelledError:
            pass

        finally:
            # 3. TEARDOWN (Очистка)
            if self.is_running:
                self._monitor.log("[green]Все загрузки завершены![/]")
            await self.stop(complete=True)  # Флаг вниз

            if self.task_creator:
                self.task_creator.cancel()

            for w in self.workers:
                w.cancel()

            # Ждем пока все умрут
            await asyncio.gather(*self.workers, return_exceptions=True)
            if self.task_creator:
                await asyncio.gather(self.task_creator, return_exceptions=True)

            # Чистим очереди
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()

            # Закрываем клиент
            await self.network.close()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def _stream_one(self, filename: str) -> AsyncGenerator[bytes]:
        self.current_file = filename
        file_obj = self.files[filename]
        total_size = file_obj.content_length
        expected_checksum = file_obj.expected_md5
        md5_hasher = hashlib.md5() if expected_checksum else None
        next_offset = 0
        self._monitor.log(f"Streaming: {filename}")
        try:
            while next_offset < total_size:
                # 1. Если в куче уже есть следующий кусок - отдаем его
                if not self.is_running:
                    break
                if self.heap and self.heap[0][0] == next_offset:
                    _, chunk_data = heapq.heappop(self.heap)
                    chunk_bytes = bytes(chunk_data)

                    async with self.condition:
                        self.condition.notify()

                    if md5_hasher:
                        md5_hasher.update(chunk_bytes)

                    yield chunk_bytes

                    lenght = len(chunk_bytes)
                    next_offset += lenght
                    continue

                # 2. Если куча пуста или там лежат "далекие" куски - ждем новых
                chunk_start, chunk_data = await self._stream_queue.get()
                if chunk_start == -1:
                    break

                # 3. Пришел кусок. Это тот, который мы ждем?
                if chunk_start == next_offset:
                    chunk_bytes = bytes(chunk_data)

                    if md5_hasher:
                        md5_hasher.update(chunk_bytes)

                    yield chunk_bytes

                    lenght = len(chunk_bytes)
                    next_offset += lenght
                else:
                    # Это кусок из будущего. Сохраняем в кучу.
                    # (Внимание: тут может закончиться память, если "первый" кусок завис!)

                    heapq.heappush(self.heap, (chunk_start, chunk_data))
            else:
                self._monitor.done(filename)
                # ПРОВЕРКА ЦЕЛОСТНОСТИ
                if md5_hasher and expected_checksum:
                    try:
                        self.storage.verify_stream(md5_hasher, expected_checksum, next_offset, total_size)
                        if self._monitor:
                            self._monitor.log("[green]MD5 Verified[/]", True)
                    except Exception as e:
                        self._monitor.log(f"[bold red]{e}[/]")
                        raise

        finally:
            del self.files[filename]
            async with self.condition:
                self.condition.notify_all()
            await asyncio.sleep(1)

    async def run(
        self,
        links: str | Iterable[str],
        expected_checksums: dict[str, str] | None = None,
    ) -> None:
        self._stream = False
        if isinstance(links, str):
            links = [links]
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        # 1. Запускаем фоновые задачи
        self.task_creator = asyncio.create_task(self._add_task_producer(links, expected_checksums))
        self.autosave_task = asyncio.create_task(self._autosave())

        self.workers = [asyncio.create_task(self._download_worker()) for _ in range(self._max_conns)]

        try:
            await self.task_creator
            async with self.condition:
                await self.condition.wait_for(lambda: not (self.files and self.is_running))

            if self.is_running:
                self._monitor.log("[green]Все загрузки завершены![/]", progress=True)

        except asyncio.CancelledError:
            pass

        finally:
            await self.stop(complete=True)

            await asyncio.gather(self.task_creator, return_exceptions=True)

            # Финальное сохранение
            self.storage.save_all_states(self.files)
            await self.network.close()

            for file_obj in self.files.values():
                file_obj.close_fd()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

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
