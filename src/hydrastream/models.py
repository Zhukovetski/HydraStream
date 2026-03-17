# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import math
import os
import ssl
from collections import defaultdict
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Self, TypedDict, TypeVar, cast, dataclass_transform

import httpx
import orjson
from aiolimiter import AsyncLimiter
from httpx._types import (
    AuthTypes,
    CookieTypes,
    HeaderTypes,
    ProxyTypes,
    TimeoutTypes,
)
from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress,
    TaskID,
)


class HttpxClientOptions(TypedDict, total=False):
    headers: HeaderTypes | None
    cookies: CookieTypes | None
    auth: AuthTypes | None
    proxy: ProxyTypes | None
    timeout: TimeoutTypes
    verify: ssl.SSLContext | str | bool
    follow_redirects: bool
    http2: bool
    http1: bool


DEFAULT_OPTIONS: HttpxClientOptions = {
    "timeout": httpx.Timeout(10.0, read=5.0),
    "http2": True,
    "follow_redirects": True,
}

_T = TypeVar("_T")


@dataclass_transform(kw_only_default=True)
def entity(cls: type[_T]) -> type[_T]:
    """
    **Mutable Entity.**
    Best for objects with identity that change over time (e.g., Chunk progress).
    - Uses `slots=True` for memory efficiency.
    - Uses `kw_only=True` for explicit initialization.
    """
    return dataclass(slots=True, kw_only=True)(cls)


@dataclass_transform(kw_only_default=True)
def ordered_entity(cls: type[_T]) -> type[_T]:
    return dataclass(slots=True, kw_only=True, order=True)(cls)


@dataclass_transform(kw_only_default=True, frozen_default=True)
def value_object(cls: type[_T]) -> type[_T]:
    """
    **Immutable Value Object.**
    Best for data that shouldn't change once created (e.g., File Metadata, Config).
    - Uses `slots=True` and `frozen=True`.
    - Hashable by default (can be used as dict keys).
    """
    return dataclass(slots=True, kw_only=True, frozen=True)(cls)


@ordered_entity
class Chunk:
    """
    Represents a specific byte range (chunk) of a remote file for downloading.

    Attributes:
        filename (str): The name of the parent file. Excluded from comparisons.
        current_pos (int): The current byte offset being downloaded.
                           Used as the primary sorting key in PriorityQueue.
        start (int): The starting byte index of the chunk. Excluded from comparisons.
        end (int): The ending byte index of the chunk. Excluded from comparisons.
    """

    filename: str = field(compare=False)
    current_pos: int
    start: int = field(compare=False)
    end: int = field(compare=False)

    @property
    def is_finished(self) -> bool:
        """Checks if the chunk has been completely downloaded."""
        return self.current_pos > self.end

    @property
    def size(self) -> int:
        """Returns the total allocated size of the chunk in bytes."""
        return self.end - self.start + 1

    @property
    def uploaded(self) -> int:
        """Returns the number of bytes successfully downloaded so far."""
        return self.current_pos - self.start + 1

    @property
    def remaining(self) -> int:
        """Returns the number of bytes left to download in this chunk."""
        return max(0, self.end - self.current_pos + 1)

    def get_header(self) -> dict[str, str]:
        """
        Generates the HTTP Range header required to fetch the remaining data.

        Returns:
            dict[str, str]: A dictionary containing the 'Range' header.
        """
        return {"Range": f"bytes={self.current_pos}-{self.end}"}


@value_object
class FileMeta:
    filename: str
    url: str
    content_length: int
    expected_md5: str | None


@entity
class File:
    meta: FileMeta
    chunk_size: int
    chunks: list[Chunk] = field(default_factory=list[Chunk])
    fd: int | None = field(default=None, repr=False, compare=False)
    verified: bool = False
    is_failed: bool = False
    """
    Represents a remote file, managing its chunk geometry, download state,
    and system resources (file descriptors).

    Attributes:
        filename (str): The local name of the file.
        url (str): The remote source URL.
        content_length (int): Total size of the file in bytes.
        chunk_size (int): Target size for each individual chunk.
        chunks (list[Chunk]): List of chunk objects representing the file's layout.
        expected_md5 (str | None): The expected MD5 hash for integrity validation.
        verified (bool): Flag indicating if the file has passed integrity checks.
        fd (int | None): The OS file descriptor for atomic writes.
                         Excluded from serialization and representation.
    """

    def __post_init__(self) -> None:
        """
        Calculates and generates the chunk layout if it hasn't been provided
        (e.g., during initialization from scratch, not deserialization).

        Raises:
            ValueError: If the defined chunk_size is zero or negative.
        """
        if self.chunks:
            return

        if self.chunk_size <= 0:
            raise ValueError(f"Chunk size must be positive, got {self.chunk_size}")

        # Fast ceiling division trick without importing the math module
        part_count = -(-self.meta.content_length // self.chunk_size)

        for i in range(part_count):
            start = i * self.chunk_size
            end = min((i + 1) * self.chunk_size - 1, self.meta.content_length - 1)

            self.chunks.append(
                Chunk(
                    start=start,
                    end=end,
                    current_pos=start,
                    filename=self.meta.filename,
                )
            )

    @property
    def is_complete(self) -> bool:
        """Checks if all chunks associated with this file are fully downloaded."""
        if not self.chunks:
            return False
        return all(c.is_finished for c in self.chunks)

    @property
    def downloaded_size(self) -> int:
        """Calculates the total number of bytes written to disk or stream."""
        return sum(c.current_pos - c.start for c in (self.chunks or []))

    @property
    def progress(self) -> float:
        """Calculates the overall download progress percentage (0.0 to 100.0)."""
        if self.meta.content_length <= 0:
            return 0.0
        return (self.downloaded_size / self.meta.content_length) * 100

    def to_json(self) -> bytes:
        """
        Serializes the File object and its nested Chunks into a JSON byte string.

        Returns:
            bytes: JSON representation of the file state.
        """
        clear_file = replace(self, fd=None)
        return orjson.dumps(
            clear_file, option=orjson.OPT_SERIALIZE_DATACLASS | orjson.OPT_INDENT_2
        )

    @classmethod
    def from_json(cls, content: bytes) -> Self:
        """
        Deserializes a JSON byte string back into a File object,
        reconstructing the nested Chunk objects.

        Args:
            content (bytes): The JSON data.

        Returns:
            File: An instantiated File object.

        Raises:
            orjson.JSONDecodeError: If the content is malformed or invalid JSON.
            TypeError: If the parsed JSON dictionary is missing required class
            attributes.
        """
        data = orjson.loads(content)
        # 1. Восстанавливаем чанки
        data["chunks"] = [Chunk(**c_data) for c_data in data.get("chunks", [])]

        # 2. ВОССТАНАВЛИВАЕМ META!
        if "meta" in data and isinstance(data["meta"], dict):
            data["meta"] = FileMeta(**data["meta"])

        return cls(**data)

    def close_fd(self) -> None:
        """
        Safely closes the associated OS file descriptor if it is open.
        Suppresses OSErrors (e.g., if the descriptor was already closed externally).
        """
        if self.fd is not None:
            with contextlib.suppress(OSError):
                os.close(self.fd)
            self.fd = None


@entity
class StorageState:
    """
    Manages disk I/O operations, state persistence for resumable downloads,
    and data integrity validation (size and checksums).
    """

    """
    Initializes the storage manager and ensures necessary directories exist.

    Args:
        output_dir (str): The target directory for downloaded files.
    """
    out_dir: Path
    is_running: bool = True
    files: dict[str, File] = field(default_factory=dict[str, File])
    state_dir: Path = field(init=False)
    log_file: Path = field(init=False)

    def __post_init__(self) -> None:
        self.out_dir = Path(self.out_dir).expanduser().resolve()
        self.state_dir = self.out_dir / ".states"
        self.log_file = self.out_dir / "download.log"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)


@entity
class UIState:
    """
    Manages the terminal UI and file-based logging for the download session.

    Provides a declarative Rich UI with live updates, global ETA, and a
    fallback mechanism ( mode) for headless server environments.
    """

    no_ui: bool = False
    quiet: bool = False

    # Route UI to stderr to keep stdout clean for data streams (Unix pipes)
    is_running: bool = True
    console = Console(stderr=True)
    progress = None
    storage: StorageState

    start_time: float = 0.0
    total_bytes: int = 0
    download_bytes: int = 0
    total_files: int = 0
    files_completed: int = 0

    refresh_per_second = 10
    renewal_rate: float = field(init=False)
    dynamic_title: str = ""
    date_printed: bool = False

    tasks: dict[str, TaskID] = field(default_factory=dict[str, TaskID])
    log_throttle: dict[str, float] = field(default_factory=dict[str, float])
    buffer: defaultdict[str, int] = field(default_factory=lambda: defaultdict(int))
    active_files: set[str] = field(default_factory=set[str])

    refresh: asyncio.Task[None] = field(init=False)

    progress: Progress | None = field(init=False)
    live: Live | None = field(init=False)

    def __post_init__(self) -> None:
        self.renewal_rate = 1 / self.refresh_per_second


@entity
class AMIDState:
    """
    Advanced rate limiter implementing AIMD (Additive Increase/Multiplicative Decrease)
    and Circuit Breaker patterns to dynamically adapt to server constraints.

    - Multiplicative Decrease: Halves the allowed RPS upon encountering HTTP 429 errors.
    - Additive Increase: Slowly probes for higher RPS limits during successful requests.
    - Circuit Breaker: Halts all outgoing requests globally if the server demands a long
    pause.
    """

    initial_rps: int
    min_rps: int = 1
    cooldown_seconds: int = 30
    break_duration: int = 300

    monitor: UIState
    lock: asyncio.Lock = field(default=asyncio.Lock())
    limiter: AsyncLimiter = field(init=False)

    current_rps: int = field(init=False)
    max_rps: int = field(init=False)

    # Timestamp of the last 429 error
    last_429_time: float = 0.0
    circuit_broken_until: float = 0.0

    def __post_init__(self) -> None:
        self.current_rps: int = self.initial_rps
        self.max_rps: int = self.initial_rps
        self.limiter = AsyncLimiter(self.initial_rps, 1)


@entity
class NetworkState:
    """
    Asynchronous HTTP client wrapper providing rate limiting, connection pooling,
    and robust error handling with exponential backoff and full jitter.
    """

    threads: int
    monitor: UIState
    client_kwargs: dict[str, Any] | None = None
    max_retries: int = 3

    client: httpx.AsyncClient = field(init=False)
    rate_limiter: AMIDState = field(init=False)

    def __post_init__(self) -> None:
        self.rate_limiter = AMIDState(
            initial_rps=self.threads * 2, monitor=self.monitor
        )

        options = cast(
            dict[str, Any], {**DEFAULT_OPTIONS, **(self.client_kwargs or {})}
        )

        user_headers = options.pop("headers", None)
        headers_obj = httpx.Headers(user_headers)
        headers_obj.setdefault("Accept-Encoding", "identity")
        headers_obj.setdefault("User-Agent", "HydraStream/1.0")

        calc_limits = httpx.Limits(
            max_connections=math.ceil(self.threads * 1.1),
            max_keepalive_connections=self.threads,
            keepalive_expiry=4.5,
        )
        final_limits = options.pop("limits", calc_limits)

        self.client = httpx.AsyncClient(
            headers=headers_obj, limits=final_limits, **options
        )


@value_object
class HydraConfig:
    threads: int = 1
    no_ui: bool = False
    quiet: bool = False
    output_dir: str = "download"
    chunk_timeout: int = 120
    stream_buffer_size: int | None = None
    client_kwargs: dict[str, Any] | None = None


@entity
class HydraContext:
    """
    ЕДИНЫЙ ИСТОЧНИК ПРАВДЫ (Single Source of Truth).
    Тот самый ящик с инструментами, который мы передаем во все функции.
    """

    config: HydraConfig

    # Глобальные флаги
    is_running: bool = True
    stream: bool = False
    current_file: str = field(default="")

    # Вложенные состояния
    net: NetworkState = field(init=False)
    ui: UIState = field(init=False)
    fs: StorageState = field(init=False)

    MIN_CHUNK: int = 1 * 1024**2
    STREAM_CHUNK_SIZE: int = 5 * 1024**2
    heap_size: int = field(init=False)

    # Глобальные данные
    files: dict[str, File] = field(default_factory=dict[str, File])
    heap: list[tuple[int, bytearray]] = field(
        default_factory=list[tuple[int, bytearray]]
    )

    # Очереди (Трубы)
    chunk_queue: asyncio.PriorityQueue[tuple[int, Chunk]] = field(init=False)
    stream_queue: asyncio.Queue[tuple[int, bytearray]] = field(init=False)
    file_discovery_queue: asyncio.Queue[str | None] = field(init=False)
    condition: asyncio.Condition = field(init=False)

    task_creator: asyncio.Task[None] = field(init=False)
    workers: list[asyncio.Task[None]] = field(init=False)
    autosave_task: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        self.chunk_queue = asyncio.PriorityQueue()
        self.stream_queue = asyncio.Queue()
        self.file_discovery_queue = asyncio.Queue()
        self.condition = asyncio.Condition()

        maxsize = (
            self.config.stream_buffer_size // self.MIN_CHUNK
            if self.config.stream_buffer_size
            else 0
        )
        maxsize = (
            maxsize if maxsize > self.config.threads * 2 else self.config.threads * 2
        )
        self.heap_size = maxsize

        self.fs = StorageState(out_dir=Path(self.config.output_dir), files=self.files)
        self.ui = UIState(
            is_running=self.is_running,
            no_ui=self.config.no_ui,
            quiet=self.config.quiet,
            storage=self.fs,
        )
        self.net = NetworkState(
            threads=self.config.threads,
            monitor=self.ui,
            client_kwargs=self.config.client_kwargs,
        )
