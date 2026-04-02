# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
import typing
import weakref
from collections import defaultdict
from dataclasses import InitVar, dataclass, field, replace
from pathlib import Path
from typing import (
    Any,
    Literal,
    Self,
    TypedDict,
    TypeVar,
    cast,
    dataclass_transform,
)

import orjson
from aiolimiter import AsyncLimiter
from curl_cffi import (
    AsyncSession,
    BrowserTypeLiteral,
    CurlHttpVersion,
    CurlOpt,
    Headers,
    HeaderTypes,
    Response,
)
from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress,
    TaskID,
)

from .interfaces import StorageBackend

TypeHash = Literal[
    "md5",
    "sha1",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "blake2b",
    "blake2s",
    "sha3_224",
    "sha3_256",
    "sha3_384",
    "sha3_512",
    "shake_128",
    "shake_256",
    "new",
    "algorithms_guaranteed",
    "algorithms_available",
    "pbkdf2_hmac",
    "file_digest",
]


class BaseSessionParams(TypedDict, total=False):
    headers: HeaderTypes | None
    verify: bool
    timeout: float | tuple[float, float]
    allow_redirects: bool
    max_redirects: int
    impersonate: BrowserTypeLiteral | None
    default_headers: bool
    http_version: CurlHttpVersion | str | None
    curl_options: dict[CurlOpt, int] | None


DEFAULT_OPTIONS: BaseSessionParams = {
    "impersonate": "chrome120",
    "timeout": 30.0,
}


_T = TypeVar("_T")


@dataclass_transform(kw_only_default=True)
def entity(cls: type[_T]) -> type[_T]:
    return dataclass(slots=True, kw_only=True, weakref_slot=True)(cls)


@dataclass_transform(kw_only_default=True)
def ordered_entity(cls: type[_T]) -> type[_T]:
    return dataclass(slots=True, kw_only=True, order=True, weakref_slot=True)(cls)


@dataclass_transform(kw_only_default=True, frozen_default=True)
def value_object(cls: type[_T]) -> type[_T]:
    return dataclass(slots=True, kw_only=True, frozen=True, weakref_slot=True)(cls)


@ordered_entity
class Chunk:
    current_pos: int
    start: int = field(compare=False)
    end: int = field(compare=False)
    _file_ref: weakref.ReferenceType["File"] = field(repr=False, compare=False)

    @property
    def file(self) -> "File":
        obj = self._file_ref()
        if obj is None:
            raise RuntimeError("File object was already garbage collected")
        return obj

    @property
    def is_finished(self) -> bool:
        return self.current_pos > self.end

    @property
    def size(self) -> int:
        return self.end - self.start + 1

    @property
    def uploaded(self) -> int:
        return self.current_pos - self.start + 1

    @property
    def remaining(self) -> int:
        return max(0, self.end - self.current_pos + 1)

    @property
    def get_header(self) -> dict[str, str]:
        return {"Range": f"bytes={self.current_pos}-{self.end}"}


@value_object
class Checksum:
    algorithm: TypeHash  # "md5", "sha256", "sha1"
    value: str


@value_object
class FileMeta:
    id: int
    filename: str = field(compare=False)
    url: str = field(compare=False)
    content_length: int = field(compare=False)
    expected_checksum: Checksum | None = field(default=None, compare=False)
    supports_ranges: bool = field(compare=False)


@entity
class File:
    meta: FileMeta
    chunk_size: int = field(compare=False)
    chunks: list[Chunk] = field(default_factory=list[Chunk], compare=False)
    fd: int | None = field(default=None, repr=False, compare=False)
    verified: bool = field(default=False, compare=False)
    is_failed: bool = field(default=False, compare=False)

    def create_chunks(self) -> None:
        if self.chunks:
            return
        if not self.meta.supports_ranges or self.meta.content_length <= 0:
            self.chunks.append(
                Chunk(
                    start=0,
                    end=sys.maxsize,
                    current_pos=0,
                    _file_ref=weakref.ref(self),
                )
            )
            return
        if self.chunk_size <= 0:
            raise ValueError(f"Chunk size must be positive, got {self.chunk_size}")
        part_count = -(-self.meta.content_length // self.chunk_size)

        for i in range(part_count):
            start = i * self.chunk_size
            end = min((i + 1) * self.chunk_size - 1, self.meta.content_length - 1)

            self.chunks.append(
                Chunk(
                    start=start,
                    end=end,
                    current_pos=start,
                    _file_ref=weakref.ref(self),
                )
            )

    @property
    def is_complete(self) -> bool:
        if not self.chunks:
            return False
        return all(c.is_finished for c in self.chunks)

    @property
    def downloaded_size(self) -> int:
        return sum(c.current_pos - c.start for c in (self.chunks or []))

    @property
    def progress(self) -> float:
        if self.meta.content_length <= 0:
            return 0.0
        return (self.downloaded_size / self.meta.content_length) * 100

    def to_json(self) -> bytes:
        clear_file = replace(self, fd=None)
        return orjson.dumps(
            clear_file, option=orjson.OPT_SERIALIZE_DATACLASS | orjson.OPT_INDENT_2
        )

    @classmethod
    def from_json(cls, content: bytes) -> Self:
        data = orjson.loads(content)
        data["chunks"] = [Chunk(**c_data) for c_data in data.get("chunks", [])]

        if "meta" in data and isinstance(data["meta"], dict):
            data["meta"] = FileMeta(**data["meta"])

        return cls(**data)


@entity
class UIState:
    log_file: Path
    no_ui: bool = False
    quiet: bool = False
    json_logs: bool = False
    speed_limit: float | None = None

    is_running: bool = True
    console: Console = Console(stderr=True)
    limit_event: asyncio.Event = field(init=False)
    frequency_speed_limit: int = 100
    time_speed_limit: float = field(init=False)

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

    log_fd: typing.TextIO | None = field(default=None, init=False, repr=False)
    log_queue: asyncio.Queue[str | None] = field(
        default_factory=asyncio.Queue[str | None], init=False
    )
    log_task: asyncio.Task[None] | None = field(default=None, init=False)

    refresh: asyncio.Task[None] | None = None
    progress: Progress | None = None

    live: Live | None = None

    def __post_init__(self) -> None:
        self.renewal_rate = 1 / self.refresh_per_second

        if self.speed_limit:
            self.speed_limit = (self.speed_limit / self.frequency_speed_limit) * 1024**2
            self.time_speed_limit = 1 / self.frequency_speed_limit

        self.limit_event = asyncio.Event()
        self.limit_event.set()


@entity
class StorageState:
    out_dir: Path
    ui: UIState
    is_running: bool = True
    files: dict[int, File] = field(default_factory=dict[int, File])
    state_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        self.out_dir = Path(self.out_dir).expanduser().resolve()
        self.state_dir = self.out_dir / ".states"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)


@entity
class AMIDState:
    initial_rps: int
    min_rps: int = 1
    cooldown_seconds: int = 30
    break_duration: int = 300

    monitor: UIState
    lock: asyncio.Lock = field(default=asyncio.Lock())
    limiter: AsyncLimiter = field(init=False)

    current_rps: int = field(init=False)
    max_rps: int = field(init=False)

    last_429_time: float = 0.0
    circuit_broken_until: float = 0.0

    def __post_init__(self) -> None:
        self.current_rps: int = self.initial_rps
        self.max_rps: int = self.initial_rps
        self.limiter = AsyncLimiter(self.initial_rps, 1)


@entity
class NetworkState:
    threads: int
    monitor: UIState
    client_kwargs: dict[str, Any] | None = None
    max_retries: int = 3

    client: AsyncSession[Response] = field(init=False)
    rate_limiter: AMIDState = field(init=False)

    def __post_init__(self) -> None:
        self.rate_limiter = AMIDState(
            initial_rps=self.threads * 2, monitor=self.monitor
        )

        options = cast(
            dict[str, Any], {**DEFAULT_OPTIONS, **(self.client_kwargs or {})}
        )
        user_headers = options.pop("headers", None)
        headers_obj = Headers(user_headers)
        headers_obj.setdefault("Accept-Encoding", "identity")
        headers_obj.setdefault("Connection", "keep-alive")

        self.client = AsyncSession(
            max_clients=self.threads,
            **options,
        )


@value_object
class HydraConfig:
    threads: int = 1
    no_ui: bool = False
    quiet: bool = False
    out_dir: str = "download"
    speed_limit: float | None = None
    dry_run: bool = False
    json_logs: bool = False
    verify: bool = True

    min_chunk_size_mb: InitVar[int] = 1
    min_stream_chunk_size_mb: InitVar[int] = 5
    stream_buffer_size_mb: InitVar[int | None] = None
    client_kwargs: dict[str, Any] | None = None

    MIN_CHUNK: int = field(init=False)  # 1MB
    STREAM_CHUNK_SIZE: int = field(init=False)  # 5MB
    STREAM_BUFFER_SIZE: int | None = None

    def __post_init__(
        self,
        min_chunk_size_mb: int,
        min_stream_chunk_size_mb: int,
        stream_buffer_size_mb: int | None,
    ) -> None:
        object.__setattr__(self, "MIN_CHUNK", min_chunk_size_mb * 1024 * 1024)
        object.__setattr__(
            self, "STREAM_CHUNK_SIZE", min_stream_chunk_size_mb * 1024 * 1024
        )
        if stream_buffer_size_mb:
            object.__setattr__(
                self, "STREAM_BUFFER_SIZE", stream_buffer_size_mb * 1024 * 1024
            )


@entity
class HydraContext:
    config: HydraConfig

    is_running: bool = True
    stream: bool = False
    current_file_id: list[int] = field(default_factory=list[int])
    next_offset: int = 0

    net: NetworkState = field(init=False)
    ui: UIState = field(init=False)
    fs: StorageBackend

    heap_size: int = field(init=False)

    files: dict[int, File] = field(default_factory=dict[int, File])
    heap: list[tuple[int, bytearray]] = field(
        default_factory=list[tuple[int, bytearray]]
    )
    links_queue: asyncio.PriorityQueue[tuple[int, str, Checksum | None]] = field(
        init=False
    )
    file_discovery_queue: asyncio.Queue[int] = field(init=False)
    chunk_queue: asyncio.PriorityQueue[tuple[int, Chunk] | tuple[int, Chunk]] = field(
        init=False
    )
    stream_queue: asyncio.Queue[tuple[int, bytearray]] = field(init=False)
    condition: asyncio.Condition = field(init=False)

    task_creators: list[asyncio.Task[None]] | None = None
    dispatcher: asyncio.Task[None] | None = None
    workers: list[asyncio.Task[None]] | None = None
    autosave_task: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        self.links_queue = asyncio.PriorityQueue()
        self.chunk_queue = asyncio.PriorityQueue()
        self.file_discovery_queue = asyncio.Queue()
        self.stream_queue = asyncio.Queue()
        self.condition = asyncio.Condition()

        maxsize = (
            self.config.STREAM_BUFFER_SIZE // self.config.MIN_CHUNK
            if self.config.STREAM_BUFFER_SIZE
            else 0
        )
        maxsize = (
            maxsize if maxsize > self.config.threads * 2 else self.config.threads * 2
        )
        self.heap_size = maxsize
        self.ui = UIState(
            is_running=self.is_running,
            no_ui=self.config.no_ui,
            quiet=self.config.quiet,
            json_logs=self.config.json_logs,
            speed_limit=self.config.speed_limit,
            log_file=Path(self.config.out_dir) / "download.log",
        )

        self.net = NetworkState(
            threads=self.config.threads,
            monitor=self.ui,
            client_kwargs=self.config.client_kwargs,
        )
