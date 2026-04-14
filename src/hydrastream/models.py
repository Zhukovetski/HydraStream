# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

import asyncio
import contextlib
import sys
import typing
import weakref
from collections import defaultdict
from dataclasses import InitVar, dataclass, field, replace
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Self,
    TypedDict,
    TypeVar,
    cast,
    dataclass_transform,
    get_args,
)
from urllib.parse import urlparse

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

from hydrastream.exceptions import (
    InvalidChecksumError,
    OrphanedChunkError,
    ValidationError,
)

from .interfaces import HashProvider, StorageBackend

if TYPE_CHECKING:
    from hydrastream.providers import ProviderRouter

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
    _file_ref: weakref.ReferenceType[File] | None = field(
        repr=False, compare=False, default=None
    )

    @property
    def file(self) -> File:
        obj = self._file_ref() if self._file_ref else None
        if obj is None:
            # Выбрасываем структурированную ошибку вместо безликого RuntimeError
            raise OrphanedChunkError(start_pos=self.start, end_pos=self.end)
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


def validate_typehash(value: TypeHash) -> None:
    allowed_hashes = get_args(TypeHash)
    if value not in allowed_hashes:
        raise ValidationError(
            param="typehash",
            value=value,
            reason=f"Invalid hash algorithm. Supported: {', '.join(allowed_hashes)}",
        )


@value_object
class Checksum:
    algorithm: TypeHash
    value: str

    def __post_init__(self) -> None:
        validate_typehash(self.algorithm)
        normalized = self.value.strip().lower()
        object.__setattr__(self, "value", normalized)

        expected_lengths = {
            "md5": 32,
            "sha1": 40,
            "sha224": 56,
            "sha3_224": 56,
            "sha256": 64,
            "sha3_256": 64,
            "blake2s": 64,
            "sha384": 96,
            "sha3_384": 96,
            "sha512": 128,
            "sha3_512": 128,
            "blake2b": 128,
        }

        # Проверка на HEX
        if not all(c in "0123456789abcdef" for c in normalized):
            raise InvalidChecksumError(
                algorithm=self.algorithm,
                value=normalized,
                reason="Contains non-hex characters",
            )

        # Проверка длины
        if self.algorithm in expected_lengths:
            expected = expected_lengths[self.algorithm]
            if len(normalized) != expected:
                raise InvalidChecksumError(
                    algorithm=self.algorithm,
                    value=normalized,
                    reason=(
                        f"Invalid length: expected {expected}, got {len(normalized)}"
                    ),
                )


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
                    end=sys.maxsize
                    if not self.meta.content_length
                    else self.meta.content_length - 1,
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
        meta = data.pop("meta")
        checksum_data = meta.get("expected_checksum")
        if isinstance(checksum_data, dict):
            meta["expected_checksum"] = Checksum(**checksum_data)

        meta = FileMeta(**meta)
        chunks_data = data.pop("chunks", [])

        file_obj = cls(meta=meta, **data)

        # Восстанавливаем weakref ПОСЛЕ создания File
        file_obj.chunks = [
            Chunk(**{**c, "_file_ref": weakref.ref(file_obj)}) for c in chunks_data
        ]
        return file_obj


@entity
class DisplayConfig:
    """Настройки отображения, переданные юзером."""

    no_ui: bool = False
    quiet: bool = False
    dry_run: bool = False
    json_logs: bool = False
    verify: bool = True
    debug: bool = False

    def __post_init__(self) -> None:
        if self.debug:
            self.quiet = False


@entity
class ProgressState:
    """Чисто статистика и счетчики загрузки."""

    has_hash: int = 0
    ranges: int = 0
    start_time: float = 0.0
    total_bytes: int = 0
    download_bytes: int = 0
    total_files: int = 0
    files_completed: int = 0


@entity
class LogState:
    """Всё, что касается записи логов на жесткий диск."""

    is_running: bool = False
    log_file: Path
    log_throttle: dict[str, float] = field(default_factory=dict[str, float])
    log_fd: typing.TextIO | None = field(default=None, init=False, repr=False)
    log_queue: asyncio.Queue[str | None] = field(
        default_factory=asyncio.Queue[str | None], init=False
    )
    log_task: asyncio.Task[None] | None = field(default=None, init=False)

    def safe_init(self) -> None:
        """Пытается создать папку для лога. Если не выходит - падает на дефолт."""
        try:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            # Пробуем открыть файл на дозапись (тест прав доступа)
            with self.log_file.open("a", encoding="utf-8"):
                pass
        except OSError:
            # Если юзер передал /root/secret/dir/ и у нас нет прав,
            # откатываемся в текущую директорию!
            fallback = Path.cwd() / "download.log"
            self.log_file = fallback


@entity
class SpeedLimiterState:
    """Логика глобального ограничения скорости."""

    speed_limit: float | None = None
    frequency_speed_limit: int = 10
    time_speed_limit: float = field(init=False)
    bytes_to_check: int = field(init=False)
    prev_bytes: int = 0
    last_checkpoint_time: float = 0.0
    target_time: float = 0.0

    limit_event: asyncio.Event = field(default_factory=asyncio.Event)
    checkpoint_event: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.time_speed_limit = 1 / self.frequency_speed_limit
        if self.speed_limit:
            self.speed_limit = self.speed_limit * 1024**2
            self.bytes_to_check = int(self.speed_limit / self.frequency_speed_limit)
            self.target_time = self.bytes_to_check / self.speed_limit
        else:
            self.bytes_to_check = 5 * 1024**2
        self.limit_event.set()


@entity
class RichUIState:
    """Всё, что относится к библиотеке Rich (Прогресс-бары, консоль)."""

    console: Console = field(
        default_factory=lambda: Console(file=sys.__stderr__, stderr=True)
    )
    refresh_per_second: int = 10
    renewal_rate: float = field(init=False)
    dynamic_title: str = ""
    date_printed: bool = False

    tasks: dict[str, TaskID] = field(default_factory=dict[str, TaskID])
    buffer: defaultdict[str, int] = field(default_factory=lambda: defaultdict(int))
    active_files: set[str] = field(default_factory=set[str])

    refresh: asyncio.Task[None] | None = None
    progress: Progress | None = None
    live: Live | None = None

    def __post_init__(self) -> None:
        self.renewal_rate = 1 / self.refresh_per_second


@entity
class UIState:
    is_running: bool = False
    display: DisplayConfig
    progress: ProgressState = field(default_factory=ProgressState)
    log: LogState
    speed: SpeedLimiterState
    rich: RichUIState = field(default_factory=RichUIState)


@entity
class AMIDState:
    max_rps: int
    min_rps: int = 1
    cooldown_seconds: int = 30
    break_duration: int = 300

    monitor: UIState
    lock: asyncio.Lock = field(default=asyncio.Lock())
    limiter: AsyncLimiter = field(init=False)

    current_rps: int = field(init=False)

    last_429_time: float = 0.0
    circuit_broken_until: float = 0.0

    def __post_init__(self) -> None:
        self.current_rps: int = self.max_rps
        self.limiter = AsyncLimiter(self.current_rps, 1)


@entity
class NetworkState:
    threads: int
    monitor: UIState
    impersonate: BrowserTypeLiteral
    client_kwargs: dict[str, Any] | None = None
    max_retries: int = 3

    client: AsyncSession[Response] = field(init=False)
    rate_limiter: AMIDState = field(init=False)

    def __post_init__(self) -> None:
        self.rate_limiter = AMIDState(max_rps=self.threads * 2, monitor=self.monitor)

        options = cast(dict[str, Any], self.client_kwargs or {}).copy()

        user_headers = options.pop("headers", None)
        headers_obj = Headers(user_headers)
        headers_obj.setdefault("Accept-Encoding", "identity")
        headers_obj.setdefault("Connection", "keep-alive")
        options["headers"] = headers_obj
        options.setdefault("max_clients", self.threads)
        options.setdefault("impersonate", self.impersonate)
        options.setdefault("timeout", 30.0)

        self.client = AsyncSession(
            **options,
        )

    async def close(self) -> None:
        with contextlib.suppress(TypeError, AttributeError):
            await self.client.close()


def validate_threads(value: int | None) -> None:
    if value is not None and (value < 1 or value > 128):
        raise ValidationError(
            param="threads", value=value, reason="Must be between 1 and 128"
        )


def validate_speed_limit(value: float | None) -> None:
    if value is not None and value <= 0.0:
        raise ValidationError(
            param="speed-limit", value=value, reason="Must be greater than 0.0 MB/s"
        )


def validate_positive_int(param_name: str, value: int | None) -> None:

    if value is None:
        return

    if value <= 0:
        raise ValidationError(
            param=param_name,
            value=value,
            reason="Value must be a positive integer (greater than 0).",
        )


def validate_input_file(value: str | None) -> None:
    if value is None or value == "-":
        return

    path = Path(value)
    if not path.exists():
        raise ValidationError(param="input", value=value, reason="File does not exist")
    if not path.is_file():
        raise ValidationError(param="input", value=value, reason="Target is not a file")


def validate_output_dir(value: str) -> None:
    path = Path(value)
    if path.exists() and not path.is_dir():
        raise ValidationError(
            param="output", value=value, reason="Path exists but is not a directory"
        )
    try:
        path.resolve()
    except Exception as e:
        raise ValidationError(
            param="output", value=value, reason="Invalid path format"
        ) from e


def validate_links(links: list[str] | None) -> None:
    if not links:
        return
    for url in links:
        result = urlparse(url)
        if not (result.scheme in ("http", "https") and result.netloc):
            raise ValidationError(
                param="link", value=url, reason="Only HTTP/HTTPS are supported"
            )


def validate_browser(value: str) -> None:
    allowed = get_args(BrowserTypeLiteral)
    if value not in allowed:
        raise ValidationError(
            param="browser",
            value=value,
            reason=f"Unsupported browser. Supported: {', '.join(allowed)}",
        )


@value_object
class HydraConfig:
    threads: int = 128
    no_ui: bool = False
    quiet: bool = False
    output_dir: str = "download"
    speed_limit: float | None = None
    dry_run: bool = False
    json_logs: bool = False
    verify: bool = True
    impersonate: BrowserTypeLiteral = "chrome120"
    debug: bool = False

    min_chunk_size_mb: InitVar[int] = 1
    max_stream_chunk_size_mb: InitVar[int] = 5
    stream_buffer_size_mb: InitVar[int | None] = None
    client_kwargs: dict[str, Any] | None = None

    custom_providers: dict[str, HashProvider] | None = None

    MIN_CHUNK: int = field(init=False)  # 1MB
    STREAM_CHUNK_SIZE: int = field(init=False)  # 5MB
    STREAM_BUFFER_SIZE: int = field(init=False)

    def __post_init__(
        self,
        min_chunk_size_mb: int,
        max_stream_chunk_size_mb: int,
        stream_buffer_size_mb: int | None,
    ) -> None:
        validate_threads(self.threads)
        validate_output_dir(self.output_dir)
        validate_speed_limit(self.speed_limit)
        validate_positive_int("min_chunk_size_mb", min_chunk_size_mb)
        validate_positive_int("max_stream_chunk_size_mb", max_stream_chunk_size_mb)
        validate_positive_int("stream_buffer_size_mb", stream_buffer_size_mb)
        validate_browser(self.impersonate)

        object.__setattr__(self, "MIN_CHUNK", min_chunk_size_mb * 1024**2)
        object.__setattr__(
            self, "STREAM_CHUNK_SIZE", max_stream_chunk_size_mb * 1024**2
        )
        if stream_buffer_size_mb:
            object.__setattr__(
                self, "STREAM_BUFFER_SIZE", stream_buffer_size_mb * 1024**2
            )
        else:
            object.__setattr__(
                self, "STREAM_BUFFER_SIZE", self.STREAM_CHUNK_SIZE * self.threads * 2
            )


@entity
class QueueSet:
    links: asyncio.PriorityQueue[tuple[int, str, Checksum | None]] = field(
        default_factory=asyncio.PriorityQueue
    )
    dispatch_file: asyncio.PriorityQueue[tuple[int, File | None]] = field(
        default_factory=asyncio.PriorityQueue
    )
    file_discovery: asyncio.Queue[int] = field(default_factory=asyncio.Queue)
    chunk: asyncio.PriorityQueue[tuple[int, Chunk] | tuple[Chunk, int]] = field(
        default_factory=asyncio.PriorityQueue
    )
    stream: asyncio.Queue[tuple[int, bytearray]] = field(default_factory=asyncio.Queue)


@entity
class TaskCounts:
    dispatch_links: int = 0
    creators: int = 0
    workers: int = 0
    dispatcher: int = 0
    autosave: int = 0
    telemetry: int = 0


@entity
class SyncSet:
    current_files: asyncio.Condition = field(default_factory=asyncio.Condition)
    chunk_from_future: asyncio.Condition = field(default_factory=asyncio.Condition)
    dynamic_limit: asyncio.Condition = field(default_factory=asyncio.Condition)
    stop_telemetry: asyncio.Event = field(default_factory=asyncio.Event)
    all_complete: asyncio.Event = field(default_factory=asyncio.Event)


@entity
class HydraContext:
    config: HydraConfig

    fs: StorageBackend
    provider: ProviderRouter

    is_stopping: bool = False
    stream: bool = False
    next_offset: int = 0
    dynamic_limit: int = 1

    current_files_id: set[int] = field(default_factory=set[int])
    active_stream: set[Response] = field(default_factory=set[Response])
    files: dict[int, File] = field(default_factory=dict[int, File])
    heap: list[tuple[int, bytearray]] = field(
        default_factory=list[tuple[int, bytearray]]
    )

    # Вложенные домены
    queues: QueueSet = field(default_factory=QueueSet)
    tasks: TaskCounts = field(default_factory=TaskCounts)
    sync: SyncSet = field(default_factory=SyncSet)

    net: NetworkState = field(init=False)
    ui: UIState = field(init=False)

    def __post_init__(self) -> None:
        self.dynamic_limit = 5 if self.config.threads >= 5 else self.config.threads
        # Инициализируем UI, собирая его из настроек конфига
        self.ui = UIState(
            display=DisplayConfig(
                no_ui=self.config.no_ui,
                quiet=self.config.quiet,
                dry_run=self.config.dry_run,
                json_logs=self.config.json_logs,
                verify=self.config.verify,
                debug=self.config.debug,
            ),
            log=LogState(log_file=Path(self.config.output_dir) / "download.log"),
            speed=SpeedLimiterState(speed_limit=self.config.speed_limit),
        )

        self.net = NetworkState(
            threads=self.config.threads,
            monitor=self.ui,
            impersonate=self.config.impersonate,
            client_kwargs=self.config.client_kwargs,
        )
