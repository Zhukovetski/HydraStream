# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

import asyncio
import contextlib
import sys
import typing
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    Literal,
    Self,
    TypeVar,
    dataclass_transform,
    get_args,
    overload,
)
from urllib.parse import urlparse

import orjson
from aiolimiter import AsyncLimiter
from curl_cffi import (
    BrowserTypeLiteral,
    Headers,
    Response,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    StringConstraints,
    TypeAdapter,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress,
    TaskID,
)

from hydrastream._curl_shim import AsyncSession
from hydrastream.exceptions import (
    OrphanedChunkError,
    ValidationError,
)
from hydrastream.interfaces import HashProvider, StorageBackend

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

_T = TypeVar("_T")


@dataclass_transform(kw_only_default=True, order_default=False)
@overload
def my_dataclass(cls: type[_T], /) -> type[_T]: ...


@dataclass_transform(kw_only_default=True, order_default=False)
@overload
def my_dataclass(
    *,
    init: bool = True,
    repr: bool = True,
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    match_args: bool = True,
    kw_only: bool = True,
    slots: bool = True,
    weakref_slot: bool = False,
) -> Callable[[type[_T]], type[_T]]: ...


def my_dataclass(cls: Any = None, /, **kwargs: Any) -> Any:
    # Ваши дефолты
    params = {"kw_only": True, "slots": True}
    params.update(kwargs)

    def wrap(obj: type[_T]) -> type[_T]:
        return dataclass(**params)(obj)

    if cls is None:
        return wrap
    return wrap(cls)


@my_dataclass
class Chunk:
    current_pos: int
    start: int
    end: int
    _file: File | None = field(repr=False, default=None)

    @property
    def file(self) -> File:
        if self._file is None:
            # Выбрасываем структурированную ошибку вместо безликого RuntimeError
            raise OrphanedChunkError(start_pos=self.start, end_pos=self.end)
        return self._file

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


# 1. Список алгоритмов и их длин
EXPECTED_LENGTHS = {
    "md5": 32,
    "sha1": 40,
    "sha224": 56,
    "sha256": 64,
    "sha384": 96,
    "sha512": 128,
    "blake2b": 128,
    "blake2s": 64,
    "sha3_224": 56,
    "sha3_256": 64,
    "sha3_384": 96,
    "sha3_512": 128,
}

# Алгоритмы с переменной длиной (просто проверяем на четность hex-строки)
VARIABLE_LENGTH = {"shake_128", "shake_256"}

# Валидатор строки: чистит пробелы, в нижний регистр, проверка на hex
HashStr = Annotated[
    str, StringConstraints(pattern=r"^[0-9a-f]+$", strip_whitespace=True, to_lower=True)
]


class Checksum(BaseModel):
    # Настройки как в твоем value_object
    model_config = ConfigDict(frozen=True)

    algorithm: TypeHash
    value: HashStr

    @model_validator(mode="after")
    def validate_hash_logic(self) -> Checksum:
        algo = self.algorithm
        length = len(self.value)

        # Если длина фиксирована — проверяем строго
        if algo in EXPECTED_LENGTHS:
            expected = EXPECTED_LENGTHS[algo]
            if length != expected:
                raise ValueError(
                    f"Invalid length for {algo}: expected {expected}, got {length}"
                )

        # Если это SHAKE — проверяем только, что это полные байты
        # (четное кол-во символов)
        elif algo in VARIABLE_LENGTH:
            if length % 2 != 0:
                raise ValueError(
                    f"Invalid length for {algo}: must be even, got {length}"
                )

        return self


@my_dataclass(frozen=True)
class FileMeta:
    id: int
    filename: str
    url: str
    content_length: int
    expected_checksum: Checksum | None = field(default=None)
    supports_ranges: bool


@my_dataclass
class File:
    meta: FileMeta
    chunk_size: int
    chunks: list[Chunk] = field(default_factory=list[Chunk])
    fd: int | None = field(default=None, repr=False)
    verified: bool = field(default=False)
    is_failed: bool = field(default=False)

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
                    _file=self,
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
                    _file=self,
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
        # 1. Сначала превращаем JSON в один большой словарь (через fast orjson)
        raw_data = orjson.loads(content)

        # 2. Используем TypeAdapter для автоматической сборки всей вложенности.
        # Он сам создаст FileMeta, внутри него Checksum, и список Chunk.
        file_obj = TypeAdapter(cls).validate_python(raw_data)

        # 3. Единственное, что Pydantic не сделает сам —
        # не проставит обратную ссылку _file в каждый чанк (т.к. это цикл)
        for chunk in file_obj.chunks:
            chunk._file = file_obj  # pyright: ignore[reportPrivateUsage]

        return file_obj


@my_dataclass
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


@my_dataclass
class ProgressState:
    """Чисто статистика и счетчики загрузки."""

    has_hash: int = 0
    ranges: int = 0
    start_time: float = 0.0
    total_bytes: int = 0
    download_bytes: int = 0
    total_files: int = 0
    files_completed: int = 0


@my_dataclass
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


@my_dataclass
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
    controller_checkpoint_event: asyncio.Event = field(default_factory=asyncio.Event)
    throttler_checkpoint_event: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.time_speed_limit = 1 / self.frequency_speed_limit
        if self.speed_limit:
            self.speed_limit = self.speed_limit * 1024**2
            self.bytes_to_check = int(self.speed_limit / self.frequency_speed_limit)
            self.target_time = self.bytes_to_check / self.speed_limit
        else:
            self.bytes_to_check = 5 * 1024**2
        self.limit_event.set()


@my_dataclass
class RichUIState:
    """Всё, что относится к библиотеке Rich (Прогресс-бары, консоль)."""

    console: Console = field(default_factory=lambda: Console(stderr=True))
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


@my_dataclass
class UIState:
    is_running: bool = False
    cancelled: bool = False
    display: DisplayConfig
    progress: ProgressState = field(default_factory=ProgressState)
    log: LogState
    speed: SpeedLimiterState
    rich: RichUIState = field(default_factory=RichUIState)

    def __post_init__(self) -> None:
        if self.display.debug:
            self.rich.console = Console(file=sys.__stderr__, stderr=True)


@my_dataclass
class AMIDState:
    max_rps: int
    min_rps: int = 1
    cooldown_seconds: int = 30
    break_duration: int = 300

    monitor: UIState
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    limiter: AsyncLimiter = field(init=False)

    current_rps: int = field(init=False)

    last_429_time: float = 0.0
    circuit_broken_until: float = 0.0

    def __post_init__(self) -> None:
        self.current_rps: int = self.max_rps
        self.limiter = AsyncLimiter(self.current_rps, 1)


@my_dataclass
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

        options = (self.client_kwargs or {}).copy()

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


class HydraConfig(BaseSettings):
    # Настройки загрузки из окружения и .env
    model_config = SettingsConfigDict(
        env_prefix="HYDRA_",
        env_file=".env",
        extra="ignore",
        frozen=True,
        arbitrary_types_allowed=True,
    )

    # --- Поля с базовой валидацией Pydantic ---
    threads: int = Field(default=128, ge=1, le=128)
    no_ui: bool = False
    quiet: bool = False
    output_dir: str = "download"
    speed_limit: float | None = Field(default=None, gt=0.0)
    dry_run: bool = False
    json_logs: bool = False
    verify: bool = True
    impersonate: BrowserTypeLiteral = "chrome120"
    debug: bool = False

    # Входные параметры в МБ (аналог InitVar)
    min_chunk_size_mb: PositiveInt = 1
    max_stream_chunk_size_mb: PositiveInt = 5
    buffer_size_mb: int | None = Field(default=None, ge=50)

    links: list[str] = Field(default_factory=list)
    client_kwargs: dict[str, Any] | None = Field(default=None, exclude=True)

    custom_providers: dict[str, HashProvider] | None = Field(
        default=None, repr=False, exclude=True
    )

    @field_validator("output_dir")
    @classmethod
    def validate_output(cls, v: str) -> str:
        path = Path(v)
        if path.exists() and not path.is_dir():
            raise ValueError(f"Path '{v}' exists but is not a directory")
        try:
            path.resolve()
        except Exception as e:
            raise ValueError(f"Invalid path format: {v}") from e
        return v

    @field_validator("links")
    @classmethod
    def validate_links_logic(cls, v: list[str]) -> list[str]:
        for url in v:
            result = urlparse(url)
            if not (result.scheme in ("http", "https") and result.netloc):
                raise ValueError(f"Only HTTP/HTTPS are supported: {url}")
        return v

    @computed_field
    @property
    def MIN_CHUNK(self) -> int:  # noqa: N802
        return self.min_chunk_size_mb * 1024**2

    @computed_field
    @property
    def STREAM_CHUNK_SIZE(self) -> int:  # noqa: N802
        return self.max_stream_chunk_size_mb * 1024**2

    @computed_field
    @property
    def BUFFER_SIZE(self) -> int:  # noqa: N802
        if self.buffer_size_mb:
            return self.buffer_size_mb * 1024**2
        return 50 * 1024**2


@my_dataclass(frozen=True)
class LinkData:
    id: int
    url: str
    checksum: Checksum | None


@my_dataclass(order=True, frozen=True)
class WriteChunk:
    fd: int
    offset: int
    length: int = field(compare=False)
    data: list[bytes] = field(compare=False)


@my_dataclass(order=True, frozen=True)
class StreamChunk:
    start: int
    data: bytes = field(compare=False)


T_co = TypeVar("T_co", covariant=True)


@my_dataclass(order=True, frozen=True)
class Envelope(Generic[T_co]):
    sort_key: tuple[int, ...]

    payload: T_co | None = field(compare=False, default=None)

    msg: bool = field(compare=False, default=False)

    is_poison_pill: bool = field(compare=False, default=False)
    is_last_survivor: bool = field(compare=False, default=False)


@my_dataclass
class QueueSet:
    links: asyncio.PriorityQueue[Envelope[LinkData | None]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[LinkData | None]]
    )
    dispatch_file: asyncio.PriorityQueue[Envelope[File | None]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[File | None]]
    )
    disk: asyncio.PriorityQueue[Envelope[WriteChunk | None]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[WriteChunk | None]]
    )
    file_discovery: asyncio.Queue[int] = field(default_factory=asyncio.Queue[int])
    chunk: asyncio.PriorityQueue[Envelope[Chunk | None]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[Chunk | None]]
    )
    stream: asyncio.PriorityQueue[Envelope[StreamChunk | None]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[StreamChunk | None]]
    )


@my_dataclass
class TaskCounts:
    feeder: int = 0
    resolvers: int = 0
    workers: int = 0
    dispatcher: int = 0
    autosaver: int = 0
    throttler: int = 0
    controller: int = 0


@my_dataclass
class SyncSet:
    current_files: asyncio.Condition = field(default_factory=asyncio.Condition)
    chunk_from_future: asyncio.Condition = field(default_factory=asyncio.Condition)
    flush_event: asyncio.Event = field(default_factory=asyncio.Event)
    stop_adaptive_controller: asyncio.Event = field(default_factory=asyncio.Event)
    all_complete: asyncio.Event = field(default_factory=asyncio.Event)


@my_dataclass
class HydraContext:
    config: HydraConfig

    fs: StorageBackend
    provider: ProviderRouter

    is_running: bool = True
    is_stopping: bool = False
    stream: bool = False
    next_offset: int = 0
    dynamic_limit: int = 1

    current_files_id: set[int] = field(default_factory=set[int])
    active_stream: set[Response] = field(default_factory=set[Response])
    files: dict[int, File] = field(default_factory=dict[int, File])
    heap: list[StreamChunk] = field(default_factory=list[StreamChunk])

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
