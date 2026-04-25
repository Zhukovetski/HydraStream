from __future__ import annotations

import uuid
from dataclasses import field
from enum import IntEnum, StrEnum
from pathlib import Path
from typing import Any

from hydrastream.models import my_dataclass
from hydrastream.utils import format_size, redact_url


class ExitCode(IntEnum):
    SUCCESS = 0
    GENERAL_ERROR = 1  # Неизвестная критическая ошибка
    USAGE_ERROR = 2  # Юзер передал плохой путь к файлу (-i bad_file.txt)
    HASH_MISMATCH = 3  # Файл скачался, но MD5 не совпал
    NETWORK_ERROR = 4  # Отвалился интернет или сервер выдает 404/503
    INTERRUPTED = 130  # Пользователь нажал Ctrl+C


class LogStatus(StrEnum):
    SUCCESS = "SUCCESS"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    INTERRUPT = "INTERRUPT"


@my_dataclass
class HydraError(Exception):
    exit_code: ExitCode = ExitCode.GENERAL_ERROR
    log_status: LogStatus = LogStatus.ERROR
    message_tpl: str = "Unknown error"

    # Исключаем служебные поля из сериализации в лог
    error_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8], init=False)

    def __post_init__(self) -> None:
        try:
            self.formatted_msg = self.message_tpl.format(**self.__dict__)
        except (KeyError, IndexError):
            self.formatted_msg = self.message_tpl

        super().__init__(f"[{self.error_id}] {self.formatted_msg}")


@my_dataclass
class HashMismatchError(HydraError):
    filename: str
    algorithm: str
    expected: str
    actual: str
    exit_code: ExitCode = ExitCode.HASH_MISMATCH
    log_status: LogStatus = LogStatus.CRITICAL

    def __post_init__(self) -> None:
        self.message_tpl: str = (
            f"Hash mismatch for {self.filename}! ({self.algorithm}) "
            f"Expected {self.expected}, got {self.actual}"
        )
        super().__post_init__()


@my_dataclass
class InsufficientSpaceError(HydraError):
    path: str | Path
    required: int
    free: int

    def __post_init__(self) -> None:
        self.message_tpl: str = (
            f"Insufficient space on {self.path}. "
            f"Need {format_size(self.required)}, have {format_size(self.free)}."
        )
        super().__post_init__()


@my_dataclass
class DownloadFailedError(HydraError):
    url: str
    status_code: int | None = None
    reason: str | None = None  # Описание ошибки от сервера или библиотеки

    exit_code: ExitCode = ExitCode.NETWORK_ERROR
    log_status: LogStatus = LogStatus.ERROR

    def __post_init__(self) -> None:
        self.url = redact_url(self.url)
        parts = [f"Failed to download {self.url}"]

        if self.status_code:
            parts.append(f"(Status: {self.status_code})")
        if self.reason:
            parts.append(f": {self.reason}")

        self.message_tpl = " ".join(parts)
        super().__post_init__()


class WorkerScaleDown(Exception):  # noqa: N818
    """Сигнал для воркера уйти в спячку при сужении лимита AIMD."""

    pass


@my_dataclass
class FileSizeMismatchError(HydraError):
    filename: str
    expected: int
    actual: int
    exit_code: ExitCode = ExitCode.HASH_MISMATCH  # Обычно размер и хеш идут рядом
    log_status: LogStatus = LogStatus.ERROR

    def __post_init__(self) -> None:
        self.message_tpl = (
            f"Size mismatch for {self.filename}: "
            f"Expected {format_size(self.expected)}, got {format_size(self.actual)}."
        )
        super().__post_init__()


@my_dataclass
class HydraFileNotFoundError(HydraError):
    filename: str
    path: str
    exit_code: ExitCode = ExitCode.USAGE_ERROR
    log_status: LogStatus = LogStatus.ERROR

    def __post_init__(self) -> None:
        self.message_tpl = f"File not found: {self.filename} (Expected at: {self.path})"
        super().__post_init__()


@my_dataclass
class StateSaveError(HydraError):
    filename: str
    target_path: str
    reason: str
    exit_code: ExitCode = ExitCode.GENERAL_ERROR
    log_status: LogStatus = LogStatus.CRITICAL  # Состояние — это критично

    def __post_init__(self) -> None:
        self.message_tpl = (
            f"Failed to save state for {self.filename} "
            f"at {self.target_path}. Reason: {self.reason}"
        )
        super().__post_init__()


@my_dataclass
class OrphanedChunkError(HydraError):
    # В DOD нам важно знать, какой именно кусок данных "осиротел"
    start_pos: int
    end_pos: int
    exit_code: ExitCode = ExitCode.GENERAL_ERROR
    log_status: LogStatus = LogStatus.CRITICAL  # Это баг логики, а не юзера

    def __post_init__(self) -> None:
        self.message_tpl = (
            f"Orphaned Chunk: Reference to File object lost! "
            f"Chunk range: {self.start_pos}-{self.end_pos}"
        )
        super().__post_init__()


@my_dataclass
class InvalidChecksumError(HydraError):
    algorithm: str
    value: str
    reason: str
    exit_code: ExitCode = ExitCode.USAGE_ERROR
    log_status: LogStatus = LogStatus.ERROR

    def __post_init__(self) -> None:
        self.message_tpl = (
            f"Invalid checksum for {self.algorithm}: {self.reason} "
            f"(Value: {self.value})"
        )
        super().__post_init__()


@my_dataclass
class LogFileError(HydraError):
    path: str
    original_err: str
    exit_code: ExitCode = ExitCode.GENERAL_ERROR
    log_status: LogStatus = LogStatus.CRITICAL
    message_tpl: str = (
        "CRITICAL: Cannot write to log file {path}. OS Error: {original_err}"
    )

    def __post_init__(self) -> None:
        super().__post_init__()


@my_dataclass
class SystemContextError(HydraError):
    operation: str  # Что мы пытались сделать (например, "initializing tasks")
    original_error: str  # Текст ошибки из OSError или Exception
    path: str | None = None  # Путь к файлу, если он был замешан

    exit_code: ExitCode = ExitCode.GENERAL_ERROR
    log_status: LogStatus = LogStatus.CRITICAL

    def __post_init__(self) -> None:
        # Собираем понятное сообщение
        msg = f"System error during {self.operation}: {self.original_error}"
        if self.path:
            msg += f" (Path: {self.path})"

        self.message_tpl = msg
        super().__post_init__()


@my_dataclass
class ValidationError(HydraError):
    # Универсальный класс для всех проблем с аргументами
    param: str
    value: Any = None
    reason: str
    exit_code: ExitCode = ExitCode.USAGE_ERROR
    log_status: LogStatus = LogStatus.ERROR

    def __post_init__(self) -> None:
        self.message_tpl = f"Invalid --{self.param} [{self.value}]: {self.reason}"
        super().__post_init__()


@my_dataclass
class FileReadError(HydraError):
    path: str
    reason: str
    exit_code: ExitCode = ExitCode.USAGE_ERROR
    log_status: LogStatus = LogStatus.ERROR


@my_dataclass
class InvalidParameterError(HydraError):
    param: str
    value: str | None = None
    reason: str
    exit_code: ExitCode = ExitCode.USAGE_ERROR
    log_status: LogStatus = LogStatus.WARNING  # Для ссылок можно WARNING,


@my_dataclass
class StreamError(HydraError):
    url: str
    filename: str

    exit_code: ExitCode = ExitCode.NETWORK_ERROR
    log_status: LogStatus = LogStatus.ERROR

    def __post_init__(self) -> None:
        self.url = redact_url(self.url)
        self.message_tpl = (
            f"Stream interrupted for {self.filename}. "
            f"Server does not support partial downloads "
            f"(Range requests) for {self.url}. "
            f"Cannot resume stream. Aborting."
        )
        super().__post_init__()
