# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.


import asyncio
import sys
from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, Self, TextIO
from urllib.parse import urlparse

from curl_cffi import BrowserTypeLiteral

from hydrastream.exceptions import FileReadError, InvalidParameterError, ValidationError
from hydrastream.monitor import log_start, log_stop, report
from hydrastream.providers import ProviderRouter
from hydrastream.storage import LocalStorageManager

from .engine import run_downloads, stream_all, teardown_engine
from .interfaces import HashProvider, StorageBackend
from .models import (
    Checksum,
    DisplayConfig,
    HydraConfig,
    HydraContext,
    LogState,
    SpeedLimiterState,
    TypeHash,
    UIState,
)


class HydraClient:
    def __init__(
        self,
        config: HydraConfig | None = None,
        threads: int = 1,
        no_ui: bool = False,
        quiet: bool = False,
        output_dir: str = "download",
        dry_run: bool = False,
        min_chunk_size_mb: int = 1,
        max_stream_chunk_size_mb: int = 5,
        stream_buffer_size_mb: int | None = None,
        speed_limit: float | None = None,
        json_logs: bool = False,
        verify: bool = True,
        debug: bool = False,
        impersonate: BrowserTypeLiteral = "chrome120",
        client_kwargs: dict[str, Any] | None = None,
        custom_providers: dict[str, "HashProvider"] | None = None,
        fs: StorageBackend | None = None,
        ui: UIState | None = None,
    ) -> None:
        if config:
            self.config = config
        else:
            self.config = HydraConfig(
                threads=threads,
                dry_run=dry_run,
                min_chunk_size_mb=min_chunk_size_mb,
                max_stream_chunk_size_mb=max_stream_chunk_size_mb,
                speed_limit=speed_limit,
                no_ui=no_ui,
                quiet=quiet,
                output_dir=output_dir,
                stream_buffer_size_mb=stream_buffer_size_mb,
                json_logs=json_logs,
                verify=verify,
                impersonate=impersonate,
                debug=debug,
                client_kwargs=client_kwargs,
            )
        if ui:
            self.ui_init = False
            self.ui = ui
        else:
            self.ui_init = True
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
        self.state: HydraContext | None = None
        if fs:
            self.fs = fs
        else:
            self.fs = LocalStorageManager(
                output_dir=Path(self.config.output_dir), debug=self.config.debug
            )
        self.provider = ProviderRouter()
        self.custom_providers = config.custom_providers if config else custom_providers
        if custom_providers:
            for domain, provider in custom_providers.items():
                self.provider.register(domain, provider)

    async def __aenter__(self) -> Self:
        if self.ui_init:
            await log_start(self.ui)
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        if self.state is not None:
            loop = asyncio.get_running_loop()
            await teardown_engine(self.state, loop)
        if self.ui_init:
            await log_stop(self.ui)

    async def run(
        self,
        links: list[str] | str | None = None,
        input_file: str | None = None,
        expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None = None,
    ) -> None:
        links = await self.validate(links, input_file)
        self.state = HydraContext(
            config=self.config, fs=self.fs, provider=self.provider
        )
        await run_downloads(self.state, links, expected_checksums)

    async def stream(
        self,
        links: list[str] | str | None = None,
        input_file: str | None = None,
        expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None = None,
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[memoryview]]]:

        links = await self.validate(links, input_file)
        self.state = HydraContext(
            config=self.config, fs=self.fs, provider=self.provider
        )
        return stream_all(self.state, links, expected_checksums)

    async def validate(
        self,
        links: list[str] | str | None,
        input_file: str | None,
    ) -> list[str]:

        if not links and not input_file:
            raise ValidationError(
                param="links",
                reason="You must provide either[LINKS] or an --input file.",
            )
        validate_input_file(input_file)
        if links is not None:
            links = [links] if isinstance(links, str) else list(links)
        valid_links = await parse_urls(self.ui, links, input_file)
        if not valid_links:
            raise ValidationError(
                param="links", reason="No valid URLs found to process!"
            )

        return valid_links


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


def is_valid_url(url: str) -> bool:
    """Checks if a given string is a structurally valid HTTP/HTTPS URL."""
    try:
        result = urlparse(url)
        return result.scheme in ("http", "https") and bool(result.netloc)
    except ValueError:
        return False


@contextmanager
def get_input_stream(filepath: str) -> Generator[TextIO, None, None]:
    if filepath == "-":
        yield sys.stdin
        return

    path = Path(filepath).expanduser().resolve()

    if not path.exists():
        raise FileReadError(path=str(path), reason="Path does not exist")
    if not path.is_file():
        raise FileReadError(path=str(path), reason="Target is a directory, not a file")

    try:
        with path.open(encoding="utf-8") as f:
            yield f
    except PermissionError as e:
        raise FileReadError(path=str(path), reason="Permission denied") from e
    except OSError as e:
        raise FileReadError(path=str(path), reason=str(e)) from e


async def parse_urls(
    ctx: UIState, links_from_args: list[str] | None, filepath: str | None
) -> list[str]:
    all_links = []

    # Обработка аргументов
    if links_from_args:
        for url in links_from_args:
            if is_valid_url(url):
                all_links.append(url)
            else:
                await report(
                    ctx,
                    InvalidParameterError(
                        param="url", value=url, reason="Invalid HTTP/HTTPS format"
                    ),
                )

    if filepath:
        with get_input_stream(filepath) as stream:
            for line in stream:
                clean_line = line.strip()
                if not clean_line or clean_line.startswith("#"):
                    continue

                url = clean_line.split()[0]
                if is_valid_url(url):
                    all_links.append(url)
                else:
                    await report(
                        ctx,
                        InvalidParameterError(
                            param="file_link",
                            value=url,
                            reason="Invalid URL in input file",
                        ),
                    )

    return list(dict.fromkeys(all_links))
