# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.


import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from .engine import run_downloads, stream_all, teardown_engine
from .interfaces import LocalStorageManager
from .models import HydraConfig, HydraContext, TypeHash


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
        min_stream_chunk_size_mb: int = 5,
        stream_buffer_size_mb: int | None = None,
        speed_limit: float | None = None,
        json_logs: bool = False,
        verify: bool = True,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if config:
            self.config = config
        else:
            self.config = HydraConfig(
                threads=threads,
                dry_run=dry_run,
                min_chunk_size_mb=min_chunk_size_mb,
                min_stream_chunk_size_mb=min_stream_chunk_size_mb,
                speed_limit=speed_limit,
                no_ui=no_ui,
                quiet=quiet,
                output_dir=output_dir,
                stream_buffer_size_mb=stream_buffer_size_mb,
                json_logs=json_logs,
                verify=verify,
                client_kwargs=client_kwargs,
            )

        self.state: HydraContext | None = None
        self.fs = LocalStorageManager(output_dir=Path(self.config.output_dir))

    async def __aenter__(self) -> Self:
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

    async def run(
        self,
        links: list[str] | str,
        expected_checksums: dict[str, tuple[TypeHash, str]] | None = None,
    ) -> None:
        self.state = HydraContext(config=self.config, fs=self.fs)
        await run_downloads(self.state, links, expected_checksums)

    def stream(
        self,
        links: list[str],
        expected_checksums: dict[str, tuple[TypeHash, str]] | None = None,
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
        self.state = HydraContext(config=self.config, fs=self.fs)
        return stream_all(self.state, links, expected_checksums)
