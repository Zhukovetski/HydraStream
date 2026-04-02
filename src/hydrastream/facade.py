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
        threads: int = 1,
        no_ui: bool = False,
        quiet: bool = False,
        out_dir: str = "download",
        stream_buffer_size_mb: int | None = None,
        verify: bool = True,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.config = HydraConfig(
            threads=threads,
            no_ui=no_ui,
            quiet=quiet,
            out_dir=out_dir,
            stream_buffer_size_mb=stream_buffer_size_mb,
            verify=verify,
            client_kwargs=client_kwargs,
        )
        self.state: HydraContext | None = None
        self.fs = LocalStorageManager(out_dir=Path(self.config.out_dir))

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
