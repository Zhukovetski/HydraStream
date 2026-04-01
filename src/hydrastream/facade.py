# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.


import asyncio
from collections.abc import AsyncGenerator
from types import TracebackType
from typing import Any, Self

from hydrastream.engine import run_downloads, stream_all, teardown_engine
from hydrastream.models import HydraConfig, HydraContext, TypeHash


class HydraClient:
    def __init__(
        self,
        threads: int = 1,
        no_ui: bool = False,
        quiet: bool = False,
        out_dir: str = "download",
        stream_buffer_size: int | None = None,
        verify: bool = True,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.config = HydraConfig(
            threads=threads,
            no_ui=no_ui,
            quiet=quiet,
            out_dir=out_dir,
            stream_buffer_size=stream_buffer_size,
            verify=verify,
            client_kwargs=client_kwargs,
        )
        self.state: HydraContext | None = None

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
        self.state = HydraContext(config=self.config)
        await run_downloads(self.state, links, expected_checksums)

    def stream(
        self,
        links: list[str],
        expected_checksums: dict[str, tuple[TypeHash, str]] | None = None,
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
        self.state = HydraContext(config=self.config)
        return stream_all(self.state, links, expected_checksums)
