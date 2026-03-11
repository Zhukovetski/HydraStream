import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Literal

import httpx
import pytest
import respx

from ncbiloader.loader import NCBILoader


@pytest.mark.asyncio
@respx.mock
async def test_add_task_producer(tmp_path: Path) -> None:
    """Check that the HEAD request is working and the file is queued"""

    url = "https://fake-ncbi.com/genome.gz"

    respx.head(url).mock(return_value=httpx.Response(200, headers={"Content-Length": "500"}))
    loader = NCBILoader(output_dir=str(tmp_path), quiet=True)

    await loader._add_task_producer([url], expected_checksums=None)
    assert "genome.gz" in loader.files

    file_obj = loader.files["genome.gz"]
    assert file_obj.content_length == 500

    file_path = loader.storage.out_dir / "genome.gz"
    assert file_path.exists()
    assert file_path.stat().st_size == 500

    assert not loader._queue.empty()


@pytest.mark.asyncio
@respx.mock
async def test_loader_handles_404_gracefully(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/missing_genome.gz"

    respx.head(url).mock(return_value=httpx.Response(404))

    loader = NCBILoader(output_dir=str(tmp_path), quiet=True)

    await loader.run(url)

    filename = "missing_genome.gz"
    assert filename not in loader.files
    assert not (tmp_path / filename).exists()


@pytest.mark.asyncio
@respx.mock
async def test_graceful_shutdown_prevents_hang_run(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/huge_file.gz"

    respx.head(url).mock(return_value=httpx.Response(200, headers={"Content-Length": "100000000000"}))

    async def slow_stream() -> AsyncGenerator[Literal[b"12345"]]:
        while True:
            yield b"12345"
            await asyncio.sleep(1)

    respx.get(url).mock(return_value=httpx.Response(206, content=slow_stream()))

    loader = NCBILoader(output_dir=str(tmp_path), quiet=True, threads=2)
    run_task = asyncio.create_task(loader.run(url))

    await asyncio.sleep(0.5)
    await loader.stop()

    try:
        await asyncio.wait_for(run_task, timeout=2.0)
    except TimeoutError:
        pytest.fail("CRITICAL ERROR: The program freezes when stopped! Dedlock is in line!")

    assert loader.is_running is False


@pytest.mark.asyncio
@respx.mock
async def test_graceful_shutdown_prevents_hang_stream(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/huge_stream_file.gz"

    respx.head(url).mock(return_value=httpx.Response(200, headers={"Content-Length": "100000000000"}))

    async def slow_stream() -> AsyncGenerator[Literal[b"12345"]]:
        while True:
            yield b"12345"
            await asyncio.sleep(1)

    respx.get(url).mock(return_value=httpx.Response(206, content=slow_stream()))

    loader = NCBILoader(output_dir=str(tmp_path), quiet=True, threads=2)

    async def consume_stream() -> None:
        async for _, file_gen in loader.stream_all([url]):
            async for _ in file_gen:
                pass

    run_task = asyncio.create_task(consume_stream())

    await asyncio.sleep(0.5)
    await loader.stop()

    try:
        await asyncio.wait_for(run_task, timeout=2.0)
    except TimeoutError:
        pytest.fail("CRITICAL ERROR: stream_all freezes when stopped! Dedlock is in line!")

    assert loader.is_running is False
