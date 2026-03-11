# tests/test_providers.py
import base64
from collections.abc import Callable, Generator
from typing import Any

import httpx
import pytest
import respx
from hydrastream.monitor import ProgressMonitor
from hydrastream.network import NetworkClient
from hydrastream.providers import CloudProvider, NCBIProvider, ProviderRouter


@pytest.fixture
def network_client() -> Generator[Any, Any, Any]:
    monitor = ProgressMonitor(quiet=True)
    client = NetworkClient(threads=1, monitor=monitor)
    yield client


@pytest.mark.asyncio
@respx.mock
async def test_ncbi_provider(network_client: Callable) -> None:
    provider = NCBIProvider(network_client)
    url = "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF_001/file.gz"
    checksum_url = "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF_001/md5checksums.txt"

    fake_md5_content = (
        "d41d8cd98f00b204e9800998ecf8427e  file.gz\n1234567890abcdef  other.gz"
    )
    respx.get(checksum_url).mock(
        return_value=httpx.Response(200, text=fake_md5_content)
    )

    hash_val = await provider.get_expected_hash(url, "file.gz")
    assert hash_val == "d41d8cd98f00b204e9800998ecf8427e"


@pytest.mark.asyncio
@respx.mock
async def test_cloud_provider_s3_etag(network_client: Callable) -> None:
    provider = CloudProvider(network_client)
    url = "https://s3.amazonaws.com/bucket/data.bin"

    respx.head(url).mock(
        return_value=httpx.Response(
            200, headers={"ETag": '"abcdef1234567890abcdef1234567890"'}
        )
    )

    hash_val = await provider.get_expected_hash(url)
    assert hash_val == "abcdef1234567890abcdef1234567890"


@pytest.mark.asyncio
@respx.mock
async def test_cloud_provider_goog_hash(network_client: Callable) -> None:
    provider = CloudProvider(network_client)
    url = "https://storage.googleapis.com/bucket/data.bin"

    raw_md5 = b"1234567890abcdef"
    b64_md5 = base64.b64encode(raw_md5).decode()

    respx.head(url).mock(
        return_value=httpx.Response(
            200, headers={"x-goog-hash": f"crc32c=..., md5={b64_md5}"}
        )
    )

    hash_val = await provider.get_expected_hash(url)
    assert hash_val == raw_md5.hex()


@pytest.mark.asyncio
async def test_provider_router_delegation(network_client: Callable) -> None:
    router = ProviderRouter(network_client)

    assert await router.resolve_hash("https://google.com/file", "file") is None
