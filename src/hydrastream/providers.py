# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import base64
import binascii
from typing import Protocol

from .models import Checksum, NetworkState
from .network import safe_request


# 1. КОНТРАКТ: Любой класс, у которого есть метод resolve, считается Провайдером!
class HashProvider(Protocol):
    async def resolve(
        self, ctx: NetworkState, url: str, filename: str
    ) -> Checksum | None: ...


# 2. КОНКРЕТНЫЙ ПРОВАЙДЕР ДЛЯ NCBI
class NCBIProvider:
    async def resolve(
        self, ctx: NetworkState, url: str, filename: str
    ) -> Checksum | None:
        base_url = url.rstrip("/").rsplit("/", 1)[0]
        checksum_url = f"{base_url}/md5checksums.txt"

        resp = await safe_request(ctx, "GET", checksum_url)
        if not resp:
            return None

        for line in resp.text.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].endswith(filename):
                return Checksum(algorithm="md5", value=parts[0])  # Возвращаем объект!
        return None


# 3. КОНКРЕТНЫЙ ПРОВАЙДЕР ДЛЯ ОБЛАКОВ (S3, GCS)
class CloudProvider:
    async def resolve(self, ctx: NetworkState, url: str) -> Checksum | None:
        resp = await safe_request(ctx, "HEAD", url)
        if not resp:
            return None

        headers = resp.headers

        # Google Cloud (может быть и crc32c, но мы парсим md5)
        goog_hash = headers.get("x-goog-hash", "")
        if "md5=" in goog_hash:
            try:
                b64_md5 = goog_hash.split("md5=")[1].split(",")[0]
                hex_md5 = base64.b64decode(b64_md5).hex()
                return Checksum(algorithm="md5", value=hex_md5)
            except (IndexError, binascii.Error):
                pass

        # AWS S3
        etag = headers.get("ETag", "")
        if etag and "-" not in etag:
            clean_etag = etag.strip('"').strip("'")
            if len(clean_etag) == 32:
                return Checksum(algorithm="md5", value=clean_etag)

        # Если найдешь облако, отдающее SHA256:
        # sha256_header = headers.get("x-amz-meta-sha256")
        # if sha256_header: return Checksum("sha256", sha256_header)

        return None


class ProviderRouter:
    def __init__(self) -> None:
        # РЕЕСТР ПРОВАЙДЕРОВ
        # Добавить новый сайт? Просто добавь 1 строчку сюда!
        self._routes: dict[str, HashProvider] = {
            "ncbi.nlm.nih.gov": NCBIProvider(),
            # "ensembl.org": EnsemblProvider(),  # Пример на будущее
            # "huggingface.co": HuggingFaceProvider(),  # Пример на будущее
        }

        # Провайдер "последней надежды" (если домен неизвестен)
        self._fallback = CloudProvider()

    def register(self, domain_keyword: str, provider: HashProvider) -> None:
        """Позволяет динамически добавлять провайдеров извне (Плагины)"""
        self._routes[domain_keyword] = provider

    async def resolve_hash(
        self, ctx: NetworkState, url: str, filename: str
    ) -> Checksum | None:
        """
        Ищет нужного провайдера по URL и делегирует ему задачу.
        """
        for keyword, provider in self._routes.items():
            if keyword in url:
                return await provider.resolve(ctx, url, filename)

        # Если ни один домен не подошел, пробуем вытащить из стандартных заголовков
        return await self._fallback.resolve(ctx, url)
