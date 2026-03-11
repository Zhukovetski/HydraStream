# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import base64
import binascii

from .network import NetworkClient


class NCBIProvider:
    """
    Handles NCBI-specific logic, such as locating and parsing
    the external md5checksums.txt file for integrity validation.
    """

    def __init__(self, network: NetworkClient) -> None:
        self.network = network

    async def get_expected_hash(self, url: str, filename: str) -> str | None:
        """
        Attempts to fetch the expected MD5 hash for a given NCBI file URL.

        Args:
            url (str): The download URL of the file.
            filename (str): The target filename to search for in the checksum list.

        Returns:
            str | None: The MD5 hash string if found, otherwise None.
        """

        # Resolve the parent directory URL
        base_url = url.rsplit("/", 1)[0]
        checksum_url = f"{base_url}/md5checksums.txt"

        resp = await self.network.safe_request("GET", checksum_url)
        if not resp:
            return None

        # Parse the standard NCBI checksum format
        for line in resp.text.splitlines():
            if filename in line:
                return line.split()[0]
        return None


class CloudProvider:
    """
    Universal provider for extracting MD5 checksums from HTTP headers.
    Supports Amazon S3 (ETag), Google Cloud (x-goog-hash), and generic Content-MD5.
    """

    def __init__(self, network: NetworkClient) -> None:
        self.network = network

    async def get_expected_hash(self, url: str) -> str | None:
        resp = await self.network.safe_request("HEAD", url)
        if not resp:
            return None

        headers = resp.headers

        goog_hash = headers.get("x-goog-hash", "")
        if "md5=" in goog_hash:
            try:
                b64_md5 = goog_hash.split("md5=")[1].split(",")[0]
                return base64.b64decode(b64_md5).hex()
            except (IndexError, binascii.Error):
                pass

        etag = headers.get("ETag", "")
        if etag and "-" not in etag:
            clean_etag = etag.strip('"').strip("'")
            if len(clean_etag) == 32:
                return clean_etag

        content_md5 = headers.get("Content-MD5")
        if content_md5:
            try:
                return base64.b64decode(content_md5).hex()
            except binascii.Error:
                pass

        return None


class ProviderRouter:
    def __init__(self, network: NetworkClient) -> None:
        self.ncbi = NCBIProvider(network)
        self.cloud = CloudProvider(network)

    async def resolve_hash(self, url: str, filename: str) -> str | None:
        if "ncbi.nlm.nih.gov" in url:
            md5 = await self.ncbi.get_expected_hash(url, filename)
            if md5:
                return md5

        return await self.cloud.get_expected_hash(url)
