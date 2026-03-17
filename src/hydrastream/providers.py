# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import base64
import binascii

from hydrastream.models import NetworkState
from hydrastream.network import safe_request


async def ncbi_get_expected_hash(
    ctx: NetworkState, url: str, filename: str
) -> str | None:
    """
    Handles NCBI-specific logic, such as locating and parsing
    the external md5checksums.txt file for integrity validation.
    """

    """
    Attempts to fetch the expected MD5 hash for a given NCBI file URL.

    Args:
        url (str): The download URL of the file.
        filename (str): The target filename to search for in the checksum list.

    Returns:
        str | None: The MD5 hash string if found, otherwise None.
    """

    # Resolve the parent directory URL
    base_url = url.rstrip("/").rsplit("/", 1)[0]
    checksum_url = f"{base_url}/md5checksums.txt"

    resp = await safe_request(ctx, "GET", checksum_url)
    if not resp:
        return None

    # Parse the standard NCBI checksum format
    for line in resp.text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].endswith(filename):
            return line.split()[0]
    return None


async def get_expected_hash(ctx: NetworkState, url: str) -> str | None:
    """
    Universal provider for extracting MD5 checksums from HTTP headers.
    Supports Amazon S3 (ETag), Google Cloud (x-goog-hash), and generic Content-MD5.
    """
    resp = await safe_request(ctx, "HEAD", url)
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


async def resolve_hash(ctx: NetworkState, url: str, filename: str) -> str | None:
    if "ncbi.nlm.nih.gov" in url:
        md5 = await ncbi_get_expected_hash(ctx, url, filename)
        if md5:
            return md5

    return await get_expected_hash(ctx, url)
