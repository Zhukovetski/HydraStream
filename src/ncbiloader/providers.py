# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

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
        if "ncbi.nlm.nih.gov" not in url:
            return None

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
