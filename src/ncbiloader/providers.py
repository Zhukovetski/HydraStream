from .network import NetworkClient


class NCBIProvider:
    def __init__(self, network: NetworkClient) -> None:
        self.network = network

    async def get_expected_hash(self, url: str, filename: str) -> str | None:
        """Твой метод для поиска MD5"""
        if "ncbi.nlm.nih.gov" not in url:
            return None

        base_url = url.rsplit("/", 1)[0]
        checksum_url = f"{base_url}/md5checksums.txt"

        resp = await self.network.safe_request("GET", checksum_url)
        if not resp:
            return None

        for line in resp.text.splitlines():
            if filename in line:
                return line.split()[0]
        return None
