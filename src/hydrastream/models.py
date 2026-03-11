# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import contextlib
import os
from dataclasses import dataclass, field
from typing import Self

import orjson


@dataclass(slots=True, order=True)
class Chunk:
    """
    Represents a specific byte range (chunk) of a remote file for downloading.

    Attributes:
        filename (str): The name of the parent file. Excluded from comparisons.
        current_pos (int): The current byte offset being downloaded.
                           Used as the primary sorting key in PriorityQueue.
        start (int): The starting byte index of the chunk. Excluded from comparisons.
        end (int): The ending byte index of the chunk. Excluded from comparisons.
    """

    filename: str = field(compare=False)
    current_pos: int
    start: int = field(compare=False)
    end: int = field(compare=False)

    @property
    def is_finished(self) -> bool:
        """Checks if the chunk has been completely downloaded."""
        return self.current_pos > self.end

    @property
    def size(self) -> int:
        """Returns the total allocated size of the chunk in bytes."""
        return self.end - self.start + 1

    @property
    def uploaded(self) -> int:
        """Returns the number of bytes successfully downloaded so far."""
        return self.current_pos - self.start + 1

    @property
    def remaining(self) -> int:
        """Returns the number of bytes left to download in this chunk."""
        return max(0, self.end - self.current_pos + 1)

    def get_header(self) -> dict[str, str]:
        """
        Generates the HTTP Range header required to fetch the remaining data.

        Returns:
            dict[str, str]: A dictionary containing the 'Range' header.
        """
        return {"Range": f"bytes={self.current_pos}-{self.end}"}


@dataclass(slots=True)
class File:
    """
    Represents a remote file, managing its chunk geometry, download state,
    and system resources (file descriptors).

    Attributes:
        filename (str): The local name of the file.
        url (str): The remote source URL.
        content_length (int): Total size of the file in bytes.
        chunk_size (int): Target size for each individual chunk.
        chunks (list[Chunk]): List of chunk objects representing the file's layout.
        expected_md5 (str | None): The expected MD5 hash for integrity validation.
        verified (bool): Flag indicating if the file has passed integrity checks.
        fd (int | None): The OS file descriptor for atomic writes.
                         Excluded from serialization and representation.
    """

    filename: str
    url: str
    content_length: int
    chunk_size: int
    chunks: list[Chunk] = field(default_factory=list[Chunk])
    expected_md5: str | None = None
    verified: bool = False
    fd: int | None = field(default=None, repr=False, compare=False)
    is_failed: bool = False

    def __post_init__(self) -> None:
        """
        Calculates and generates the chunk layout if it hasn't been provided
        (e.g., during initialization from scratch, not deserialization).

        Raises:
            ValueError: If the defined chunk_size is zero or negative.
        """
        if self.chunks:
            return

        if self.chunk_size <= 0:
            raise ValueError(f"Chunk size must be positive, got {self.chunk_size}")

        # Fast ceiling division trick without importing the math module
        part_count = -(-self.content_length // self.chunk_size)

        for i in range(part_count):
            start = i * self.chunk_size
            end = min((i + 1) * self.chunk_size - 1, self.content_length - 1)

            self.chunks.append(
                Chunk(
                    start=start,
                    end=end,
                    current_pos=start,
                    filename=self.filename,
                )
            )

    @property
    def is_complete(self) -> bool:
        """Checks if all chunks associated with this file are fully downloaded."""
        if not self.chunks:
            return False
        return all(c.is_finished for c in self.chunks)

    @property
    def downloaded_size(self) -> int:
        """Calculates the total number of bytes written to disk or stream."""
        return sum(c.current_pos - c.start for c in (self.chunks or []))

    @property
    def progress(self) -> float:
        """Calculates the overall download progress percentage (0.0 to 100.0)."""
        if self.content_length <= 0:
            return 0.0
        return (self.downloaded_size / self.content_length) * 100

    def to_json(self) -> bytes:
        """
        Serializes the File object and its nested Chunks into a JSON byte string.

        Returns:
            bytes: JSON representation of the file state.
        """
        return orjson.dumps(self, option=orjson.OPT_SERIALIZE_DATACLASS | orjson.OPT_INDENT_2)

    @classmethod
    def from_json(cls, content: bytes) -> Self:
        """
        Deserializes a JSON byte string back into a File object,
        reconstructing the nested Chunk objects.

        Args:
            content (bytes): The JSON data.

        Returns:
            File: An instantiated File object.

        Raises:
            orjson.JSONDecodeError: If the content is malformed or invalid JSON.
            TypeError: If the parsed JSON dictionary is missing required class attributes.
        """
        data = orjson.loads(content)
        chunks_data = data.get("chunks", [])
        data["chunks"] = [Chunk(**c_data) for c_data in chunks_data]

        return cls(**data)

    def close_fd(self) -> None:
        """
        Safely closes the associated OS file descriptor if it is open.
        Suppresses OSErrors (e.g., if the descriptor was already closed externally).
        """
        if self.fd is not None:
            with contextlib.suppress(OSError):
                os.close(self.fd)
            self.fd = None
