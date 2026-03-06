# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import hashlib
import os
from _hashlib import HASH
from pathlib import Path

from .models import File


class StorageManager:
    """
    Manages disk I/O operations, state persistence for resumable downloads,
    and data integrity validation (size and checksums).
    """

    def __init__(self, output_dir: str) -> None:
        """
        Initializes the storage manager and ensures necessary directories exist.

        Args:
            output_dir (str): The target directory for downloaded files.
        """
        self.out_dir = Path(output_dir).expanduser().resolve()
        self.state_dir = self.out_dir / ".states"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def create_sparse_file(self, filename: str, size: int) -> None:
        """
        Allocates disk space for a file using OS-level truncation.
        This prevents disk fragmentation and allows atomic random-access writes.

        Args:
            filename (str): Name of the file to create.
            size (int): Target size in bytes.
        """
        filepath = self.out_dir / filename
        with filepath.open("wb") as f:
            f.truncate(size)

    def open_file(self, filename: str) -> int:
        """
        Opens a file at the OS level for random-access read/write.

        Args:
            filename (str): Name of the file.

        Returns:
            int: The OS file descriptor (fd).
        """
        filepath = self.out_dir / filename
        # os.pwrite is atomic and thread-safe for offset-based writing
        return os.open(filepath, os.O_RDWR)

    async def write_chunk_data(self, fd: int, data: bytearray, offset: int) -> None:
        """
        Asynchronously writes a byte array to a specific file offset using
        a thread pool to prevent blocking the asyncio Event Loop.

        Args:
            fd (int): The OS file descriptor.
            data (bytearray): The raw bytes to write.
            offset (int): The absolute byte position in the file.
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.pwrite, fd, data, offset)

    def get_state_path(self, filename: str) -> Path:
        """Constructs the path for the JSON state file."""
        return self.state_dir / f"{filename}.state.json"

    def save_state(self, file_obj: File) -> None:
        """Serializes and saves a single File object state to disk."""
        path = self.get_state_path(file_obj.filename)
        path.write_bytes(file_obj.to_json())

    def save_all_states(self, files: dict[str, File]) -> None:
        """
        Iterates over all active files and saves their states,
        skipping files that are completely downloaded.

        Args:
            files (dict[str, File]): Dictionary mapping filenames to File objects.
        """
        for _, file in list(files.items()):
            # Only save state if at least one chunk is not completely finished
            if not all(c.current_pos > c.end for c in (file.chunks or [])):
                self.save_state(file)

    def load_state(self, filename: str) -> File | None:
        """
        Attempts to recover the download state from a JSON file.

        It ensures that both the state tracker file and the actual partial
        data file exist on disk before attempting deserialization.

        Args:
            filename (str): The name of the target file.

        Returns:
            File | None: An instantiated File object if recovery is successful,
                         or None if the state is missing or corrupted.
        """
        state_path = self.get_state_path(filename)
        file_path = self.out_dir / filename

        if state_path.is_file() and file_path.is_file():
            with state_path.open("rb") as f:
                content = f.read()
                return File.from_json(content) if content else None
        return None

    def delete_state(self, filename: str) -> None:
        """
        Silently removes the state tracking file once a download is complete.

        Args:
            filename (str): The name of the target file.
        """
        self.get_state_path(filename).unlink(missing_ok=True)

    def verify_size(self, file: File) -> None:
        """
        Verifies that the physical file size on disk matches the expected Content-Length.

        Args:
            file (File): The File object containing metadata.

        Raises:
            ValueError: If there is a mismatch between expected and actual file size.
        """
        file_path = self.out_dir / file.filename

        if file_path.is_file():
            actual_size = file_path.stat().st_size
            expected_size = file.content_length

            if expected_size and actual_size != expected_size:
                err_msg = (
                    f"Size mismatch for {file.filename}: "
                    f"Expected {expected_size} bytes, got {actual_size} bytes."
                )
                raise ValueError(err_msg)

    def verify_file_hash(self, file: File) -> None:
        """
        Calculates the MD5 checksum of the downloaded file and compares it
        against the expected hash.

        Note: This is a synchronous, CPU/Disk-bound operation designed to be
        executed within a thread pool (run_in_executor) to prevent Event Loop blocking.

        Args:
            file (File): The File object containing the expected MD5 hash.

        Raises:
            ValueError: If the calculated hash does not match the expected hash.
        """
        if not file or not file.expected_md5:
            return

        filepath = self.out_dir / file.filename
        if not filepath.exists():
            return

        # Compute MD5 by reading in 4MB chunks to conserve RAM
        hash_md5 = hashlib.md5()
        with filepath.open("rb") as f:
            for chunk in iter(lambda: f.read(4096 * 1024), b""):
                hash_md5.update(chunk)

        calculated = hash_md5.hexdigest()

        if calculated != file.expected_md5:
            err_msg = (
                f"CRITICAL: Hash mismatch for {file.filename}!\n"
                f"Expected: {file.expected_md5}\n"
                f"Got:      {calculated}"
            )

            # TODO: Consider adding logic to automatically quarantine
            # or delete the corrupted file here.
            # filepath.unlink(missing_ok=True)

            raise ValueError(err_msg)

    def verify_stream(
        self, md5_hasher: HASH, expected_checksum: str, next_offset: int, total_size: int
    ) -> None:
        """
        Validates the integrity of an in-memory data stream immediately after completion.
        Checks both the cryptographic hash and the total byte count.

        Args:
            md5_hasher (HASH): The hashlib object populated with stream data.
            expected_checksum (str): The MD5 hash fetched from the provider.
            next_offset (int): Total bytes yielded to the consumer.
            total_size (int): Expected Content-Length of the stream.

        Raises:
            ValueError: If either the MD5 checksum or the byte count is invalid.
        """
        calculated = md5_hasher.hexdigest()
        if calculated != expected_checksum:
            err_msg = (
                f"CRITICAL: Stream Integrity Check Failed!\n"
                f"Expected MD5: {expected_checksum}\n"
                f"Got MD5:      {calculated}"
            )
            raise ValueError(err_msg)

        if next_offset != total_size:
            raise ValueError(
                f"Incomplete stream data! Yielded {next_offset} bytes, but expected {total_size} bytes."
            )
