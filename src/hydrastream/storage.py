# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import hashlib
import os
import shutil
import tempfile
from _hashlib import HASH
from copy import deepcopy
from pathlib import Path

from hydrastream.monitor import log

from .models import File, HydraContext, StorageState


def get_unique_path(file_path: Path) -> Path:
    if not file_path.is_file():
        return file_path

    stem = file_path.stem
    suffix = file_path.suffix
    directory = file_path.parent

    counter = 1

    while True:
        new_name = f"{stem} ({counter}){suffix}"
        new_path = directory / new_name

        if not new_path.is_file():
            return new_path

        counter += 1


def create_sparse_file(ctx: StorageState, filename: str, size: int) -> str | None:
    """
    Allocates disk space for a file using OS-level truncation.
    This prevents disk fragmentation and allows atomic random-access writes.

    Args:
        filename (str): Name of the file to create.
        size (int): Target size in bytes.
    """
    free_space = shutil.disk_usage(ctx.out_dir).free
    if free_space < size:
        raise OSError(
            f"Insufficient disk space. "
            f"Required: {size / (1024**2):.2f} MB,"
            f" Available: {free_space / (1024**2):.2f} MB."
        )
    filepath = ctx.out_dir / filename

    if filepath.is_file():
        filepath = get_unique_path(filepath)

    with filepath.open("wb") as f:
        f.truncate(size)

    if filepath.name != filename:
        return filepath.name
    return None


def open_file(ctx: StorageState, filename: str) -> int:
    """
    Opens a file at the OS level for random-access read/write.

    Args:
        filename (str): Name of the file.

    Returns:
        int: The OS file descriptor (fd).
    """
    filepath = ctx.out_dir / filename
    # os.pwrite is atomic and thread-safe for offset-based writing
    return os.open(filepath, os.O_RDWR)


async def write_chunk_data(fd: int, data: bytearray, offset: int) -> None:
    """
    Asynchronously writes a byte array to a specific file offset using
    a thread pool to prevent blocking the asyncio Event Loop.

    Args:
        fd (int): The OS file descriptor.
        data (bytearray): The raw bytes to write.
        offset (int): The absolute byte position in the file.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, os.pwrite, fd, data, offset)


def get_state_path(ctx: StorageState, filename: str) -> Path:
    """Constructs the path for the JSON state file."""
    return ctx.state_dir / f"{filename}.state.json"


def save_state(ctx: StorageState, file_obj: File) -> None:
    """Serializes and saves a single File object state to disk."""
    path = Path(get_state_path(ctx, file_obj.meta.filename))
    temp_dir = path.parent
    temp_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile("wb", dir=temp_dir, delete=False) as tf:
        tf.write(file_obj.to_json())
        tf.flush()
        os.fsync(tf.fileno())
        temp_path = Path(tf.name)

    try:
        Path.replace(temp_path, path)
        if os.name != "nt":
            dir_fd = os.open(str(temp_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    except Exception:
        if Path.exists(temp_path):
            Path.unlink(temp_path)
        raise


def save_all_states(ctx: StorageState, files: dict[str, File]) -> None:
    """
    Iterates over all active files and saves their states,
    skipping files that are completely downloaded.

    Args:
        files (dict[str, File]): Dictionary mapping filenames to File objects.
    """
    files_snapshot = deepcopy(files)
    for file in files_snapshot.values():
        # Only save state if at least one chunk is not completely finished
        if not all(c.current_pos > c.end for c in (file.chunks or [])):
            save_state(ctx, file)


async def autosave(ctx: HydraContext, interval: int) -> None:
    loop = asyncio.get_running_loop()
    while ctx.is_running:
        try:
            await asyncio.sleep(interval)
            await loop.run_in_executor(None, save_all_states, ctx.fs, ctx.files)
        except asyncio.CancelledError:
            break
        except Exception as e:
            await log(ctx.ui, f"Auto-save operation failed: {e}", status="ERROR")


def load_state(ctx: StorageState, filename: str) -> tuple[File | None, int]:
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
    search_pattern = f"{filename}*.state.json"
    states = list(ctx.state_dir.glob(search_pattern))

    if not states:
        return None, 0

    states.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    state_path = states[0]
    try:
        with state_path.open("rb") as f:
            content = f.read()
        file = File.from_json(content) if content else None
    except Exception:
        return None, len(states)

    if not file:
        return None, len(states)

    if (ctx.out_dir / file.meta.filename).is_file():
        return file, len(states)

    return None, len(states)


def delete_state(ctx: StorageState, filename: str) -> None:
    """
    Silently removes the state tracking file once a download is complete.

    Args:
        filename (str): The name of the target file.
    """
    get_state_path(ctx, filename).unlink(missing_ok=True)


def verify_size(ctx: StorageState, file: File) -> None:
    """
    Verifies that the physical file size on disk matches the expected Content-Length.

    Args:
        file (File): The File object containing metadata.

    Raises:
        ValueError: If there is a mismatch between expected and actual file size.
    """
    file_path = ctx.out_dir / file.meta.filename

    if file_path.is_file():
        actual_size = file_path.stat().st_size
        expected_size = file.meta.content_length

        if expected_size and actual_size != expected_size:
            err_msg = (
                f"Size mismatch for {file.meta.filename}: "
                f"Expected {expected_size} bytes, got {actual_size} bytes."
            )
            raise ValueError(err_msg)


def verify_file_hash(ctx: StorageState, file: File) -> None:
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
    if not file or not file.meta.expected_md5:
        return

    filepath = ctx.out_dir / file.meta.filename
    if not filepath.exists():
        return

    # Compute MD5 by reading in 4MB chunks to conserve RAM
    hash_md5 = hashlib.md5()
    with filepath.open("rb") as f:
        for chunk in iter(lambda: f.read(4096 * 1024), b""):
            hash_md5.update(chunk)

    calculated = hash_md5.hexdigest()

    if calculated != file.meta.expected_md5:
        err_msg = (
            f"CRITICAL: Hash mismatch for {file.meta.filename}!\n"
            f"Expected: {file.meta.expected_md5}\n"
            f"Got:      {calculated}"
        )

        filepath.unlink(missing_ok=True)

        raise ValueError(err_msg)


def verify_stream(
    md5_hasher: HASH, expected_checksum: str, next_offset: int, total_size: int
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
            f"Incomplete stream data! Yielded {next_offset} bytes,"
            f" but expected {total_size} bytes."
        )
