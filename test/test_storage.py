import hashlib
import os
from pathlib import Path

import pytest

from ncbiloader.models import File
from ncbiloader.storage import StorageManager


def test_storage_creates_directories(tmp_path: Path) -> None:
    """Check that the storage creates the correct folder structure"""
    storage = StorageManager(output_dir=str(tmp_path))

    assert storage.out_dir.exists()
    assert storage.state_dir.exists()
    assert storage.state_dir.name == ".states"


@pytest.mark.asyncio
async def test_write_chunk_data_out_of_order(tmp_path: Path) -> None:
    """
    We prove that our system can write pieces of a file randomly
    (first the end, then the beginning), and the file will assemble correctly.
    """
    storage = StorageManager(output_dir=str(tmp_path))
    filename = "genome_test.bin"
    file_size = 10

    storage.create_sparse_file(filename, file_size)

    fd = storage.open_file(filename)

    try:
        await storage.write_chunk_data(fd, b"B", 5)
        await storage.write_chunk_data(fd, b"A", 0)
        await storage.write_chunk_data(fd, b"C", 9)
    finally:
        os.close(fd)

    final_file = tmp_path / filename
    content = final_file.read_bytes()

    assert len(content) == 10
    assert content[0:1] == b"A"
    assert content[5:6] == b"B"
    assert content[9:10] == b"C"

    assert content[1:5] == b"\x00\x00\x00\x00"


def test_save_and_load_state(tmp_path: Path) -> None:
    """We check that StorageManager is correctly searching for and loading .state.json"""
    storage = StorageManager(output_dir=str(tmp_path))

    file_obj = File(filename="data.tar", url="http://", content_length=1000, chunk_size=500)
    (tmp_path / "data.tar").touch()

    storage.save_state(file_obj)

    assert (tmp_path / ".states" / "data.tar.state.json").exists()

    loaded_file, count = storage.load_state("data.tar")

    assert loaded_file is not None
    assert count == 1
    assert loaded_file.filename == "data.tar"
    assert len(loaded_file.chunks) == 2


def test_verify_file_hash(tmp_path: Path) -> None:
    """We check that the cryptographic verification fails on broken files"""
    storage = StorageManager(output_dir=str(tmp_path))

    real_content = b"The quick brown fox jumps over the lazy dog"
    real_md5 = hashlib.md5(real_content).hexdigest()

    (tmp_path / "fox.txt").write_bytes(real_content)

    good_file = File(
        filename="fox.txt", url="", content_length=len(real_content), chunk_size=10, expected_md5=real_md5
    )
    storage.verify_file_hash(good_file)

    bad_file = File(
        filename="fox.txt",
        url="",
        content_length=len(real_content),
        chunk_size=10,
        expected_md5="1234567890abcdef",
    )

    with pytest.raises(ValueError, match="Hash mismatch for fox.txt"):
        storage.verify_file_hash(bad_file)
