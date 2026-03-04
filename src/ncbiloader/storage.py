# storage.py
import asyncio
import hashlib
import os
from _hashlib import HASH
from pathlib import Path

from .models import File


class StorageManager:
    def __init__(self, output_dir: str) -> None:
        self.out_dir = Path(output_dir).expanduser().resolve()
        self.state_dir = self.out_dir / ".states"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def create_sparse_file(self, filename: str, size: int) -> None:
        filepath = self.out_dir / filename
        with filepath.open("wb") as f:
            f.truncate(size)

    def open_file(self, filename: str) -> int:
        filepath = self.out_dir / filename
        return os.open(filepath, os.O_RDWR)

    async def write_chunk_data(self, fd: int, data: bytearray, offset: int) -> None:
        """Пишет кусок в файл асинхронно"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.pwrite, fd, data, offset)

    def get_state_path(self, filename: str) -> Path:
        return self.state_dir / f"{filename}.state.json"

    def save_state(self, file_obj: File) -> None:
        path = self.get_state_path(file_obj.filename)
        path.write_bytes(file_obj.to_json())

    def save_all_states(self, files: dict[str, File]) -> None:
        for _, file in list(files.items()):
            if not all(c.current_pos > c.end for c in (file.chunks or [])):
                self.save_state(file)

    def load_state(self, filename: str) -> File | None:
        state_path = self.get_state_path(filename)
        file_path = self.out_dir / filename
        if state_path.is_file() and file_path.is_file():
            with state_path.open("rb") as f:
                content = f.read()
                return File.from_json(content) if content else None
        return None

    def delete_state(self, filename: str) -> None:
        self.get_state_path(filename).unlink(missing_ok=True)

    def verify_size(self, file: File) -> None:
        file_path = self.out_dir / file.filename
        # 1. Проверяем физический размер файла
        if file_path.is_file():
            actual_size = file_path.stat().st_size
            expected_size = file.content_length

            if expected_size and actual_size != expected_size:
                err_msg = f"[!] Файл битый: {file.filename} ({actual_size} != {expected_size})"

                raise ValueError(err_msg)

    def verify_file_hash(self, file: File) -> None:
        """Синхронный метод для запуска в экзекуторе"""
        if not file or not file.expected_md5:
            return

        filepath = self.out_dir / file.filename
        if not filepath.exists():
            return

        # Считаем MD5
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

            raise ValueError(err_msg)
            # Можно удалить битый файл
            # os.remove(filepath)
            # И пометить в file_obj, что он битый, чтобы run() выбросил ошибку в конце

    def verify_stream(
        self, md5_hasher: HASH, expected_checksum: str, next_offset: int, total_size: int
    ) -> None:
        calculated = md5_hasher.hexdigest()
        if calculated != expected_checksum:
            err_msg = f"CRITICAL: Integrity Check Failed!\nExpected: {expected_checksum}\nGot: {calculated}"

            # Бросаем исключение. Это прервет consumer-а.
            raise ValueError(err_msg)

        if next_offset != total_size:
            raise ValueError(f"Incomplete stream! Got {next_offset} of {total_size}")
