from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from .models import File, TypeHash


class StorageBackend(Protocol):
    def allocate_space(self, filename: str, size: int) -> str | None: ...

    def open_file(self, filename: str) -> Any: ...

    async def write_chunk_data(
        self, fd_or_conn: Any, data: bytearray, offset: int
    ) -> None: ...

    def close_file(self, fd_or_conn: Any) -> None: ...

    def delete_file(self, filename: str) -> None: ...

    def save_state(self, file_obj: File) -> None: ...

    def load_state(self, filename: str) -> tuple[File | None, int]: ...

    def delete_state(self, filename: str) -> None: ...

    def verify_size(self, filename: str, expected_size: int) -> bool: ...

    async def verify_file_hash(
        self, filename: str, expected_checksum: str, algorithm: TypeHash
    ) -> None: ...

    def get_unique_path(self, file_path: Any) -> Any: ...

    def get_state_path(self, filename: str) -> Any: ...


class LocalStorageManager:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = Path(out_dir).expanduser().resolve()
        self.state_dir = self.out_dir / ".states"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def allocate_space(self, filename: str, size: int) -> str | None:
        free_space = shutil.disk_usage(self.out_dir).free
        if free_space < size:
            raise OSError(
                f"Insufficient disk space. "
                f"Required: {size / (1024**2):.2f} MB,"
                f" Available: {free_space / (1024**2):.2f} MB."
            )
        filepath = self.out_dir / filename

        if filepath.is_file():
            filepath = self.get_unique_path(filepath)

        with filepath.open("wb") as f:
            f.truncate(size)

        if filepath.name != filename:
            return filepath.name
        return None

    def open_file(self, filename: str) -> int:
        filepath = self.out_dir / filename
        return os.open(filepath, os.O_RDWR)

    async def write_chunk_data(
        self, fd_or_conn: int, data: bytearray, offset: int
    ) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, os.pwrite, fd_or_conn, data, offset)

    def close_file(self, fd_or_conn: int) -> None:
        with contextlib.suppress(OSError):
            os.close(fd_or_conn)

    def delete_file(self, filename: str) -> None:
        filepath = self.out_dir / filename
        filepath.unlink(missing_ok=True)

    def save_state(self, file_obj: File) -> None:
        path = Path(self.get_state_path(file_obj.meta.filename))
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

    def load_state(self, filename: str) -> tuple[File | None, int]:
        p = Path(filename)
        main_name = p.stem  # "GCF_..._genomic.fna"
        last_ext = p.suffix  # ".gz"

        # Экранируем обе части
        safe_main = re.escape(main_name)
        safe_ext = re.escape(last_ext)

        # Паттерн: Название + (число) + расширение + .state.json
        pattern = re.compile(rf"^{safe_main}(?: \((\d+)\))?{safe_ext}\.state\.json$")

        found_states: list[tuple[Path, int]] = []
        for f in self.state_dir.iterdir():
            match = pattern.match(f.name)
            if match:
                # Если скобок нет (оригинал), считаем номер 0
                # Если есть, берем число из первой группы
                counter = int(match.group(1)) if match.group(1) else 0
                found_states.append((f, counter))

        if not found_states:
            return None, 0

        # Сортируем по числу (второй элемент кортежа) и берем самый большой
        state_path, _ = max(found_states, key=lambda x: x[1])

        try:
            with state_path.open("rb") as f:
                content = f.read()
            file = File.from_json(content) if content else None
        except Exception:
            return None, len(found_states)

        if not file:
            return None, len(found_states)

        if (self.out_dir / file.meta.filename).is_file():
            return file, len(found_states)

        return None, len(found_states)

    def delete_state(self, filename: str) -> None:
        self.get_state_path(filename).unlink(missing_ok=True)

    def verify_size(self, filename: str, expected_size: int) -> bool:

        file_path = self.out_dir / filename

        if file_path.is_file():
            actual_size = file_path.stat().st_size

            if expected_size and actual_size != expected_size:
                err_msg = (
                    f"Size mismatch for {filename}: "
                    f"Expected {expected_size} bytes, got {actual_size} bytes."
                )
                raise ValueError(err_msg)
            return True
        raise OSError(f"File: {filename} not found")

    async def verify_file_hash(
        self, filename: str, expected_checksum: str, algorithm: TypeHash
    ) -> None:
        if not expected_checksum:
            return

        def _compute_hash(algorithm: TypeHash) -> str:
            filepath = self.out_dir / filename
            if not filepath.exists():
                raise OSError(f"File: {filename} not found")
            with filepath.open("rb") as f:
                digest = hashlib.file_digest(f, algorithm)
                return digest.hexdigest()

        loop = asyncio.get_running_loop()
        calculated = await loop.run_in_executor(None, _compute_hash, algorithm)

        if calculated != expected_checksum:
            filepath = self.out_dir / filename
            err_msg = (
                f"CRITICAL: Hash mismatch for {filename}!\n"
                f"Expected: {algorithm} "
                f"{expected_checksum}\n"
                f"Got:      {calculated}"
            )
            filepath.unlink(missing_ok=True)
            raise ValueError(err_msg)

    def get_unique_path(self, file_path: Path) -> Path:
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

    def get_state_path(self, filename: str) -> Path:
        return self.state_dir / f"{filename}.state.json"
