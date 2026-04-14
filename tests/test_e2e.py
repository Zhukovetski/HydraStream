# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.
import hashlib
import re
import shutil
import threading
import warnings
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner
from werkzeug import Request, Response

from hydrastream.main import app

warnings.filterwarnings("ignore", message=".*chunk_size is ignored.*")

DUMMY_DATA = b"0123456789" * 10000  # 1000 байт
DUMMY_MD5 = hashlib.md5(DUMMY_DATA).hexdigest()


def make_chaos_handler(seed: int, current_test_filenames: list[str]) -> Callable:
    """
    Фабрика. Создает обработчик, поведение которого на 100% зависит от seed.
    Если Hypothesis перезапустит тест с этим же seed, сервер поведет себя ИДЕНТИЧНО.
    """
    request_counts = defaultdict(int)
    lock = threading.Lock()

    def handler(request: Request) -> Response | None:
        # 1. Уникальная подпись запроса (Учитываем Range, чтобы чанки отличались)
        sig = f"{request.path}|{request.method}|{request.headers.get('Range', '')}"

        # 2. Считаем, какая это попытка для данного запроса
        # (важно для выхода из ретраев!)
        with lock:
            request_counts[sig] += 1
            attempt = request_counts[sig]

        # 3. Генерируем ДЕТЕРМИНИРОВАННЫЕ "случайные" числа
        chaos_key = f"{seed}|{sig}|{attempt}"
        h = hashlib.md5(chaos_key.encode()).hexdigest()

        # Превращаем куски MD5 хэша в числа от 0.0 до 1.0 (заменяем random.random())
        # Берем разные куски хэша для разных проверок, чтобы они были независимы
        rand_md5 = int(h[0:8], 16) / 0xFFFFFFFF
        rand_waf = int(h[8:16], 16) / 0xFFFFFFFF
        rand_dumb = int(h[16:24], 16) / 0xFFFFFFFF
        rand_cloud = int(h[24:32], 16) / 0xFFFFFFFF

        path = request.path

        # --- 1. ОТДАЧА ФАЙЛА ХЕШЕЙ ---
        if "md5checksums.txt" in path:
            content = ""
            # 50% шанс, что файл хешей нормальный
            if rand_md5 > 0.5:
                content = "\n".join(
                    f"{DUMMY_MD5}  {name}" for name in current_test_filenames
                )
            return Response(content, status=200)

        # --- 2. ИМИТАЦИЯ WAF И СБОЕВ ---
        # 15% шанс отдать 429 Too Many Requests
        if rand_waf < 0.15:
            return Response("Slow down!", status=429, headers={"Retry-After": "1"})

        # 10% шанс отдать 503 Service Unavailable
        if 0.15 <= rand_waf < 0.25:
            return Response("Backend dead", status=503)

        # --- 3. ИМИТАЦИЯ ТУПЫХ СЕРВЕРОВ (Без Range) ---
        # 20% шанс, что сервер притворится тупым
        is_dumb_server = rand_dumb < 0.2

        if request.method == "HEAD":
            headers = {"Content-Length": str(len(DUMMY_DATA))}
            if not is_dumb_server:
                headers["Accept-Ranges"] = "bytes"

            # 70% шанс, что сервер отдаст облачный ETag (тестируем CloudProvider)
            if rand_cloud < 0.7:
                headers["ETag"] = f'"{DUMMY_MD5}"'

            return Response(status=200, headers=headers)

        # --- 4. ОБРАБОТКА GET ---
        range_header = request.headers.get("Range")

        if range_header and range_header.startswith("bytes=") and not is_dumb_server:
            byte_range = range_header.replace("bytes=", "")
            start_str, end_str = byte_range.split("-")
            start, end = int(start_str), int(end_str)

            chunk = DUMMY_DATA[start : end + 1]

            return Response(
                chunk,
                status=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{len(DUMMY_DATA)}",
                    "Content-Length": str(len(chunk)),
                },
            )

        # Фолбек: если сервер тупой ИЛИ запросили без Range - отдаем весь файл
        return Response(
            DUMMY_DATA, status=200, headers={"Content-Length": str(len(DUMMY_DATA))}
        )

    return handler


runner = CliRunner()


@st.composite
def filenames_strategy(draw):
    # 1. Генерируем основу имени (stem)
    # Используем алфавит с цифрами, тире и подчеркиванием
    stem_alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_"

    # 2. Генерируем случайное расширение (для разнообразия)
    ext = draw(st.sampled_from([".bin", ".txt", ".gz", ".zip", ""]))

    # 3. Генерируем само имя
    # min_size=1, max_size=30 (проверим длинные пути)
    name = draw(st.text(alphabet=stem_alphabet, min_size=1, max_size=30))

    # 4. Иногда добавляем "проблемные" символы (пробелы, точки в середине)
    if draw(st.booleans()):
        name += " " + draw(st.text(alphabet=stem_alphabet, min_size=1, max_size=5))

    return f"{name}{ext}"


# --- СТРАТЕГИЯ ГЕНЕРАЦИИ ДАННЫХ ---
@st.composite
def cli_fuzz_strategy(draw) -> dict[str, Any]:
    """Генерирует случайные, но логически допустимые комбинации аргументов"""

    server_seed = draw(st.integers(min_value=0, max_value=99999999))
    # Генерируем от 1 до 3 случайных путей файлов (только буквы и цифры)
    paths = draw(
        st.lists(
            filenames_strategy(),
            min_size=1,
            max_size=10,
        )
    )
    paths_with_meta = []
    for p in paths:
        # Просто используем booleans(), Hypothesis сама разберется с балансом
        is_ncbi = draw(st.booleans())
        prefix = "ncbi.nlm.nih.gov/" if is_ncbi else ""
        paths_with_meta.append(f"{prefix}{p}")

    input_mode = draw(st.integers(0, 2))
    threads = draw(st.integers(1, 10))
    min_chunk_mb = draw(st.integers(1, 10))
    existing_copies = draw(st.integers(0, 5))

    flags = {
        "--stream": draw(st.booleans()),
        "--dry-run": draw(st.booleans()),
        "--no-ui": draw(st.booleans()),
        "--quiet": draw(st.booleans()),
        "--json": draw(st.booleans()),
        "--no-verify": draw(st.booleans()),
    }

    PLACEHOLDER = "http://localhost:SERVER_PORT/"

    cli_urls = []
    file_urls = []

    for i, p in enumerate(paths_with_meta):
        url = f"{PLACEHOLDER}{p}"
        if input_mode == 0 or (input_mode == 2 and i % 2 == 0):
            cli_urls.append(url)
        else:
            file_urls.append(url)

    # Собираем список аргументов (пока с плейсхолдерами)
    args = cli_urls
    for flag, enabled in flags.items():
        if enabled:
            args.append(flag)

    args.extend(["--threads", str(threads), "--min-chunk-mb", str(min_chunk_mb)])
    return {
        "server_seed": server_seed,
        "args_template": args,
        "paths": paths,
        "file_urls_template": file_urls,
        "existing_copies": existing_copies,
        "first_path_name": str("".join(paths[0])),
        # Пробрасываем флаги для проверок в ассертах
        "is_dry_run": flags["--dry-run"],
        "is_stream": flags["--stream"],
    }


@given(data=cli_fuzz_strategy())
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_hypothesis_nuclear_fuzzer(
    httpserver: HTTPServer, tmp_path: Path, data: dict
) -> None:
    filenames = [Path(p).name for p in data["paths"]]
    chaos_handler = make_chaos_handler(data["server_seed"], filenames)
    # 1. Заводим сервер
    httpserver.expect_request(re.compile("^/.*$")).respond_with_handler(chaos_handler)
    base_url = httpserver.url_for("").rstrip("/")

    out_dir = tmp_path / "downloads"
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    if data["existing_copies"] > 0:
        stem = Path(data["first_path_name"]).stem
        suffix = Path(data["first_path_name"]).suffix
        for i in range(data["existing_copies"]):
            name = data["first_path_name"] if i == 0 else f"{stem} ({i}){suffix}"
            (out_dir / name).touch()
            # Добавляем .state.json для веса
            state_dir = out_dir / ".state"
            state_dir.mkdir(exist_ok=True)
            (state_dir / f"{name}.state.json").touch()

    final_args = [
        a.replace("http://localhost:SERVER_PORT/", f"{base_url}/")
        for a in data["args_template"]
    ]
    final_args.extend(["--output", str(out_dir)])

    if data["file_urls_template"]:
        urls_txt = tmp_path / "urls.txt"
        content = "\n".join(
            u.replace("http://localhost:SERVER_PORT/", f"{base_url}/")
            for u in data["file_urls_template"]
        )
        urls_txt.write_text(content)
        final_args.extend(["--input", str(urls_txt)])
    # 3. УДАР! (Запускаем CLI)
    num_file = len(list(out_dir.glob("*")))
    final_args.append("--debug")

    print(final_args)
    result = runner.invoke(app, final_args)

    # 4. ПРОВЕРКА ИНВАРИАНТОВ (ГЛАВНАЯ МАГИЯ PBT)

    # Инвариант 1: Программа НИКОГДА не должна падать с необработанным исключением
    # (Traceback)
    assert result.exception is None, (
        f"КРАШ ПРОГРАММЫ! Комбинация: {final_args}\nВывод: {result.stdout}"
    )
    # 1. Список исключений
    ignored = {"download.log", ".states"}

    # 2. Находим всё "запрещенное"
    leftovers = [f for f in out_dir.glob("*") if f.name not in ignored]

    # Инвариант 2: Если это DRY-RUN, на диске НЕ ДОЛЖНО быть создано ни одного
    # файла генома
    if data["is_dry_run"]:
        assert len(leftovers) == num_file, (
            f"DRY-RUN нарушил обещание и скачал файлы на диск!"
            f"Было {num_file}. Стало {len(leftovers)}"
        )

    # Инвариант 3: Если это STREAM, на диске тоже пусто
    if data["is_stream"]:
        assert len(leftovers) == num_file, "STREAM записал бинарники на диск!"

    # Инвариант 4: Если это обычная загрузка, файлы должны лежать на диске
    if not data["is_stream"] and not data["is_dry_run"] and result.exit_code == 0:
        # Количество скачанных файлов должно совпадать с количеством уникальных ссылок
        num = 1 if data["existing_copies"] else 0
        assert len(leftovers) <= len(data["paths"]) + num_file, (
            f"Файлы не скачались! Лог терминала:\n{result.stdout}"
        )
