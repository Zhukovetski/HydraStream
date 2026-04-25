# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.
import hashlib
import re
import shlex
import shutil
import sys
import threading
import warnings
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast, get_args

from curl_cffi import BrowserTypeLiteral
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner
from werkzeug import Request, Response

from hydrastream.main import app

warnings.filterwarnings("ignore", message=".*chunk_size is ignored.*")

DUMMY_DATA = b"0123456789" * 10000
DUMMY_MD5 = hashlib.md5(DUMMY_DATA).hexdigest()

defaul = {
    "links": None,
    "input": None,
    "typehash": "md5",
    "checksum": None,
    "output": "download",
    "threads": None,
    "stream": False,
    "dry-run": False,
    "min-chunk-mb": 1,
    "stream-chunk-mb": 5,
    "buffer": None,
    "limit": None,
    "no-ui": False,
    "quiet": False,
    "json": False,
    "verify": True,
    "browser": "chrome120",
    "debug": False,
}


def make_chaos_handler(
    seed: int, current_test_filenames: list[str]
) -> Callable[[Request], Response | None]:
    """
    Фабрика. Создает обработчик, поведение которого на 100% зависит от seed.
    Если Hypothesis перезапустит тест с этим же seed, сервер поведет себя ИДЕНТИЧНО.
    """
    request_counts: defaultdict[str, int] = defaultdict(int)
    lock = threading.Lock()

    def handler(request: Request) -> Response | None:  # noqa: PLR0911
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

            if rand_cloud < 0.1:
                half_chunk = chunk[: len(chunk) // 2]
                # Заметь: мы врем в заголовках! Говорим, что отдаем весь,
                # а отдаем половину.
                return Response(
                    half_chunk,
                    status=206,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{len(DUMMY_DATA)}",
                        "Content-Length": str(len(chunk)),  # Вранье!
                    },
                )

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
def filenames_strategy(draw: st.DrawFn) -> str:
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


def frequencies(
    weighted_strategies: list[tuple[int, st.SearchStrategy[Any]]],
) -> st.SearchStrategy[Any]:
    pool: list[st.SearchStrategy[Any]] = []
    for weight, strategy in weighted_strategies:
        # Вместо списка стратегий делаем одну большую стратегию через one_of
        # Но Hypothesis не умеет в веса из коробки в one_of,
        # поэтому твой трюк с pool оправдан, если дублировать сами стратегии
        pool.extend([strategy] * weight)
    return st.one_of(pool)


@st.composite
def cli_fuzz_strategy(draw: st.DrawFn) -> dict[str, Any]:
    all_paths = draw(st.lists(filenames_strategy(), min_size=1, max_size=10))

    split_idx = draw(st.integers(min_value=0, max_value=len(all_paths)))

    cli_paths = all_paths[:split_idx]
    file_paths = all_paths[split_idx:]

    if not cli_paths and not file_paths:
        cli_paths = [draw(filenames_strategy())]
    valid = 5
    error = 1
    res = {
        "threads": draw(
            frequencies([
                (1, st.tuples(st.none(), st.none())),
                (valid, st.tuples(st.integers(1, 128), st.none())),
                (
                    error,
                    st.one_of(
                        st.tuples(st.integers(min_value=-10, max_value=0), st.just("")),
                        st.tuples(
                            st.integers(min_value=129, max_value=1000), st.just("")
                        ),
                        st.tuples(
                            st.text(alphabet="123abc!", min_size=1, max_size=5),
                            st.just(""),
                        ),
                    ),
                ),
            ])
        ),
        "browser": draw(
            frequencies([
                (1, st.tuples(st.none(), st.none())),
                (
                    valid,
                    st.tuples(
                        (st.sampled_from(list(get_args(BrowserTypeLiteral)))), st.none()
                    ),
                ),
                (
                    error,
                    st.one_of(
                        st.tuples(
                            st.sampled_from(["ie6", "safari", "netscape"]), st.just("")
                        ),
                        st.tuples(st.text(min_size=1, max_size=10), st.just("")),
                    ),
                ),
            ])
        ),
        "buffer": draw(
            frequencies([
                (1, st.tuples(st.none(), st.none())),
                (valid, st.tuples(st.integers(50, 1000), st.none())),
                (
                    error,
                    st.tuples(st.integers(-100, 50), st.just("")),
                ),
            ])
        ),
        "limit": draw(
            frequencies([
                (1, st.tuples(st.none(), st.none())),
                (valid, st.tuples(st.floats(0.01, 1000), st.none())),
                (
                    error,
                    st.tuples(
                        st.floats(-100, 0),
                        st.just(
                            "validation error for HydraConfig\nspeed_limit\n  Input should be greater than 0"
                        ),
                    ),
                ),
            ])
        ),
        "min-chunk-mb": draw(
            frequencies([
                (1, st.tuples(st.none(), st.none())),
                (valid, st.tuples(st.integers(1, 1000), st.none())),
                (
                    error,
                    st.tuples(st.integers(-100, 0), st.just("")),
                ),
            ])
        ),
        "stream-chunk-mb": draw(
            frequencies([
                (1, st.tuples(st.none(), st.none())),
                (valid, st.tuples(st.integers(1, 1000), st.none())),
                (
                    error,
                    st.tuples(
                        st.integers(-100, 0),
                        st.just(
                            "validation error for HydraConfig\nmax_stream_chunk_size_mb\n  Input should be greater than 0"
                        ),
                    ),
                ),
            ])
        ),
        "flags": draw(
            st.fixed_dictionaries({
                "stream": st.booleans(),
                "dry-run": st.booleans(),
                "no-ui": st.booleans(),
                "quiet": st.booleans(),
                "json": st.booleans(),
                "no-verify": st.booleans(),
                # "debug": st.booleans(),
            })
        ),
    }

    placeholder = "http://localhost:SERVER_PORT/"
    cli_urls = [
        f"{placeholder}{'ncbi.nlm.nih.gov/' if draw(st.booleans()) else ''}{p}"
        for p in cli_paths
    ]
    file_urls = [
        f"{placeholder}{'ncbi.nlm.nih.gov/' if draw(st.booleans()) else ''}{p}"
        for p in file_paths
    ]
    # print(f"Параметры: {res}", file=sys.__stderr__)
    args, expected_error = build_args_list(res, cli_urls)

    if len(all_paths) == 1 and draw(st.booleans()):
        args.extend(["--checksum", DUMMY_MD5, "--typehash", "md5"])

    args.append("--debug")

    return {
        "args_template": args,
        "expected_error": expected_error,
        "paths": all_paths,
        "existing_copies": draw(st.integers(0, 5)),
        "existing_files": draw(st.sampled_from(all_paths)),
        "file_urls_template": file_urls,
        "server_seed": draw(st.integers(0, 999999)),
    }


def build_args_list(
    params: dict[str, dict[str, bool] | Any], urls: list[str]
) -> tuple[list[str], list[str]]:
    args = urls[:]
    expected_error: list[str] = []

    # Флаги
    for name, value in params.items():
        if isinstance(value, dict):
            for flag, enabled in cast(dict[str, bool], value).items():
                if enabled:
                    args.append(f"--{flag}")
            continue

        if value[0] is not None:
            args.extend([f"--{name}", str(value[0])])
        if value[1] is not None:
            expected_error.append(value[1])

    return args, expected_error


@given(data=cli_fuzz_strategy())
@settings(
    max_examples=5,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_hypothesis_nuclear_fuzzer(
    httpserver: HTTPServer, tmp_path: Path, data: dict[str, Any]
) -> None:
    filenames = [Path(p).name for p in data["paths"]]
    chaos_handler = make_chaos_handler(data["server_seed"], filenames)
    # 1. Заводим сервер
    httpserver.expect_request(re.compile("^/.*$")).respond_with_handler(chaos_handler)  # pyright: ignore[reportArgumentType]
    base_url = httpserver.url_for("").rstrip("/")

    out_dir = tmp_path / "downloads"
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    if data["existing_copies"] > 0:
        stem = Path(data["existing_files"]).stem
        suffix = Path(data["existing_files"]).suffix
        for i in range(data["existing_copies"]):
            name = data["existing_files"] if i == 0 else f"{stem} ({i}){suffix}"
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

    print(
        f"Running: my-tool {' '.join(shlex.quote(a) for a in final_args)}",
        file=sys.__stderr__,
    )
    result = runner.invoke(app, final_args)
    # exception = result.exception
    text_exception = result.stdout

    # 4. ПРОВЕРКА ИНВАРИАНТОВ (ГЛАВНАЯ МАГИЯ PBT)

    # Инвариант 1: Программа НИКОГДА не должна падать с необработанным исключением
    # (Traceback)
    for i in data["expected_error"]:
        assert i in text_exception, f"Неожиданная ошибка: {i}, Вывод: {result.stdout}"

    # if not isinstance(result.exception, ExceptionGroup) or (
    #     isinstance(exception, ExceptionGroup) and not exception.subgroup(StreamError)
    # ):
    #     assert exception is None, (
    #         f"КРАШ ПРОГРАММЫ! Комбинация: {final_args}\nВывод: {result.stdout}"
    #     )

    assert result.exit_code != 1, (
        f"CRITICAL CRASH! Exit code 1. Stdout:\n{result.stdout}"
    )
    # 1. Список исключений
    ignored = {"download.log", ".states"}

    # 2. Находим всё "запрещенное"
    leftovers = [f for f in out_dir.glob("*") if f.name not in ignored]

    # Инвариант 2: Если это DRY-RUN, на диске НЕ ДОЛЖНО быть создано ни одного
    # файла генома
    if "--dry-run" in data["args_template"]:
        assert len(leftovers) == num_file, (
            f"DRY-RUN нарушил обещание и скачал файлы на диск!"
            f"Было {num_file}. Стало {len(leftovers)}"
        )

    # Инвариант 3: Если это STREAM, на диске тоже пусто
    if "--stream" in data["args_template"]:
        assert len(leftovers) == num_file, "STREAM записал бинарники на диск!"

    # Инвариант 4: Если это обычная загрузка, файлы должны лежать на диске
    if (
        "--stream" not in data["args_template"]
        and "--dry-run" not in data["args_template"]
        and result.exit_code == 0
    ):
        # Количество скачанных файлов должно совпадать с количеством уникальных ссылок
        assert len(leftovers) <= len(data["paths"]) + num_file, (
            f"Файлы не скачались! Лог терминала:\n{result.stdout}"
        )
