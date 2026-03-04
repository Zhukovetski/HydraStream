import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Literal

import httpx
import pytest
import respx

from ncbiloader.loader import NCBILoader


@pytest.mark.asyncio
@respx.mock
async def test_add_task_producer(tmp_path: Path) -> None:
    """Проверяем, что HEAD запрос работает и файл ставится в очередь"""

    url = "https://fake-ncbi.com/genome.gz"

    # 1. Настраиваем наш ФЕЙКОВЫЙ сервер
    # Говорим: если кто-то сделает HEAD запрос на этот URL, ответь 200 OK
    # и скажи, что размер файла 500 байт.
    respx.head(url).mock(return_value=httpx.Response(200, headers={"Content-Length": "500"}))

    # 2. Инициализируем лоадер (с временной папкой)
    loader = NCBILoader(output_dir=str(tmp_path), silent=True)

    await loader._add_task_producer([url], expected_checksums=None)

    # 4. Проверяем результаты
    # Файл должен был создаться в словаре
    assert "genome.gz" in loader.files

    file_obj = loader.files["genome.gz"]
    assert file_obj.content_length == 500

    # Проверяем, что на диске создалась пустая "болванка" файла
    file_path = loader.storage.out_dir / "genome.gz"
    assert file_path.exists()
    assert file_path.stat().st_size == 500

    # Проверяем, что чанки упали в очередь
    assert not loader._queue.empty()


@pytest.mark.asyncio
@respx.mock
async def test_loader_handles_404_gracefully(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/missing_genome.gz"

    # 1. Настраиваем respx: на любой HEAD запрос к этому URL возвращаем 404
    respx.head(url).mock(return_value=httpx.Response(404))

    loader = NCBILoader(output_dir=str(tmp_path), silent=True)

    # 2. Запускаем. Если тут вылетит исключение - тест провален (красный)
    # Если пройдет тихо - тест успешен (зеленый)
    await loader.run(url)

    # 3. Проверяем, что файл НЕ создался на диске и не добавлен в словарь
    filename = "missing_genome.gz"
    assert filename not in loader.files
    assert not (tmp_path / filename).exists()


@pytest.mark.asyncio
@respx.mock
async def test_graceful_shutdown_prevents_hang_run(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/huge_file.gz"

    # 1. Настраиваем фейковый сервер.
    # HEAD отвечает, что файл весит 100 ГБ.
    respx.head(url).mock(return_value=httpx.Response(200, headers={"Content-Length": "100000000000"}))

    # GET запрос будет бесконечно висеть (имитация долгого скачивания)
    # Для этого просто отдаем очень медленный поток байт
    async def slow_stream() -> AsyncGenerator[Literal[b"12345"]]:
        while True:
            yield b"12345"
            await asyncio.sleep(1)  # Сервер отдает по чайной ложке

    respx.get(url).mock(return_value=httpx.Response(206, content=slow_stream()))

    loader = NCBILoader(output_dir=str(tmp_path), silent=True, threads=2)

    # 2. Запускаем лоадер как ФОНОВУЮ ЗАДАЧУ
    run_task = asyncio.create_task(loader.run(url))

    # Даем программе 0.5 секунды, чтобы запустить воркеров, создать очереди и начать "качать"
    await asyncio.sleep(0.5)

    # 3. ИМИТАЦИЯ НАЖАТИЯ Ctrl+C (Посылаем сигнал остановки)
    await loader.stop()

    # 4. МОМЕНТ ИСТИНЫ
    # Ждем завершения run_task максимум 2 секунды.
    # Если твоя логика stop() (очистка очереди и отмена воркеров) работает правильно,
    # run_task завершится почти мгновенно.
    try:
        await asyncio.wait_for(run_task, timeout=2.0)
    except TimeoutError:
        pytest.fail("КРИТИЧЕСКАЯ ОШИБКА: Программа зависла при остановке! Дедлок в очередях!")

    # Проверяем, что флаг опущен
    assert loader.is_running is False


@pytest.mark.asyncio
@respx.mock
async def test_graceful_shutdown_prevents_hang_stream(tmp_path: Path) -> None:
    url = "https://fake-ncbi.com/huge_stream_file.gz"

    # 1. Настраиваем фейковый сервер
    respx.head(url).mock(return_value=httpx.Response(200, headers={"Content-Length": "100000000000"}))

    async def slow_stream() -> AsyncGenerator[Literal[b"12345"]]:
        while True:
            yield b"12345"
            await asyncio.sleep(1)  # Медленная отдача

    respx.get(url).mock(return_value=httpx.Response(206, content=slow_stream()))

    loader = NCBILoader(output_dir=str(tmp_path), silent=True, threads=2)

    # 2. СОЗДАЕМ ОБЕРТКУ-ПОТРЕБИТЕЛЬ
    # Эта корутина "распакует" генератор и заставит его работать
    async def consume_stream() -> None:
        # Передаем список[url], так как stream_all ждет Iterable
        async for _, file_gen in loader.stream_all([url]):
            async for _ in file_gen:
                pass  # Просто "съедаем" байты, имитируя работу

    # 3. Запускаем нашего потребителя как фоновую задачу
    run_task = asyncio.create_task(consume_stream())

    # Даем стриму немного времени на запуск воркеров и начало скачивания
    await asyncio.sleep(0.5)

    # 4. Бьем по тормозам
    await loader.stop()

    # 5. Ждем завершения с тайм-аутом
    try:
        await asyncio.wait_for(run_task, timeout=2.0)
    except TimeoutError:
        pytest.fail("КРИТИЧЕСКАЯ ОШИБКА: stream_all завис при остановке! Дедлок в очередях!")

    assert loader.is_running is False
