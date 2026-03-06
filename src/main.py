# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import sys
from typing import Annotated

import typer
import uvloop

from ncbiloader import NCBILoader

# Создаем приложение Typer
app = typer.Typer(add_completion=False)


async def async_main(
    links: list[str],
    stream: bool,
    threads: int,
    silent: bool,
    output_dir: str,
    md5: str | None,  # Принимаем один MD5 как строку
    timeout: float,
    follow_redirects: bool,
    stream_buffer_size: int | None,
    http2: bool,
    verify: bool,
) -> None:

    # Логика хешей для CLI:
    # Если ссылка одна и хеш передан -> создаем словарь.
    # Если ссылок много -> игнорируем (или можно кинуть ошибку).
    expected_checksums: dict[str, str] = {}
    if md5 and len(links) == 1:
        expected_checksums[links[0]] = md5
    elif md5 and len(links) > 1:
        typer.secho("Предупреждение: Флаг --md5 игнорируется для нескольких ссылок.", fg="yellow", err=True)

    async with NCBILoader(
        threads=threads,
        silent=silent,
        output_dir=output_dir,
        timeout=timeout,
        follow_redirects=follow_redirects,
        stream_buffer_size=stream_buffer_size,
        http2=http2,
        verify=verify,
    ) as loader:
        if stream:
            # Проверяем, куда мы пишем.
            # sys.stdout.isatty() == True, если это терминал (человек смотрит).
            # sys.stdout.isatty() == False, если это труба (|) или файл (>).
            assert sys.__stdout__ is not None
            stream_to_stdout = sys.__stdout__.isatty()

            async for _, file_gen in loader.stream_all(links, expected_checksums):
                async for chunk in file_gen:
                    if stream_to_stdout:
                        # Пишем сырые байты в stdout
                        pass
                    else:
                        # Если юзер запустил --stream просто в консоли,
                        # не надо гадить бинарниками в экран. Просто потребляем.
                        sys.stdout.buffer.write(chunk)

                # Важно: flush stdout после каждого файла
                if stream_to_stdout:
                    sys.stdout.buffer.flush()
        else:
            await loader.run(links, expected_checksums)


H = {
    "L": "Список URL для скачивания",
    "M": "Ожидаемый MD5 (только для одной ссылки)",
    "O": "Папка для сохранения",
    "T": "Количество потоков",
    "S": "Режим потоковой обработки (без сохранения)",
    "SL": "Отключить GUI",
    "TM": "Таймаут соединения",
    "B": "Размер буфера стрима (байт)",
    "H2": "Использовать HTTP/2",
    "R": "Следовать редиректам",
    "V": "Проверять размер файла после скачивания",
}


@app.command()
def loader(
    links: Annotated[list[str], typer.Argument(help=H["L"])],
    # Options
    md5: Annotated[str | None, typer.Option("--md5", help=H["M"])] = None,
    output_dir: Annotated[str, typer.Option("-o", "--output", help=H["O"])] = "download",
    threads: Annotated[int, typer.Option("-t", "--threads", help=H["T"])] = 3,
    stream: Annotated[bool, typer.Option("-s", "--stream", help=H["S"])] = False,
    silent: Annotated[bool, typer.Option("--silent", help=H["SL"])] = False,
    # Технические настройки
    timeout: Annotated[float, typer.Option(help=H["TM"])] = 30.0,
    stream_buffer_size: Annotated[int | None, typer.Option("--buffer", help=H["B"])] = None,
    http2: Annotated[bool, typer.Option("--http2/--no-http2", help=H["H2"])] = True,
    follow_redirects: Annotated[bool, typer.Option("--redirects/--no-redirects", help=H["R"])] = True,
    verify: Annotated[bool, typer.Option("--verify/--no-verify", help=H["V"])] = True,
) -> None:
    """
    NCBI Async Downloader: Быстрый загрузчик геномных данных.
    """
    if not links:
        typer.secho("Нет ссылок для скачивания!", fg="red", bold=True, err=True)
        raise typer.Exit(code=1)

    try:
        uvloop.run(
            async_main(
                links=links,
                stream=stream,
                threads=threads,
                silent=silent,
                output_dir=output_dir,
                md5=md5,
                timeout=timeout,
                follow_redirects=follow_redirects,
                stream_buffer_size=stream_buffer_size,
                http2=http2,
                verify=verify,
            )
        )
    except KeyboardInterrupt:
        typer.secho("\n⛔ Прервано пользователем.", fg="yellow", err=True)
    except Exception as e:
        typer.secho(f"\n💥 Критическая ошибка: {e}", fg="red", bold=True, err=True)
        raise  # Раскомментируй для отладки


if __name__ == "__main__":
    app()
