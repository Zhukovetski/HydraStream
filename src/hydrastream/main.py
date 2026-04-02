# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
from pathlib import Path
from typing import Annotated
from urllib.parse import urlparse

import typer

from hydrastream import __version__
from hydrastream.facade import HydraClient

app = typer.Typer(add_completion=False, no_args_is_help=True)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"HydraStream v{__version__}")
        raise typer.Exit()


def is_valid_url(url: str) -> bool:
    """Проверяет, является ли строка корректным URL."""
    try:
        result = urlparse(url)
        # Урл считается валидным, если есть протокол (http/https) и домен
        return all([result.scheme in ("http", "https"), result.netloc])
    except ValueError:
        return False


def parse_urls(links_from_args: list[str] | None, filepath: str | None) -> list[str]:
    all_links: list[str] = []

    # 1. Ссылки из аргументов
    if links_from_args:
        all_links.extend([url for url in links_from_args if is_valid_url(url)])

    # 2. Обработка файла или stdin
    if filepath:
        try:
            if filepath == "-":
                source = sys.stdin
                # Читаем до конца ввода (Ctrl+D)
                for line in source:
                    process_line(line, all_links)
            else:
                with Path(filepath).open(encoding="utf-8") as f:
                    for line in f:
                        process_line(line, all_links)
        except (FileNotFoundError, PermissionError) as e:
            print(f"Ошибка при чтении файла: {e}", file=sys.stderr)

    return list(set(all_links))


def process_line(line: str, links_list: list[str]) -> None:
    """Очищает строку и добавляет валидные ссылки в список."""
    clean_line = line.strip()
    if clean_line and not clean_line.startswith("#"):
        # split()[0] берет первое слово, если в строке есть лишний мусор
        url = clean_line.split()[0]
        if is_valid_url(url):
            links_list.append(url)


async def async_main(
    links: list[str],
    stream: bool,
    threads: int,
    no_ui: bool,
    quiet: bool,
    output_dir: str,
    hash: str | None,
    stream_buffer_size_mb: int | None,
    verify: bool,
) -> None:
    """
    Core asynchronous execution function for downloading or streaming files.

    Args:
        links (list[str]): List of target URLs.
        stream (bool): Whether to stream data to stdout instead of writing to disk.
        threads (int): Maximum number of concurrent download threads.
        no_ui (bool): Disable GUI (plain text logs only) if set to True.
        quiet (bool): Dead silence. No console output at all if set to True.
        output_dir (str): Destination directory for downloaded files.
        hash (str | None): Expected hash checksum (only evaluated if a single link is
        provided).
        chunk_timeout (float): Timeout in seconds for individual chunk requests.
        stream_buffer_size (int | None): Maximum buffer size for in-memory streaming.
    """
    expected_checksums: dict[str, str] = {}

    # Hash logic: only map the hash if a single URL is provided
    if hash and len(links) == 1:
        expected_checksums[links[0]] = hash
    elif hash and len(links) > 1:
        typer.secho(
            "Warning: The --hash flag is ignored when multiple URLs are provided.",
            fg="yellow",
            err=True,
        )

    async with HydraClient(
        threads=threads,
        no_ui=no_ui,
        quiet=quiet,
        out_dir=output_dir,
        stream_buffer_size_mb=stream_buffer_size_mb,
        verify=verify,
        client_kwargs=None,
    ) as loader:
        if stream:
            assert sys.__stdout__ is not None
            is_terminal = sys.__stdout__.isatty()

            if is_terminal:
                typer.secho(
                    "Warning: You are running in --stream mode but output "
                    "is not redirected!\n"
                    "The downloaded binary data will be discarded.",
                    fg="yellow",
                    err=True,
                )

                if not hash:
                    typer.secho(
                        "Please use a pipe (e.g., '| zcat') or redirect to a file "
                        "(e.g., '> file.gz').\n"
                        "Aborting to save bandwidth.",
                        fg="red",
                        err=True,
                    )
                    raise typer.Exit(code=1)

                typer.secho(
                    "Proceeding in 'Verification Only' mode since --hash is provided.",
                    fg="cyan",
                    err=True,
                )

            async for _, file_gen in loader.stream(links, expected_checksums):
                async for chunk in file_gen:
                    if not is_terminal:
                        sys.stdout.buffer.write(chunk)
                    else:
                        pass

                if not is_terminal:
                    sys.stdout.buffer.flush()
        else:
            await loader.run(links, expected_checksums)


@app.command()
def cli(
    links: Annotated[
        list[str], typer.Argument(help="List of target URLs to download.")
    ],
    input_file: Annotated[
        str | None,
        typer.Option("-i", "--input", help="Read URLs from file or '-' for stdin"),
    ] = None,
    hash: Annotated[
        str | None,
        typer.Option(
            "--hash", help="Expected hash checksum (applicable only for a single URL)."
        ),
    ] = None,
    output_dir: Annotated[
        str,
        typer.Option(
            "-o", "--output", help="Destination directory for downloaded files."
        ),
    ] = "download",
    threads: Annotated[
        int,
        typer.Option(
            "-t", "--threads", help="Number of concurrent download connections."
        ),
    ] = 3,
    stream: Annotated[
        bool,
        typer.Option(
            "-s",
            "--stream",
            help="Enable streaming mode (outputs to stdout without saving to disk).",
        ),
    ] = False,
    no_ui: Annotated[
        bool,
        typer.Option(
            "--no-ui", "-nu", help="Disable GUI (plain text logs only) if set to True"
        ),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Dead silence. No console output at all."),
    ] = False,
    json_logs: Annotated[
        bool,
        typer.Option(
            "--json", "-j", help="Output logs in JSON Lines format (for machines)."
        ),
    ] = False,
    stream_buffer_size_mb: Annotated[
        int | None,
        typer.Option("--buffer", "-b", help="Maximum stream buffer size in bytes."),
    ] = None,
    verify: Annotated[
        bool,
        typer.Option(
            "--verify/--no-verify",
            "-V/-N",
            help="Verify the downloaded file hash. Use --no-verify to skip check.",
        ),
    ] = True,
    version: Annotated[
        bool | None,
        typer.Option("--version", "-v", callback=version_callback, is_eager=True),
    ] = None,
) -> None:
    """
    HydraStream: Concurrent HTTP downloader with in-memory stream reordering
    (httpx + uvloop).
    """
    if not links:
        typer.secho("No URLs provided for download!", fg="red", bold=True, err=True)
        raise typer.Exit(code=1)

    try:
        if sys.platform != "win32":
            try:
                import uvloop  # noqa

                uvloop.install()
            except ImportError:
                pass

        asyncio.run(
            async_main(
                links=links,
                stream=stream,
                threads=threads,
                no_ui=no_ui,
                quiet=quiet,
                output_dir=output_dir,
                hash=hash,
                stream_buffer_size_mb=stream_buffer_size_mb,
                verify=verify,
            )
        )
    except KeyboardInterrupt:
        typer.secho("\nInterrupted by user (CLI).", fg="yellow", err=True)
        raise typer.Exit(code=130) from None

    except Exception as e:
        typer.secho(f"\nCritical error: {e}", fg="red", bold=True, err=True)
        raise  # typer.Exit(code=1) from None


if __name__ == "__main__":
    app()
