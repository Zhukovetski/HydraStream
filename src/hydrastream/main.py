# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
from collections.abc import Generator
from contextlib import contextmanager
from io import TextIOWrapper
from pathlib import Path
from typing import Annotated, Any, TextIO
from urllib.parse import urlparse

import typer

from hydrastream import __version__
from hydrastream.facade import HydraClient
from hydrastream.models import HydraConfig, TypeHash

app = typer.Typer(add_completion=False, no_args_is_help=True)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"HydraStream v{__version__}")
        raise typer.Exit()


def is_valid_url(url: str) -> bool:
    """Checks if a given string is a structurally valid HTTP/HTTPS URL."""
    try:
        result = urlparse(url)
        return result.scheme in ("http", "https") and bool(result.netloc)
    except ValueError:
        return False


@contextmanager
def get_input_stream(
    filepath: str,
) -> Generator[TextIO | Any | TextIOWrapper, Any, None]:
    """Context manager for reading from a file or standard input (pipe)."""
    if filepath == "-":
        yield sys.stdin
    else:
        path = Path(filepath).expanduser().resolve()

        if not path.exists():
            typer.secho(f"Error: Path does not exist: {path}", fg="red", err=True)
            raise typer.Exit(code=2)
        if not path.is_file():
            typer.secho(
                f"Error: Target is a directory, not a file: {path}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2)

        try:
            with path.open(encoding="utf-8") as f:
                yield f
        except PermissionError:
            typer.secho(f"Error: Permission denied: {path}", fg="red", err=True)
            raise typer.Exit(code=2) from None


def parse_urls(links_from_args: list[str] | None, filepath: str | None) -> list[str]:
    """
    Extracts, validates, and deduplicates URLs from both CLI arguments and
    an input file/stdin.
    Preserves the original order of URLs.
    """
    all_links = []

    # 1. Ссылки из прямых аргументов (hs url1 url2)
    if links_from_args:
        all_links.extend(url for url in links_from_args if is_valid_url(url))

    # 2. Обработка файла или пайпа (hs -i urls.txt или hs -i -)
    if filepath:
        try:
            with get_input_stream(filepath) as stream:
                for line in stream:
                    clean_line = line.strip()
                    if clean_line and not clean_line.startswith("#"):
                        # Берем первое слово, отсекая комментарии или пробелы в конце
                        url = clean_line.split()[0]
                        if is_valid_url(url):
                            all_links.append(url)
        except (FileNotFoundError, PermissionError, OSError) as e:
            typer.secho(f"Read error: {e}", fg="red", err=True)
            raise typer.Exit(code=2) from None

    # Используем dict.fromkeys для удаления дубликатов с сохранением порядка
    return list(dict.fromkeys(all_links))


async def async_main(
    config: HydraConfig,
    stream: bool,
    valid_links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str]],
) -> None:
    """
    Core asynchronous orchestrator for downloading or streaming files.

    Args:
        links: List of target URLs provided via positional arguments.
        input_file: Path to a text file containing URLs, or '-' for stdin.
        typehash: Hash algorithm type (e.g., md5, sha256).
        hash: Expected hash checksum (only evaluated if a single valid link is provided).
        stream: Whether to stream data to stdout instead of writing to disk.
        threads: Maximum number of concurrent download connections.
        no_ui: Disable GUI (plain text logs only) if set to True.
        quiet: Dead silence mode. No console output at all if set to True.
        output_dir: Destination directory for downloaded files.
        dry_run: Simulate the download process (metadata fetch only).
        min_chunk_size_mb: Minimum chunk size in MB for disk mode.
        min_stream_chunk_size_mb: Target chunk size in MB for stream mode.
        stream_buffer_size_mb: Maximum memory buffer size in MB for streaming.
        speed_limit: Global bandwidth throttle limit in MB/s.
        json_logs: Output logs in structured JSON Lines format.
        verify: Enable or disable post-download hash verification.
    """  # noqa: E501

    async with HydraClient(config=config) as loader:
        if stream:
            assert sys.__stdout__ is not None
            is_terminal = sys.__stdout__.isatty()

            if is_terminal and not config.dry_run:
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

            async for _, file_gen in loader.stream(valid_links, expected_checksums):
                async for chunk in file_gen:
                    if not is_terminal:
                        sys.stdout.buffer.write(chunk)
                    else:
                        pass

                if not is_terminal:
                    sys.stdout.buffer.flush()
        else:
            await loader.run(valid_links, expected_checksums)


@app.command()
def cli(
    links: Annotated[
        list[str] | None, typer.Argument(help="List of target URLs to download.")
    ] = None,
    input_file: Annotated[
        str | None,
        typer.Option("-i", "--input", help="Read URLs from file or '-' for stdin"),
    ] = None,
    typehash: Annotated[
        TypeHash,
        typer.Option(
            "--typehash", "-th", help="Hash algorithm type (e.g., md5, sha256)."
        ),
    ] = "md5",
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
    ] = 1,
    stream: Annotated[
        bool,
        typer.Option(
            "-s",
            "--stream",
            help="Enable streaming mode (outputs to stdout without saving to disk).",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="""Simulate the process: fetch metadata, check disk space, and print a
             report without downloading data.""",
        ),
    ] = False,
    min_chunk_size_mb: Annotated[
        int,
        typer.Option(
            "--min-chunk-mb",
            help="Minimum chunk size in Megabytes for standard disk downloads.",
        ),
    ] = 1,
    min_stream_chunk_size_mb: Annotated[
        int,
        typer.Option(
            "--stream-chunk-mb",
            help="Target chunk size in Megabytes for streaming mode.",
        ),
    ] = 5,
    stream_buffer_size_mb: Annotated[
        int | None,
        typer.Option(
            "--buffer",
            "-b",
            help="Maximum stream buffer size in Megabytes to prevent OOM.",
        ),
    ] = None,
    speed_limit: Annotated[
        float | None,
        typer.Option("--limit", "-l", help="Global download speed limit in MB/s."),
    ] = None,
    no_ui: Annotated[
        bool,
        typer.Option(
            "--no-ui", "-nu", help="Disable GUI (plain text logs only) if set to True."
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
    (curl_cffi + uvloop).
    """
    if not links and not input_file:
        typer.secho(
            "You must provide either[LINKS] or an --input file.",
            fg="red",
            bold=True,
            err=True,
        )
        raise typer.Exit(code=1)

    try:
        if sys.platform != "win32":
            try:
                import uvloop  # noqa

                uvloop.install()
            except ImportError:
                pass

        # ВСЕГДА парсим ссылки, чтобы отфильтровать мусор и дубликаты из прямых
        # аргументов!
        valid_links = parse_urls(links, input_file)

        if not valid_links:
            typer.secho(
                "No valid URLs found to process!", fg="red", bold=True, err=True
            )
            raise typer.Exit(code=1)

        expected_checksums: dict[str, tuple[TypeHash, str]] = {}

        # Hash logic: only map the hash if a single URL is provided
        if hash and len(valid_links) == 1:
            expected_checksums[valid_links[0]] = (typehash, hash)
        elif hash and len(valid_links) > 1:
            typer.secho(
                "Warning: The --hash flag is ignored when multiple URLs are provided.",
                fg="yellow",
                err=True,
            )
            raise typer.Exit(code=1)

        config = HydraConfig(
            threads=threads,
            dry_run=dry_run,
            min_chunk_size_mb=min_chunk_size_mb,
            min_stream_chunk_size_mb=min_stream_chunk_size_mb,
            speed_limit=speed_limit,
            no_ui=no_ui,
            quiet=quiet,
            output_dir=output_dir,
            stream_buffer_size_mb=stream_buffer_size_mb,
            json_logs=json_logs,
            verify=verify,
            client_kwargs=None,
        )
        asyncio.run(
            async_main(
                config=config,
                stream=stream,
                valid_links=valid_links,
                expected_checksums=expected_checksums,
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
