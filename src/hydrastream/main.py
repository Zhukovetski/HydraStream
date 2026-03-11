# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
from typing import Annotated

import typer

from hydrastream import HydraStream

# Initialize Typer app with disabled completion and auto-help on empty run
app = typer.Typer(add_completion=False, no_args_is_help=True)


async def async_main(
    links: list[str],
    stream: bool,
    threads: int,
    no_ui: bool,
    quiet: bool,
    output_dir: str,
    md5: str | None,
    chunk_timeout: int,
    stream_buffer_size: int | None,
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
        md5 (str | None): Expected MD5 checksum (only evaluated if a single link is provided).
        chunk_timeout (float): Timeout in seconds for individual chunk requests.
        stream_buffer_size (int | None): Maximum buffer size for in-memory streaming.
    """
    expected_checksums: dict[str, str] = {}

    # MD5 logic: only map the hash if a single URL is provided
    if md5 and len(links) == 1:
        expected_checksums[links[0]] = md5
    elif md5 and len(links) > 1:
        typer.secho(
            "Warning: The --md5 flag is ignored when multiple URLs are provided.", fg="yellow", err=True
        )

    async with HydraStream(
        threads=threads,
        no_ui=no_ui,
        quiet=quiet,
        output_dir=output_dir,
        stream_buffer_size=stream_buffer_size,
        chunk_timeout=chunk_timeout,
        client_kwargs=None,  # Passed internally to NetworkClient
    ) as loader:
        if stream:
            # sys.stdout.isatty() is True if running in an interactive terminal.
            # It is False if output is piped (e.g., `| zcat` or `> file.txt`).
            assert sys.__stdout__ is not None
            is_terminal = sys.__stdout__.isatty()

            if is_terminal:
                typer.secho(
                    "⚠️ Warning: You are running in --stream mode but output is not redirected!\n"
                    "The downloaded binary data will be discarded.",
                    fg="yellow",
                    err=True,
                )

                if not md5:
                    typer.secho(
                        "Please use a pipe (e.g., '| zcat') or redirect to a file (e.g., '> file.gz').\n"
                        "Aborting to save bandwidth.",
                        fg="red",
                        err=True,
                    )
                    raise typer.Exit(code=1)

                typer.secho(
                    "Proceeding in 'Verification Only' mode since --md5 is provided.", fg="cyan", err=True
                )

            async for _, file_gen in loader.stream_all(links, expected_checksums):
                async for chunk in file_gen:
                    if not is_terminal:
                        # Safely write raw bytes to the pipe/file
                        sys.stdout.buffer.write(chunk)
                    else:
                        # Terminal mode: consume bytes silently for MD5 validation
                        # to avoid flooding the screen with binary garbage.
                        pass

                if not is_terminal:
                    # Ensure all data is pushed to the next process in the pipeline
                    sys.stdout.buffer.flush()
        else:
            await loader.run(links, expected_checksums)


@app.command()
def cli(
    links: Annotated[list[str], typer.Argument(help="List of target URLs to download.")],
    md5: Annotated[
        str | None, typer.Option("--md5", help="Expected MD5 checksum (applicable only for a single URL).")
    ] = None,
    output_dir: Annotated[
        str, typer.Option("-o", "--output", help="Destination directory for downloaded files.")
    ] = "download",
    threads: Annotated[
        int, typer.Option("-t", "--threads", help="Number of concurrent download connections.")
    ] = 3,
    stream: Annotated[
        bool,
        typer.Option(
            "-s", "--stream", help="Enable streaming mode (outputs to stdout without saving to disk)."
        ),
    ] = False,
    no_ui: Annotated[
        bool, typer.Option("--no-ui", "-nu", help="Disable GUI (plain text logs only) if set to True")
    ] = False,
    quiet: Annotated[
        bool, typer.Option("--quiet", "-q", help="Dead silence. No console output at all.")
    ] = False,
    chunk_timeout: Annotated[
        int, typer.Option(help="Connection timeout in seconds for chunk downloads.")
    ] = 30,
    stream_buffer_size: Annotated[
        int | None, typer.Option("--buffer", "-b", help="Maximum stream buffer size in bytes.")
    ] = None,
) -> None:
    """
    HydraStream: A high-performance, asynchronous genomics data downloader.

    Optimized for fetching massive datasets from NCBI/EBI with in-memory streaming
    capabilities, concurrent connections, and robust error recovery.
    """
    if not links:
        typer.secho("No URLs provided for download!", fg="red", bold=True, err=True)
        raise typer.Exit(code=1)

    try:
        # uvloop replaces the standard asyncio event loop for maximum performance
        try:
            import uvloop

            async_run = uvloop.run
        except ImportError:
            async_run = asyncio.run

        async_run(
            async_main(
                links=links,
                stream=stream,
                threads=threads,
                no_ui=no_ui,
                quiet=quiet,
                output_dir=output_dir,
                md5=md5,
                chunk_timeout=chunk_timeout,
                stream_buffer_size=stream_buffer_size,
            )
        )
    except KeyboardInterrupt:
        typer.secho("\n⛔ Interrupted by user (CLI).", fg="yellow", err=True)
        # 130 is the standard Unix exit code for script termination via Ctrl+C
        raise typer.Exit(code=130) from None
    except Exception as e:
        typer.secho(f"\n💥 Critical error: {e}", fg="red", bold=True, err=True)
        # 1 indicates a general error (useful for automated pipelines)
        raise typer.Exit(code=1) from None


if __name__ == "__main__":
    app()
