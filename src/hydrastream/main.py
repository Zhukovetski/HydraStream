# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
import tomllib
from functools import partial
from pathlib import Path
from typing import Annotated, Any

import typer
from curl_cffi import BrowserTypeLiteral

from hydrastream.__init__ import __version__
from hydrastream.exceptions import (
    ExitCode,
    HydraError,
    InvalidParameterError,
    LogStatus,
    ValidationError,
)
from hydrastream.facade import HydraClient
from hydrastream.models import (
    Checksum,
    DisplayConfig,
    HydraConfig,
    LogState,
    SpeedLimiterState,
    TypeHash,
    UIState,
)
from hydrastream.monitor import log, log_start, log_stop, report

if sys.platform != "win32":
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass


def load_user_config() -> dict[str, Any]:
    config_path = Path.home() / ".config" / "hydrastream" / "config.toml"

    if not config_path.is_file():
        return {}

    try:
        with config_path.open("rb") as f:
            return tomllib.load(f)
    except Exception:
        return {}


USER_CONFIG: dict[str, Any] = load_user_config()

app = typer.Typer(
    no_args_is_help=True,
    rich_markup_mode="rich",
    epilog="[bold]Examples:[/]\n  hs https://example.com/file.gz\n  hs -i urls.txt "
    "-t 10 -o ./data\n  hs --stream https://example.com/file.gz | zcat | grep pattern",
)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"HydraStream v{__version__}")
        raise typer.Exit()


async def async_main(  # noqa: C901, PLR0912
    links: list[str] | None,
    input_file: str | None,
    stream: bool,
    typehash: TypeHash,
    hash: str | None,
    output_dir: str,
    dry_run: bool,
    no_ui: bool,
    quiet: bool,
    json_logs: bool,
    verify: bool,
    threads: int,
    min_chunk_size_mb: int,
    max_stream_chunk_size_mb: int,
    stream_buffer_size_mb: int | None,
    speed_limit: float | None,
    impersonate: BrowserTypeLiteral,
    debug: bool,
) -> None:
    """
    Core asynchronous orchestrator for downloading or streaming files.

    Args:
        links: List of target URLs provided via positional arguments.
        input_file: Path to a text file containing URLs, or '-' for stdin.
        stream: Whether to stream data to stdout instead of writing to disk.
        typehash: Hash algorithm type (e.g., md5, sha256).
        hash: Expected hash checksum (only evaluated if a single valid link is provided).
        output_dir: Destination directory for downloaded files.
        dry_run: Simulate the download process (metadata fetch only).
        no_ui: Disable GUI (plain text logs only) if set to True.
        quiet: Dead silence mode. No console output at all if set to True.
        json_logs: Output logs in structured JSON Lines format.
        verify: Enable or disable post-download hash verification.
        threads: Maximum number of concurrent download connections.
        min_chunk_size_mb: Minimum chunk size in MB for disk mode.
        max_stream_chunk_size_mb: Target chunk size in MB for stream mode.
        stream_buffer_size_mb: Maximum memory buffer size in MB for streaming.
        speed_limit: Global bandwidth throttle limit in MB/s.
        browser: Browser TLS fingerprint to impersonate.
        debug: Enable debug mode to propagate full tracebacks on failure.
    """  # noqa: E501
    ui = UIState(
        display=DisplayConfig(
            no_ui=no_ui,
            quiet=quiet,
            dry_run=dry_run,
            json_logs=json_logs,
            verify=verify,
        ),
        log=LogState(log_file=Path(output_dir).expanduser().resolve() / "download.log"),
        speed=SpeedLimiterState(speed_limit=speed_limit),
    )
    try:
        await log_start(ui)

        expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] = {}

        # Hash logic: only map the hash if a single URL is provided
        if hash and links and len(links) == 1:
            expected_checksums[links[0]] = Checksum(algorithm=typehash, value=hash)
        elif hash and links and len(links) > 1:
            raise ValidationError(
                param="hash",
                reason=(
                    "Warning: The --hash flag is ignored when "
                    "multiple URLs are provided."
                ),
            )

        config = HydraConfig(
            threads=threads,
            dry_run=dry_run,
            min_chunk_size_mb=min_chunk_size_mb,
            max_stream_chunk_size_mb=max_stream_chunk_size_mb,
            speed_limit=speed_limit,
            no_ui=no_ui,
            quiet=quiet,
            output_dir=output_dir,
            stream_buffer_size_mb=stream_buffer_size_mb,
            json_logs=json_logs,
            verify=verify,
            client_kwargs=None,
            impersonate=impersonate,
            debug=debug,
        )

        async with HydraClient(config=config, ui=ui) as loader:
            if stream and not config.dry_run:
                assert sys.__stdout__ is not None
                is_terminal = sys.__stdout__.isatty()

                if is_terminal:
                    await report(
                        ui,
                        InvalidParameterError(
                            param="stream",
                            reason=(
                                "Warning: You are running in --stream mode but output "
                                "is not redirected!\n"
                                "The downloaded binary data will be discarded."
                            ),
                        ),
                    )

                    if not expected_checksums:
                        await report(
                            ui,
                            ValidationError(
                                param="stream",
                                reason=(
                                    "Please use a pipe (e.g., '| zcat') or redirect to "
                                    "a file (e.g., '> file.gz').\n"
                                    "Aborting to save bandwidth."
                                ),
                            ),
                        )

                        await report(
                            ui,
                            InvalidParameterError(
                                param="stream",
                                reason=(
                                    "Proceeding in 'Verification Only' mode "
                                    "since --hash is provided."
                                ),
                            ),
                        )

                async for _, file_gen in await loader.stream(
                    links, input_file, expected_checksums
                ):
                    async for chunk in file_gen:
                        if not is_terminal:
                            sys.stdout.buffer.write(chunk)
                        else:
                            pass

                    if not is_terminal:
                        sys.stdout.buffer.flush()
            else:
                await loader.run(links, input_file, expected_checksums)

    except (KeyboardInterrupt, asyncio.CancelledError):
        if debug:
            raise
        sys.exit(ExitCode.INTERRUPTED)

    except (Exception, ExceptionGroup) as e:
        if debug:
            raise

        all_errors = flatten_exceptions(e)

        codes = []
        for err in all_errors:
            if isinstance(err, HydraError):
                await report(ui, err)
                await log(ui, f"FATAL: {err!r}", status=LogStatus.CRITICAL)
                codes.append(err.exit_code)

        exit_code = max(codes) if codes else ExitCode.GENERAL_ERROR

        sys.exit(exit_code)

    finally:
        await log_stop(ui)


def flatten_exceptions(e: BaseException) -> list[BaseException]:
    """Разворачивает BaseExceptionGroup в плоский список ошибок."""
    if isinstance(e, BaseExceptionGroup):
        result = []
        for child in e.exceptions:
            result.extend(flatten_exceptions(child))
        return result
    return [e]


def get_cfg(key: str, default: Any = None) -> Any:  # noqa: ANN401
    return USER_CONFIG.get(key, default)


@app.command()
def cli(
    links: Annotated[
        list[str] | None,
        typer.Argument(
            help="List of target URLs to download.",
            default_factory=partial(get_cfg, "links"),
        ),
    ],
    input_file: Annotated[
        str | None,
        typer.Option(
            "-i",
            "--input",
            help="Read URLs from file or '-' for stdin",
            default_factory=partial(get_cfg, "input"),
        ),
    ],
    typehash: Annotated[
        TypeHash,
        typer.Option(
            "--typehash",
            "-th",
            help="Hash algorithm type (e.g., md5, sha256).",
            default_factory=partial(get_cfg, "typehash", "md5"),
        ),
    ],
    hash: Annotated[
        str | None,
        typer.Option(
            "--hash",
            help="Expected hash checksum (applicable only for a single URL).",
            default_factory=partial(get_cfg, "hash"),
        ),
    ],
    output_dir: Annotated[
        str,
        typer.Option(
            "-o",
            "--output",
            help="Destination directory for downloaded files.",
            default_factory=partial(get_cfg, "output", "download"),
        ),
    ],
    threads: Annotated[
        int | None,
        typer.Option(
            "-t",
            "--threads",
            help="Number of concurrent download connections.",
            default_factory=partial(get_cfg, "threads"),
        ),
    ],
    stream: Annotated[
        bool,
        typer.Option(
            "-s",
            "--stream",
            help="Enable streaming mode (outputs to stdout without saving to disk).",
            default_factory=partial(get_cfg, "stream", False),
        ),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="""Simulate the process: fetch metadata, check disk space, and print a
             report without downloading data.""",
            default_factory=partial(get_cfg, "dry-run", False),
        ),
    ],
    min_chunk_size_mb: Annotated[
        int | None,
        typer.Option(
            "--min-chunk-mb",
            help="Minimum chunk size in Megabytes for standard disk downloads.",
            default_factory=partial(get_cfg, "min-chunk-mb", 1),
        ),
    ],
    max_stream_chunk_size_mb: Annotated[
        int | None,
        typer.Option(
            "--stream-chunk-mb",
            help="Target chunk size in Megabytes for streaming mode.",
            default_factory=partial(get_cfg, "stream-chunk-mb", 5),
        ),
    ],
    stream_buffer_size_mb: Annotated[
        int | None,
        typer.Option(
            "--buffer",
            "-b",
            help="Maximum stream buffer size in Megabytes to prevent OOM.",
            default_factory=partial(get_cfg, "buffer"),
        ),
    ],
    speed_limit: Annotated[
        float | None,
        typer.Option(
            "--limit",
            "-l",
            help="Global download speed limit in MB/s.",
            default_factory=partial(get_cfg, "limit"),
        ),
    ],
    no_ui: Annotated[
        bool,
        typer.Option(
            "--no-ui",
            "-nu",
            help="Disable GUI (plain text logs only) if set to True.",
            default_factory=partial(get_cfg, "no-ui", False),
        ),
    ],
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Dead silence. No console output at all.",
            default_factory=partial(get_cfg, "quiet", False),
        ),
    ],
    json_logs: Annotated[
        bool,
        typer.Option(
            "--json",
            "-j",
            help="Output logs in JSON Lines format (for machines).",
            default_factory=partial(get_cfg, "json", False),
        ),
    ],
    verify: Annotated[
        bool,
        typer.Option(
            "--verify/--no-verify",
            "-V/-N",
            help="Verify the downloaded file hash. Use --no-verify to skip check.",
            default_factory=partial(get_cfg, "verify", True),
        ),
    ],
    browser: Annotated[
        BrowserTypeLiteral,
        typer.Option(
            "-B",
            "--browser",
            help="Browser TLS fingerprint to impersonate (e.g., chrome120, safari153).",
            default_factory=partial(get_cfg, "browser", "chrome120"),
        ),
    ],
    version: Annotated[
        bool | None,
        typer.Option("--version", "-v", callback=version_callback, is_eager=True),
    ] = None,
    debug: Annotated[bool, typer.Option("--debug", "-d")] = False,
) -> None:
    """
    HydraStream: Concurrent HTTP downloader with in-memory stream reordering
    (curl_cffi + uvloop).
    """

    if threads is None:
        threads = 128
    if min_chunk_size_mb is None:
        min_chunk_size_mb = 5
    if max_stream_chunk_size_mb is None:
        max_stream_chunk_size_mb = 5

    asyncio.run(
        async_main(
            links=links,
            input_file=input_file,
            stream=stream,
            typehash=typehash,
            hash=hash,
            threads=threads,
            dry_run=dry_run,
            min_chunk_size_mb=min_chunk_size_mb,
            max_stream_chunk_size_mb=max_stream_chunk_size_mb,
            speed_limit=speed_limit,
            no_ui=no_ui,
            quiet=quiet,
            output_dir=output_dir,
            stream_buffer_size_mb=stream_buffer_size_mb,
            json_logs=json_logs,
            verify=verify,
            impersonate=browser,
            debug=debug,
        )
    )


if __name__ == "__main__":
    app()
