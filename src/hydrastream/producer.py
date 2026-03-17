import asyncio
from collections.abc import Iterable

from hydrastream.models import File, FileMeta, HydraContext
from hydrastream.monitor import add_file, done, log, update
from hydrastream.network import extract_filename, safe_request
from hydrastream.providers import resolve_hash
from hydrastream.storage import create_sparse_file, load_state, open_file


async def chunk_producer(
    ctx: HydraContext,
    links: Iterable[str],
    expected_checksums: dict[str, str] | None = None,
) -> None:
    """Main orchestration loop for task generation."""
    checksums_map = expected_checksums or {}

    for i, url in enumerate(links):
        if not ctx.is_running:
            break

        try:
            # 1. Fetch Metadata (HEAD)
            meta = await _fetch_metadata(ctx, url)
            if not meta:
                await ctx.file_discovery_queue.put(None)
                continue
            filename, total_size = meta

            # 2. Resolve MD5
            md5_val = await _resolve_md5(ctx, url, filename, checksums_map.get(url))

            # 3. Prepare File Object (State recovery, Disk allocation)
            file_obj = await _prepare_file_object(
                ctx, url, filename, total_size, md5_val
            )

            # 4. Open File Descriptor (Disk mode only)
            if not ctx.stream:
                file_obj.fd = open_file(ctx.fs, filename=file_obj.meta.filename)

            # 5. Register in UI & Dispatch to Workers
            await _register_and_dispatch(ctx, file_obj, i)

        except asyncio.CancelledError:
            break
        except OSError as e:
            await log(ctx.ui, f"OS/Disk Error: {e}", status="CRITICAL")
            ctx.is_running = False  # Fatal, halt everything
            break
        except Exception as e:
            await log(ctx.ui, f"Failed to process URL {url}: {e}", status="ERROR")
            await ctx.file_discovery_queue.put(None)


async def _fetch_metadata(ctx: HydraContext, url: str) -> tuple[str, int] | None:
    """Fetches headers, calculates total size, and extracts the optimal filename."""
    response = await safe_request(ctx.net, "HEAD", url=url)
    if response is None:
        await log(ctx.ui, f"Skipping {url} due to unreachable remote.", status="ERROR")
        return None

    total_size = int(response.headers.get("content-length", 0))
    filename = extract_filename(url, response.headers)

    if total_size <= 0:
        await log(
            ctx.ui,
            f"Invalid Content-Length ({total_size}) for {filename}",
            status="ERROR",
        )
        return None

    return filename, total_size


async def _resolve_md5(
    ctx: HydraContext, url: str, filename: str, predefined_md5: str | None
) -> str | None:
    """
    Resolves the MD5 checksum either from provided args or by fetching from providers.
    """
    if predefined_md5:
        return predefined_md5

    add_file(ctx.ui, filename)  # Start indeterminate spinner
    md5_val = await resolve_hash(ctx.net, url, filename)
    await done(ctx.ui, filename)  # Stop spinner

    if md5_val is None:
        await log(ctx.ui, f"Missing MD5 hash for file: {filename}", status="WARNING")

    return md5_val


async def _prepare_file_object(
    ctx: HydraContext, url: str, filename: str, total_size: int, md5_val: str | None
) -> File:
    """Handles state recovery, disk space validation, and File object creation."""
    # Calculate optimal chunk size
    parts = ctx.config.threads
    chunk_size = max(total_size // parts, ctx.MIN_CHUNK)
    if ctx.stream and chunk_size > ctx.STREAM_CHUNK_SIZE:
        chunk_size = ctx.STREAM_CHUNK_SIZE

    # Stream Mode: No disk usage, create fresh File
    if ctx.stream:
        return File(
            meta=FileMeta(
                filename=filename,
                url=url,
                content_length=total_size,
                expected_md5=md5_val,
            ),
            chunk_size=chunk_size,
        )

    # Disk Mode: Try State Recovery
    file_obj, num_states = load_state(ctx.fs, filename=filename)
    if num_states > 1:
        await log(
            ctx.ui, f"Multiple state files found for {filename}!", status="WARNING"
        )

    if file_obj:
        return file_obj

    # Disk Mode: Fresh Download (Check space & allocate)
    new_filename = create_sparse_file(ctx.fs, filename=filename, size=total_size)
    if new_filename:
        await log(
            ctx.ui,
            f"{filename} already exists. Saving as {new_filename}.",
            status="WARNING",
        )
        filename = new_filename
    print(filename)
    return File(
        meta=FileMeta(
            filename=filename, url=url, content_length=total_size, expected_md5=md5_val
        ),
        chunk_size=chunk_size,
    )


async def _register_and_dispatch(
    ctx: HydraContext, file_obj: File, priority_index: int
) -> None:
    """Registers the file in the global context/UI and dispatches its chunks."""
    filename = file_obj.meta.filename
    ctx.files[filename] = file_obj
    chunks = file_obj.chunks or []

    add_file(ctx.ui, filename, file_obj.meta.content_length)
    if not ctx.stream:
        downloaded = sum(c.uploaded for c in chunks)
        if downloaded - len(chunks) > 0:
            update(ctx.ui, filename, downloaded)
    else:
        await ctx.file_discovery_queue.put(filename)

    # 4. Dispatch tasks to queue
    for c in chunks:
        if not ctx.is_running:
            break
        if c.current_pos <= c.end:
            await ctx.chunk_queue.put((priority_index, c))
