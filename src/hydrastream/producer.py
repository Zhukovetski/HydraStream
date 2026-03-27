# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from collections.abc import Iterable

from curl_cffi import Headers

from hydrastream.constants import MIN_CHUNK, STREAM_CHUNK_SIZE
from hydrastream.models import Checksum, File, FileMeta, HydraContext, TypeHash
from hydrastream.monitor import add_file, done, log, update
from hydrastream.network import extract_filename, safe_request, stream_chunk
from hydrastream.providers import ProviderRouter
from hydrastream.storage import create_sparse_file, load_state, open_file


async def chunk_producer(
    ctx: HydraContext,
    links: Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str]] | None = None,
) -> None:
    checksums_map = expected_checksums or {}

    for i, url in enumerate(links):
        if not ctx.is_running:
            break

        try:
            meta = await _fetch_metadata(ctx, url)
            if not meta:
                await ctx.file_discovery_queue.put(None)
                continue

            filename, total_size, supports_ranges = meta
            checksum = None

            if ctx.config.verify:
                checksum = await _resolve_md5(
                    ctx, url, filename, checksums_map.get(url)
                )
            file_obj = await _prepare_file_object(
                ctx,
                url=url,
                filename=filename,
                total_size=total_size,
                supports_ranges=supports_ranges,
                checksum=checksum,
            )
            if not ctx.stream:
                file_obj.fd = open_file(ctx.fs, filename=file_obj.meta.filename)

            await _register_and_dispatch(ctx, file_obj, i)

        except asyncio.CancelledError:
            break
        except OSError as e:
            await log(ctx.ui, f"OS/Disk Error: {e}", status="CRITICAL")
            ctx.is_running = False
            break
        except Exception as e:
            await log(ctx.ui, f"Failed to process URL {url}: {e}", status="ERROR")
            await ctx.file_discovery_queue.put(None)


async def _fetch_metadata(ctx: HydraContext, url: str) -> tuple[str, int, bool] | None:
    # 1. Пробуем HEAD
    response = await safe_request(ctx.net, "HEAD", url=url)
    # 2. Если HEAD не дал инфы, используем GET, но ОБЯЗАТЕЛЬНО через stream
    if response is None or int(response.headers.get("content-length", 0)) == 0:
        try:  # Используем stream, чтобы не грузить файл в память!
            # Контекстный менеджер 'async with' сам закроет соединение в конце
            async with stream_chunk(ctx.net, url, ctx.config.chunk_timeout) as resp:
                headers = resp.headers
                return parse_headers(url, headers)
        except Exception as e:
            await log(ctx.ui, f"Failed GET metadata for {url}: {e}", status="ERROR")
            return None

    return parse_headers(url, response.headers)


def parse_headers(url: str, headers: Headers) -> tuple[str, int, bool]:
    total_size = int(headers.get("content-length", 0))
    accept_ranges = headers.get("accept-ranges", "").lower()
    supports_ranges = (accept_ranges == "bytes") and (total_size > 0)
    filename = extract_filename(url, headers)
    return filename, total_size, supports_ranges


async def _resolve_md5(
    ctx: HydraContext,
    url: str,
    filename: str,
    checksum_tuple: tuple[TypeHash, str] | None,
) -> Checksum | None:
    if checksum_tuple:
        return Checksum(algorithm=checksum_tuple[0], value=checksum_tuple[1])

    add_file(ctx.ui, filename)

    provider = ProviderRouter()
    checksum = await provider.resolve_hash(ctx.net, url, filename)
    await done(ctx.ui, filename)

    if checksum is None:
        await log(ctx.ui, f"Missing MD5 hash for file: {filename}", status="WARNING")

    return checksum


async def _prepare_file_object(
    ctx: HydraContext,
    url: str,
    filename: str,
    total_size: int,
    supports_ranges: bool,
    checksum: Checksum | None,
) -> File:
    parts = ctx.config.threads
    chunk_size = max(total_size // parts, MIN_CHUNK)
    if ctx.stream and chunk_size > STREAM_CHUNK_SIZE:
        chunk_size = STREAM_CHUNK_SIZE

    if ctx.stream:
        return File(
            meta=FileMeta(
                filename=filename,
                url=url,
                content_length=total_size,
                supports_ranges=supports_ranges,
                expected_checksum=checksum,
            ),
            chunk_size=chunk_size,
        )
    file_obj = None
    if supports_ranges:
        file_obj, num_states = load_state(ctx.fs, filename=filename)
        if num_states > 1:
            await log(
                ctx.ui, f"Multiple state files found for {filename}!", status="WARNING"
            )

    if file_obj:
        return file_obj

    new_filename = create_sparse_file(ctx.fs, filename=filename, size=total_size)
    if new_filename:
        await log(
            ctx.ui,
            f"{filename} already exists. Saving as {new_filename}.",
            status="WARNING",
        )
        filename = new_filename
    return File(
        meta=FileMeta(
            filename=filename,
            url=url,
            content_length=total_size,
            supports_ranges=supports_ranges,
            expected_checksum=checksum,
        ),
        chunk_size=chunk_size,
    )


async def _register_and_dispatch(
    ctx: HydraContext, file_obj: File, priority_index: int
) -> None:
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

    for c in chunks:
        if not ctx.is_running:
            break
        if c.current_pos <= c.end:
            await ctx.chunk_queue.put((priority_index, c))
