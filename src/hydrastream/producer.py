# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import random

from curl_cffi import Headers, Response
from curl_cffi.requests import RequestsError

from .models import Checksum, File, FileMeta, HydraContext, TypeHash
from .monitor import add_file, done, log, update
from .network import extract_filename, safe_request, stream_chunk
from .providers import ProviderRouter


async def chunk_producer(
    ctx: HydraContext,
) -> None:

    while not ctx.links_queue.empty():
        id, url, checksum = await ctx.links_queue.get()

        if not ctx.is_running:
            break

        try:
            meta = await _fetch_metadata(ctx, url)

            filename, total_size, supports_ranges = meta
            checksum = None

            if ctx.config.verify:
                checksum = await _resolve_hash(ctx, url, filename, checksum)
            file_obj = await _prepare_file_object(
                ctx,
                id=id,
                url=url,
                filename=filename,
                total_size=total_size,
                supports_ranges=supports_ranges,
                checksum=checksum,
            )
            if not ctx.stream and not ctx.config.dry_run:
                file_obj.fd = ctx.fs.open_file(filename=file_obj.meta.filename)

            await _register_file(ctx, file_obj, id)

        except asyncio.CancelledError:
            break

        except RequestsError as e:
            response = e.response  # type: ignore

            if isinstance(response, Response):
                status = response.status_code

                if url and status in {400, 401, 403, 404, 410, 416}:
                    await log(
                        ctx.ui,
                        f"Link {url} failed permanently (HTTP {status}).",
                        status="ERROR",
                    )
                    continue

                # Остальные ошибки сервера (5xx, 429) — пробуем перекинуть ссылку
                # в очередь
                await requeue_chunk(ctx, url, checksum, delay_range=(0.5, 2.0))
            # 2. Если ответа нет (Сетевая ошибка / CurlError / Timeout)
            else:
                await requeue_chunk(ctx, url, checksum)

        except TimeoutError:
            await requeue_chunk(ctx, url, checksum)
        except OSError as e:
            await log(ctx.ui, f"OS/Disk Error: {e}", status="CRITICAL")
            ctx.is_running = False
            break
        except Exception as e:
            await log(
                ctx.ui, f"Critical Task creator Exception: {e!r}", status="CRITICAL"
            )
            raise
        finally:
            ctx.links_queue.task_done()


async def requeue_chunk(
    ctx: HydraContext,
    url: str,
    checksum: Checksum | None,
    delay_range: tuple[float, float] = (1.0, 3.0),
) -> None:

    await ctx.links_queue.put((-1, url, checksum))
    delay = random.uniform(*delay_range)
    await asyncio.sleep(delay)


async def _fetch_metadata(ctx: HydraContext, url: str) -> tuple[str, int, bool]:
    # 1. Пробуем HEAD
    response = await safe_request(ctx.net, "HEAD", url=url)
    # 2. Если HEAD не дал инфы, используем GET, но ОБЯЗАТЕЛЬНО через stream
    if response is None or int(response.headers.get("content-length", 0)) == 0:
        # Контекстный менеджер 'async with' сам закроет соединение в конце
        async with stream_chunk(ctx.net, url) as resp:
            headers = resp.headers
            return parse_headers(url, headers)

    return parse_headers(url, response.headers)


def parse_headers(url: str, headers: Headers) -> tuple[str, int, bool]:
    total_size = int(headers.get("content-length", 0))
    accept_ranges = headers.get("accept-ranges", "").lower()
    supports_ranges = (accept_ranges == "bytes") and (total_size > 0)
    filename = extract_filename(url, headers)
    return filename, total_size, supports_ranges


async def _resolve_hash(
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


async def _prepare_file_object(  # noqa
    ctx: HydraContext,
    id: int,
    url: str,
    filename: str,
    total_size: int,
    supports_ranges: bool,
    checksum: Checksum | None,
) -> File:
    parts = ctx.config.threads
    chunk_size = max(total_size // parts, ctx.config.MIN_CHUNK)
    if ctx.stream and chunk_size > ctx.config.STREAM_CHUNK_SIZE:
        chunk_size = ctx.config.STREAM_CHUNK_SIZE

    if ctx.stream:
        return File(
            meta=FileMeta(
                id=id,
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
        file_obj, num_states = ctx.fs.load_state(filename=filename)
        if num_states > 1:
            await log(
                ctx.ui, f"Multiple state files found for {filename}!", status="WARNING"
            )

    if file_obj:
        return file_obj
    if not ctx.config.dry_run:
        new_filename = ctx.fs.allocate_space(filename=filename, size=total_size)
        if new_filename:
            await log(
                ctx.ui,
                f"{filename} already exists. Saving as {new_filename}.",
                status="WARNING",
            )
            filename = new_filename
    return File(
        meta=FileMeta(
            id=id,
            filename=filename,
            url=url,
            content_length=total_size,
            supports_ranges=supports_ranges,
            expected_checksum=checksum,
        ),
        chunk_size=chunk_size,
    )


async def _register_file(
    ctx: HydraContext, file_obj: File, priority_index: int
) -> None:
    filename = file_obj.meta.filename
    ctx.files[priority_index] = file_obj
    chunks = file_obj.chunks or []

    add_file(ctx.ui, filename, file_obj.meta.content_length)
    if not ctx.stream:
        downloaded = sum(c.uploaded for c in chunks)
        if downloaded - len(chunks) > 0:
            update(ctx.ui, filename, downloaded)
    await ctx.dispatch_file_queue.put((priority_index, file_obj))


async def dispatch_chunks(ctx: HydraContext) -> None:
    if ctx.task_creators:
        while not ctx.dispatch_file_queue.empty() or any(
            not t.done() for t in ctx.task_creators
        ):
            priority_index, file = await ctx.dispatch_file_queue.get()
            if ctx.stream:
                await ctx.file_discovery_queue.put(priority_index)
            file.create_chunks()
            for c in file.chunks:
                if not ctx.is_running:
                    break
                if c.current_pos <= c.end:
                    if ctx.stream:
                        await ctx.chunk_queue.put((priority_index, c))
                    else:
                        await ctx.chunk_queue.put((c, priority_index))  # type: ignore
            ctx.current_file_id.append(priority_index)
            async with ctx.condition:
                if ctx.stream:
                    await ctx.condition.wait_for(lambda: not ctx.current_file_id)
                else:
                    await ctx.condition.wait_for(
                        lambda: len(ctx.current_file_id) < ctx.config.threads
                    )
        if ctx.stream:
            await ctx.file_discovery_queue.put(-1)
        async with ctx.condition:
            ctx.condition.notify_all()
