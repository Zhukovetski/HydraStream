# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import orjson
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    FileSizeColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)
from rich.progress_bar import ProgressBar
from rich.rule import Rule
from rich.table import Column, Table
from rich.text import Text

from .models import File, UIState

STATUS = Literal["SUCCESS", "INFO", "WARNING", "ERROR", "CRITICAL", "INTERRUPT"]


def truncate_filename(name: str, w: int = 30) -> str:
    return f"{name[: w // 2 - 1]}...{name[-w // 2 + 2 :]}" if len(name) > w else name


def get_gradient_color(percentage: float) -> str:
    p = max(0, min(100, percentage or 0))
    if p < 50:
        r, g, b = 255, int((p / 50) * 255), 0
    else:
        r, g, b = int(255 - ((p - 50) / 50) * 255), 255, 0
    return f"#{r:02x}{g:02x}{b:02x}"


class GradientBar(BarColumn):
    def render(self, task: Task) -> ProgressBar:
        if task.total is None:
            self.complete_style = "cyan"
        elif task.finished:
            self.complete_style = "bold bright_green blink"
        else:
            self.complete_style = get_gradient_color(task.percentage)
        return super().render(task)


class GradientPercent(ProgressColumn):
    def render(self, task: Task) -> Text:
        if task.total is None:
            return Text(" CALC ", style="yellow")
        p = task.percentage
        color = get_gradient_color(p)
        return Text(f"{p:>5.1f}%", style=f"bold {color}")


def write_log(ctx: UIState, msg: str) -> None:
    if not ctx.log_file:
        return

    try:
        with Path(ctx.log_file).open("a", encoding="utf-8") as f:
            clean_msg = Text.from_markup(str(msg)).plain
            f.write(f"{clean_msg}\n")
    except OSError:
        pass


async def date_print(ctx: UIState) -> None:
    current_date = datetime.now().strftime("%Y-%m-%d")
    date_header = f"[bold cyan] Date: {current_date}[/]"

    if not (ctx.no_ui or ctx.quiet):
        ctx.console.print(Rule(date_header))
    await log(ctx, f"--- {current_date} ---")


def formatting_log(
    message: str | Rule, formatted_msg: str, status: STATUS = "INFO"
) -> Panel | str | Rule:
    match status.upper():
        case "CRITICAL" | "INTERRUPT":
            renderable = Panel(
                f"[bold red]{message}[/]\n[dim white]Partial data may have been saved.",
                title="[bold red]Interrupted",
                border_style="red",
                expand=False,
            )
        case "ERROR":
            renderable = Panel(
                f"[bold red]{message}[/]",
                title="Error",
                border_style="red",
                padding=(0, 1),
            )
        case "WARNING":
            renderable = f"[yellow]{formatted_msg}[/]"
        case "INFO":
            renderable = f"[white]{formatted_msg}[/]"
        case "SUCCESS":
            renderable = f"[green]{formatted_msg}[/]"
        case _:
            renderable = message
    return renderable


async def log(
    ctx: UIState,
    message: str | Rule,
    *,
    status: STATUS = "INFO",
    progress: bool = False,
    throttle_key: str | None = None,
    throttle_sec: float = 10.0,
    **kwargs: object,
) -> None:
    if throttle_key:
        now = time.monotonic()
        last_time = ctx.log_throttle.get(throttle_key, 0.0)
        if now - last_time < throttle_sec:
            return
        ctx.log_throttle[throttle_key] = now
    if ctx.json_logs:
        # Собираем словарь для JSON
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": status.upper(),
            "message": message,
            **kwargs,  # Распаковываем дополнительные данные!
        }
        # Сериализуем в байты, потом в строку
        final_msg = orjson.dumps(log_record).decode("utf-8")
    else:
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        final_msg = f"{timestamp} {message}"

    if ctx.log_file:
        clean_msg = Text.from_markup(str(final_msg)).plain
        ctx.log_queue.put_nowait(clean_msg)

    if ctx.quiet:
        return

    if ctx.json_logs:
        ctx.console.print(final_msg)
        return

    renderable = final_msg
    if ctx.progress:
        renderable = formatting_log(message, renderable, status)
        if progress or status in ["WARNING", "ERROR", "CRITICAL", "INTERRUPT"]:
            ctx.progress.console.print(renderable)
    else:
        ctx.console.print(Text.from_markup(str(renderable)).plain)


async def log_worker(ctx: UIState) -> None:
    """
    Фоновый воркер. Живет всё время работы программы.
    Берет строки из очереди и пишет в ОТКРЫТЫЙ файл.
    """
    if not ctx.log_fd:
        return

    while True:
        msg = await ctx.log_queue.get()

        # Ядовитая пилюля для остановки логгера
        if msg is None:
            ctx.log_queue.task_done()
            break

        try:
            ctx.log_fd.write(f"{msg}\n")
            ctx.log_fd.flush()  # Гарантируем, что строка сразу упала на диск
        except OSError:
            pass  # Если диск отвалился, просто глотаем ошибку
        finally:
            ctx.log_queue.task_done()


async def speed_limiter(ctx: UIState) -> None:
    prev = 0
    time = ctx.time_speed_limit
    if ctx.speed_limit is None:
        return
    while ctx.is_running:
        d_time = (ctx.download_bytes - prev) / (ctx.speed_limit)
        if d_time > 1:
            ctx.limit_event.clear()
            await asyncio.sleep(ctx.time_speed_limit * d_time)
            ctx.limit_event.set()
        prev = ctx.download_bytes
        await asyncio.sleep(time)


def add_file(ctx: UIState, filename: str, total_size: int | None = None) -> None:
    if total_size is not None:
        ctx.total_bytes += total_size
        ctx.total_files += 1

    if ctx.progress:
        t_filename = truncate_filename(filename)
        if total_size is None:
            task_id = ctx.progress.add_task(
                "Download Hash for", filename=t_filename, total=total_size
            )
        else:
            task_id = ctx.progress.add_task(
                "Download file", filename=t_filename, total=total_size, visible=False
            )
        ctx.tasks[filename] = task_id
        update_panel_title(ctx)


def update(ctx: UIState, filename: str, advance_bytes: int) -> None:
    ctx.buffer[filename] += advance_bytes
    ctx.download_bytes += advance_bytes


async def refresh_loop(ctx: UIState) -> None:
    if ctx.progress:
        while ctx.is_running:
            try:
                current_batch = ctx.buffer.copy()
                ctx.buffer.clear()

                for filename, bytes_to_advance in current_batch.items():
                    if bytes_to_advance > 0 and filename in ctx.tasks:
                        ctx.progress.update(
                            ctx.tasks[filename], advance=bytes_to_advance, visible=True
                        )
                        if filename not in ctx.active_files:
                            ctx.active_files.add(filename)
                            update_panel_title(ctx)

                await asyncio.sleep(ctx.renewal_rate)
            except Exception as e:
                await log(ctx, f"UI Refresh Error: {e!r}", status="ERROR")


def update_panel_title(ctx: UIState) -> None:
    if not ctx.live:
        ctx.dynamic_title = ""

    active = len(ctx.active_files)
    ctx.dynamic_title = (
        f"[bold white][green]{ctx.files_completed}[/]/"
        + f"[blue]{ctx.total_files}[/] Files | [yellow]{active} Active[/]"
    )


async def done(ctx: UIState, filename: str) -> None:
    if ctx.progress and filename in ctx.tasks:
        task_id = ctx.tasks[filename]
        ctx.progress.update(
            task_id, completed=ctx.progress.tasks[task_id].total, visible=False
        )
        del ctx.tasks[filename]
        ctx.active_files.discard(filename)

        if ctx.progress.tasks[task_id].total is not None:
            ctx.files_completed += 1
            update_panel_title(ctx)
            await log(ctx, f"Done: {filename}", status="SUCCESS", progress=True)

    elif ctx.buffer.get(filename, 0):
        ctx.files_completed += 1
        await log(ctx, f"Done: {filename}", status="SUCCESS", progress=True)


def make_panel(ctx: UIState) -> Panel | str:
    if not ctx.progress:
        return ""

    if not ctx.tasks and len(ctx.progress.tasks) == 0:
        return ""

    elapsed = time.monotonic() - ctx.start_time
    avg_speed = ctx.download_bytes / elapsed if elapsed > 0 else 0
    speed_str = f"{avg_speed / 1024 / 1024:.2f} MB/s"

    mins, secs = divmod(int(elapsed), 60)
    hours = 0
    if mins >= 60:
        hours, mins = divmod(mins, 60)
    time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

    remain_time = (
        (ctx.total_bytes - ctx.download_bytes) / avg_speed
        if ctx.total_bytes and avg_speed
        else 0
    )

    r_mins, r_secs = divmod(int(remain_time), 60)
    r_hours = 0
    if r_mins >= 60:
        r_hours, r_mins = divmod(r_mins, 60)
    remain_time_str = f"{r_hours:02d}:{r_mins:02d}:{r_secs:02d}"

    if ctx.total_bytes < 1_073_741_824:
        size_str = (
            f"{ctx.download_bytes / (1024**2):.2f}/{ctx.total_bytes / (1024**2):.2f} MB"
        )
    else:
        size_str = (
            f"{ctx.download_bytes / (1024**3):.2f}/{ctx.total_bytes / (1024**3):.2f} GB"
        )

    if not ctx.tasks and ctx.total_files > 0 and ctx.total_files == ctx.files_completed:
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="center")

        content = Group("[green]All downloads completed successfully!\n", grid)
        grid.add_row(
            "[white]Total files:", f"[green3]{ctx.files_completed}/{ctx.total_files}[/]"
        )
        grid.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
        grid.add_row("[white]Average Speed:", f"[bold yellow]{speed_str}[/]")
        grid.add_row("[white]Total Time:", f"[bold magenta]{time_str}[/]")
        return Panel(
            content,
            title="[#2e8b57]Final Report",
            border_style="#2e8b57",
            expand=False,
        )

    if ctx.dry_run:
        if ctx.total_bytes < 1_073_741_824:
            size_str = f"{ctx.total_bytes / (1024**2):.2f} MB"
        else:
            size_str = f"{ctx.total_bytes / (1024**3):.2f} GB"
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="center")

        grid.add_row("[white]Total files:", f"[green3]{ctx.total_files}[/]")
        grid.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
        return Panel(
            grid,
            title="[#2e8b57]Final Report",
            border_style="#2e8b57",
            expand=False,
        )

    dynamic_title_full = (
        f"\nAvg: [yellow]{speed_str}[/] | "
        f"Remaining Time: [green3]{remain_time_str}[/] | "
        f"Time: [magenta]{time_str}[/] | Download: [bold cyan]{size_str}[/]"
    )

    return Panel(
        ctx.progress,
        title=ctx.dynamic_title + dynamic_title_full,
        border_style="blue",
        padding=(1, 2),
    )


async def print_dry_run_report(
    ctx: UIState, files: dict[int, File], stream: bool, output_dir: str | Path
) -> None:
    """Выводит отчет о том, что БЫЛО БЫ сделано, без фактического скачивания."""

    # 1. Если включен режим JSON логов, отдаем структурированные данные
    if ctx.json_logs:
        report_data = {
            "total_files": ctx.total_files,
            "total_bytes": ctx.total_bytes,
            "files": [
                {
                    "filename": f.meta.filename,
                    "size_bytes": f.meta.content_length,
                    "chunks": len(f.chunks),
                    "supports_ranges": f.meta.supports_ranges,
                    "algorithm": f.meta.expected_checksum.algorithm
                    if f.meta.expected_checksum
                    else None,
                    "expected_hash": f.meta.expected_checksum.value
                    if f.meta.expected_checksum
                    else None,
                }
                for f in files.values()
            ],
        }
        # Отправляем в твой универсальный логгер

        await log(
            ctx,
            "DRY_RUN_REPORT",
            status="INFO",
            progress=False,
            throttle_key=None,
            throttle_sec=10,
            **report_data,
        )
        return

    # 2. Если режим тишины (без JSON), просто выходим
    if ctx.quiet:
        return

    # 3. Красивый UI для людей
    table = Table(title="[bold yellow] DRY RUN REPORT (No data will be downloaded)[/]")
    table.add_column("Filename", style="cyan", no_wrap=True)
    table.add_column("Size", justify="right")
    table.add_column("Chunks", justify="right")
    if ctx.verify:
        table.add_column("Hash Found", justify="center")
    table.add_column("Ranges", justify="center")

    for f in files.values():
        f.create_chunks()
        size_mb = f.meta.content_length / (1024 * 1024)
        if ctx.verify:
            has_hash = "✅" if f.meta.expected_checksum else "❌"
        ranges = "✅" if f.meta.supports_ranges else "❌ (Fallback to 1 thread)"
        if ctx.verify:
            table.add_row(
                f.meta.filename,
                f"{size_mb:.2f} MB",
                str(len(f.chunks)),
                has_hash,
                ranges,
            )
        else:
            table.add_row(
                f.meta.filename, f"{size_mb:.2f} MB", str(len(f.chunks)), ranges
            )
    # Печатаем таблицу в stderr (чтобы не сломать пайпы)
    ctx.console.print(table)

    # 4. Проверка свободного места (Только для режима диска)
    if not stream:
        # ctx.config.output_dir мы парсим через Path, как ты делал в Storage

        output_dir = Path(output_dir).expanduser().resolve()

        # Если папки еще нет, проверяем место на родительском диске
        check_dir = output_dir if output_dir.exists() else output_dir.parent

        try:
            free_space = shutil.disk_usage(check_dir).free

            if free_space < ctx.total_bytes:
                ctx.console.print("\n[bold red] DANGER: Insufficient disk space![/]")
                ctx.console.print(
                    f"[red]Required: {ctx.total_bytes / (1024**3):.2f} GB | "
                    f"Available: {free_space / (1024**3):.2f} GB[/]"
                )
            else:
                ctx.console.print(
                    f"\n[bold green] Disk space check passed "
                    f"({free_space / (1024**3):.2f} GB free).[/]"
                )
        except OSError:
            pass  # Игнорируем ошибки доступа при проверке места


async def handle_exit(ctx: UIState, cancelled: bool = False) -> None:
    if ctx.live:
        ctx.live.stop()
        if ctx.refresh:
            ctx.refresh.cancel()
    elapsed = time.monotonic() - ctx.start_time
    avg_speed = (ctx.download_bytes / elapsed) / (1024**2) if elapsed > 0 else 0

    if ctx.total_bytes < 1_073_741_824:
        size_str = (
            f"{ctx.download_bytes / (1024**2):.2f}/{ctx.total_bytes / (1024**2):.2f} MB"
        )
    else:
        size_str = (
            f"{ctx.download_bytes / (1024**3):.2f}/{ctx.total_bytes / (1024**3):.2f} GB"
        )

    mins, secs = divmod(int(elapsed), 60)
    hours, mins = divmod(mins, 60)
    time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

    status_word = "CANCELLED" if cancelled else "SUCCESS"

    report = (
        f"\n--- Final Report ({status_word}) ---\n"
        f"Total files:   {ctx.files_completed}/{ctx.total_files}\n"
        f"Total Data:    {size_str}\n"
        f"Average Speed: {avg_speed:.2f} MB/s\n"
        f"Total Time:    {time_str}\n"
        f"--------------------------------"
    )
    await log(ctx, report)


async def ui_start(ctx: UIState) -> None:
    # 1. ОТКРЫВАЕМ ФАЙЛ ЛОГОВ И ЗАПУСКАЕМ ВОРКЕРА
    if ctx.log_file:
        try:
            # Открываем в режиме Append (дозапись)
            ctx.log_fd = ctx.log_file.open("a", encoding="utf-8")
            ctx.log_task = asyncio.create_task(log_worker(ctx))
        except OSError:
            ctx.log_fd = None

    if not (ctx.no_ui or ctx.quiet):
        ctx.progress = Progress(
            SpinnerColumn("aesthetic"),
            TextColumn("[bold yellow]{task.description}"),
            TextColumn(
                "[bold blue]{task.fields[filename]}",
                justify="left",
                table_column=Column(overflow="ellipsis", no_wrap=True, width=30),
            ),
            GradientBar(bar_width=None, finished_style="green"),
            GradientPercent(),
            "•",
            FileSizeColumn(),
            "/",
            TotalFileSizeColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=ctx.console,
            transient=False,
            expand=True,
        )

        ctx.live = Live(
            get_renderable=lambda: make_panel(ctx),
            console=ctx.console,
            auto_refresh=True,
            refresh_per_second=10,
            transient=False,
        )
        ctx.live.start()
        ctx.refresh = asyncio.create_task(refresh_loop(ctx))
    ctx.start_time = time.monotonic()
    await log(ctx, "--- Session Started ---")
    await date_print(ctx)


async def ui_stop(ctx: UIState) -> None:
    await handle_exit(ctx)
    await log(ctx, "--- Session Finished ---")

    # 2. КОРРЕКТНО ГАСИМ ЛОГГЕР
    if ctx.log_task:
        # Отправляем ядовитую пилюлю
        ctx.log_queue.put_nowait(None)
        # Ждем, пока логгер допишет все оставшиеся в очереди строки на диск
        await ctx.log_task

    # 3. ЗАКРЫВАЕМ ФАЙЛОВЫЙ ДЕСКРИПТОР
    if ctx.log_fd:
        ctx.log_fd.close()
        ctx.log_fd = None
