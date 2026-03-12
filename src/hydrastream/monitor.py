# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Literal, Self

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    FileSizeColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)
from rich.progress_bar import ProgressBar
from rich.rule import Rule
from rich.table import Column, Table
from rich.text import Text

STATUS = Literal["SUCCESS", "INFO", "WARNING", "ERROR", "CRITICAL", "INTERRUPT"]


def truncate_filename(name: str, w: int = 30) -> str:
    """
    Truncates a long filename from the middle to preserve the extension and prefix.

    Args:
        name (str): Original filename.
        w (int): Maximum allowed width.

    Returns:
        str: Truncated string with ellipses if it exceeds the specified width.
    """
    return f"{name[: w // 2 - 1]}...{name[-w // 2 + 2 :]}" if len(name) > w else name


def get_gradient_color(percentage: float) -> str:
    """
    Calculates a hex color transitioning smoothly from Red (0%) to Yellow (50%) to Green (100%).

    Args:
        percentage (float): The completion percentage.

    Returns:
        str: Hex color code (e.g., '#ff0000').
    """
    p = max(0, min(100, percentage or 0))
    if p < 50:
        r, g, b = 255, int((p / 50) * 255), 0
    else:
        r, g, b = int(255 - ((p - 50) / 50) * 255), 255, 0
    return f"#{r:02x}{g:02x}{b:02x}"


class GradientBar(BarColumn):
    """Custom Rich BarColumn that applies a dynamic color gradient based on progress."""

    def render(self, task: Task) -> ProgressBar:
        # Color for indeterminate processes (pulsing effect)
        if task.total is None:
            self.complete_style = "cyan"
        elif task.finished:
            # High-visibility style for completed tasks
            self.complete_style = "bold bright_green blink"
        else:
            # Dynamic gradient calculation
            self.complete_style = get_gradient_color(task.percentage)
        return super().render(task)


class GradientPercent(ProgressColumn):
    """Custom Rich ProgressColumn that displays the percentage with a matching gradient color."""

    def render(self, task: Task) -> Text:
        if task.total is None:
            return Text(" CALC ", style="yellow")
        p = task.percentage
        color = get_gradient_color(p)
        return Text(f"{p:>5.1f}%", style=f"bold {color}")


class ProgressMonitor:
    """
    Manages the terminal UI and file-based logging for the download session.

    Provides a declarative Rich UI with live updates, global ETA, and a
    fallback mechanism ( mode) for headless server environments.
    """

    def __init__(
        self,
        no_ui: bool = False,
        quiet: bool = False,
        log_file: Path | str | None = "download.log",
    ) -> None:
        self.no_ui = no_ui
        self.quiet = quiet
        self.log_file = Path(log_file) if log_file else None
        self.is_running = True
        self._refresh = None

        # Route UI to stderr to keep stdout clean for data streams (Unix pipes)
        self.console = Console(stderr=True)
        self.progress = None

        self.start_time = 0
        self.total_bytes = 0
        self.download_bytes = 0
        self.tasks: dict[str, TaskID] = {}
        self._dynamic_title = ""
        self.total_files = 0
        self.active_files: set[str] = set()
        self.files_completed = 0
        self._date_printed = False
        self._buffer: dict[str, int] = defaultdict(int)
        self.refresh_per_second = 10
        self.renewal_rate = 1 / self.refresh_per_second
        self._log_throttle: dict[str, float] = {}

        if not (self.no_ui or self.quiet):
            self.progress = Progress(
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
                console=self.console,
                transient=False,
                expand=True,
            )

            self.live = Live(
                get_renderable=self._make_panel,
                console=self.console,
                auto_refresh=True,
                refresh_per_second=10,
            )

    async def log(
        self,
        message: str | Rule,
        status: STATUS = "INFO",
        progress: bool = False,
        throttle_key: str | None = None,
        throttle_sec: float = 10.0,
    ) -> None:
        """
        Universal logging method. Writes to the log file and conditionally
        renders to the terminal UI based on the operational mode.

        Args:
            message (str): The log message content.
            status (STATUS): Severity level dictating UI formatting.
            progress (bool): If True, forces display in the UI even if it's an INFO message.
        """
        if throttle_key:
            now = time.monotonic()
            last_time = self._log_throttle.get(throttle_key, 0.0)
            if now - last_time < throttle_sec:
                return
            self._log_throttle[throttle_key] = now

        timestamp = datetime.now().strftime("[%H:%M:%S]")
        formatted_msg = f"{timestamp} {message}"

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_log, formatted_msg)
        renderable = formatted_msg

        if self.quiet:
            return

        if self.progress:
            match status.upper():
                case "CRITICAL" | "INTERRUPT":
                    renderable = Panel(
                        f"[bold red]⚠️ {message}[/]\n[dim white]Partial data may have been saved.",
                        title="[bold red]Interrupted",
                        border_style="red",
                        expand=False,
                    )
                case "ERROR":
                    renderable = Panel(
                        f"[bold red]{message}[/]", title="Error", border_style="red", padding=(0, 1)
                    )
                case "WARNING":
                    renderable = f"⚠️ [yellow]{formatted_msg}[/]"
                case "INFO":
                    renderable = f"[white]{formatted_msg}[/]"
                case "SUCCESS":
                    renderable = f"✅ [green]{formatted_msg}[/]"
                case _:
                    renderable = message

            if progress or status in ["WARNING", "ERROR", "CRITICAL", "INTERRUPT"]:
                self.progress.console.print(renderable)
        else:
            self.console.print(Text.from_markup(str(renderable)).plain)

    async def _date_print(self) -> None:
        if not getattr(self, "_date_printed", False):
            current_date = datetime.now().strftime("%Y-%m-%d")
            date_header = f"[bold cyan]📅 Date: {current_date}[/]"

            if not (self.no_ui or self.quiet):
                self.console.print(Rule(date_header))
            await self.log(f"--- {current_date} ---")
            self._date_printed = True

    def _write_log(self, msg: str) -> None:
        """Appends a raw text message to the log file, stripping ANSI UI tags."""
        if not self.log_file:
            return

        try:
            with self.log_file.open("a", encoding="utf-8") as f:
                clean_msg = Text.from_markup(str(msg)).plain
                f.write(f"{clean_msg}\n")
        except OSError:
            # Graceful degradation: do not crash the app if logging fails
            pass

    def add_file(self, filename: str, total_size: int | None = None) -> None:
        """Registers a new file in the UI, keeping it hidden until data arrives."""
        if total_size is not None:
            self.total_bytes += total_size
            self.total_files += 1

        if self.progress:
            t_filename = truncate_filename(filename)
            if total_size is None:
                task_id = self.progress.add_task("Download MD5 for", filename=t_filename, total=total_size)
            else:
                task_id = self.progress.add_task(
                    "Download file", filename=t_filename, total=total_size, visible=False
                )
            self.tasks[filename] = task_id
            self._update_panel_title()

    def update(self, filename: str, advance_bytes: int) -> None:
        """Instantly writes bytes to the memory buffer."""

        self._buffer[filename] += advance_bytes
        self.download_bytes += advance_bytes

    async def _refresh_loop(self) -> None:
        """Resets the accumulated buffer in the UI (Rich)"""

        if self.progress:
            while self.is_running:
                try:
                    current_batch = self._buffer.copy()
                    self._buffer.clear()

                    for filename, bytes_to_advance in current_batch.items():
                        if bytes_to_advance > 0 and filename in self.tasks:
                            self.progress.update(self.tasks[filename], advance=bytes_to_advance, visible=True)
                            if filename not in self.active_files:
                                self.active_files.add(filename)
                                self._update_panel_title()

                    await asyncio.sleep(self.renewal_rate)
                except Exception as e:
                    await self.log(f"UI Refresh Error: {e!r}", status="ERROR")

    def _update_panel_title(self) -> None:
        """Re-evaluates the active task count and updates the dynamic title string."""
        if not self.live or not self.progress:
            return

        # Count tasks
        active = len(self.active_files)

        # Create a dynamic string
        self._dynamic_title = (
            f"[bold white][green]{self.files_completed}[/]/"
            f"[blue]{self.total_files}[/] Files | [yellow]{active} Active[/]"
        )

    async def done(self, filename: str) -> None:
        """Marks a file task as complete, hides its progress bar, and updates metrics."""
        if self.progress and filename in self.tasks:
            task_id = self.tasks[filename]
            self.progress.update(task_id, completed=self.progress.tasks[task_id].total, visible=False)
            del self.tasks[filename]
            self.active_files.discard(filename)

            if self.progress.tasks[task_id].total is not None:
                self.files_completed += 1
                self._update_panel_title()
                await self.log(f"Done: {filename}", status="SUCCESS", progress=True)

        else:
            if self._buffer.get(filename, 0):
                self.files_completed += 1
                await self.log(f"Done: {filename}", status="SUCCESS", progress=True)

    def _make_panel(self) -> Panel | str:
        """
        Declarative rendering method called rapidly by Rich Live.
        Evaluates global state to generate either the active downloads UI or the final report.
        """
        if not self.progress:
            return ""

        # 2. Logic: If nothing to show, return empty string (removes the box)
        if not self.tasks and len(self.progress.tasks) == 0:
            return ""

        elapsed = time.monotonic() - self.start_time
        avg_speed = self.download_bytes / elapsed if elapsed > 0 else 0
        speed_str = f"{avg_speed / 1024 / 1024:.2f} MB/s"

        mins, secs = divmod(int(elapsed), 60)
        hours = 0
        if mins >= 60:
            hours, mins = divmod(mins, 60)
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

        remain_time = (
            (self.total_bytes - self.download_bytes) / avg_speed if self.total_bytes and avg_speed else 0
        )

        r_mins, r_secs = divmod(int(remain_time), 60)
        r_hours = 0
        if r_mins >= 60:
            r_hours, r_mins = divmod(r_mins, 60)
        remain_time_str = f"{r_hours:02d}:{r_mins:02d}:{r_secs:02d}"

        if self.total_bytes < 1_073_741_824:  # GB
            size_str = f"{self.download_bytes / 1024 / 1024:.2f} / {self.total_bytes / 1024 / 1024:.2f} MB"
        else:
            size_str = (
                f"{self.download_bytes / 1024 / 1024 / 1024:.2f} /"
                f" {self.total_bytes / 1024 / 1024 / 1024:.2f} GB"
            )

        # Trigger final report rendering if all tasks are complete
        if not self.tasks and len(self.progress.tasks) > 0:
            grid = Table.grid(expand=True)
            grid.add_column()
            grid.add_column(justify="center")

            content = Group("[green]✅ All downloads completed successfully!\n", grid)
            grid.add_row("[white]Total files:", f"[green3]{self.files_completed}/{self.total_files}[/]")
            grid.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
            grid.add_row("[white]Average Speed:", f"[bold yellow]{speed_str}[/]")
            grid.add_row("[white]Total Time:", f"[bold magenta]{time_str}[/]")

            return Panel(
                content,
                title="[#2e8b57]Final Report",
                border_style="#2e8b57",
                expand=False,
            )

        dynamic_title_full = (
            f"\nAvg: [yellow]{speed_str}[/] | Remaining Time: [green3]{remain_time_str}[/] | "
            f"Time: [magenta]{time_str}[/] | Download: [bold cyan]{size_str}[/][/]"
        )

        return Panel(
            self.progress, title=self._dynamic_title + dynamic_title_full, border_style="blue", padding=(1, 2)
        )

    async def handle_exit(self, cancelled: bool = False) -> None:
        """Closes the UI and logs the session end, generating a summary report."""
        if self.progress:
            self.live.stop()
            if self._refresh:
                self._refresh.cancel()

        elapsed = time.monotonic() - self.start_time
        avg_speed = (self.download_bytes / elapsed) / 1024 / 1024 if elapsed > 0 else 0

        total_mb = self.download_bytes / 1024 / 1024
        mins, secs = divmod(int(elapsed), 60)
        hours, mins = divmod(mins, 60)
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

        status_word = "CANCELLED" if cancelled or not self.is_running else "SUCCESS"

        report = (
            f"\n--- Final Report ({status_word}) ---\n"
            f"Total files:   {self.files_completed}/{self.total_files}\n"
            f"Total Data:    {total_mb:.2f} MB\n"
            f"Average Speed: {avg_speed:.2f} MB/s\n"
            f"Total Time:    {time_str}\n"
            f"--------------------------------"
        )

        await self.log(report)

    async def start(self) -> None:
        """Initializes the display engine and internal timers."""
        if self.progress:
            self.live.start()
            self._refresh = asyncio.create_task(self._refresh_loop())
        self.start_time = time.monotonic()
        self._write_log("--- Session Started ---")
        await self._date_print()

    async def stop(self) -> None:
        """Manually stops the monitor."""
        await self.handle_exit()
        self._write_log("--- Session Finished ---")

    async def __aenter__(self) -> Self:
        if self.progress:
            self.live.start()
            self._refresh = asyncio.create_task(self._refresh_loop())
        self.start_time = time.monotonic()
        self._write_log("--- Session Started ---")
        await self._date_print()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None = None,
        _exc: BaseException | None = None,
        _tb: TracebackType | None = None,
    ) -> None:
        is_cancelled = _exc_type in (KeyboardInterrupt, asyncio.CancelledError)
        await self.handle_exit(cancelled=is_cancelled)
        self._write_log("--- Session Finished ---")
