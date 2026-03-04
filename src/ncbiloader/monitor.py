# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import time
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Self

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    FileSizeColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)
from rich.progress_bar import ProgressBar
from rich.text import Text


def get_gradient_color(percentage: float) -> str:
    """Математика цвета: Красный -> Желтый -> Зеленый"""
    p = max(0, min(100, percentage or 0))
    if p < 50:
        r, g, b = 255, int((p / 50) * 255), 0
    else:
        r, g, b = int(255 - ((p - 50) / 50) * 255), 255, 0
    return f"#{r:02x}{g:02x}{b:02x}"


class GradientBar(BarColumn):
    """Ваша полоса, которая меняет цвет"""

    def render(self, task: Task) -> ProgressBar:
        # 1. Если задача завершена — включаем спецэффект
        if task.finished:
            # "blink" заставляет текст/полосу мигать (поддерживается не всеми терминалами)
            # "bold bright_green" делает цвет максимально насыщенным
            self.complete_style = "bold bright_green blink"
            self.finished_style = "bold bright_green blink"
        else:
            # 2. Если в процессе — считаем градиент как обычно
            self.complete_style = get_gradient_color(task.percentage)
            self.finished_style = "bold green"  # Стиль "по умолчанию" для конца
        return super().render(task)


class GradientPercent(ProgressColumn):
    """Ваш текст процентов, который меняет цвет"""

    def render(self, task: Task) -> Text:
        p = task.percentage
        color = get_gradient_color(p)
        return Text(f"{p:>5.1f}%", style=f"bold {color}")


class DynamicIconColumn(ProgressColumn):
    """A column that changes its icon based on progress."""

    def render(self, task: Task) -> Text:
        p = task.percentage

        if p < 100:
            return Text("📁")
        return Text("✅")


class ProgressMonitor:
    def __init__(self, silent: bool = False, log_file: Path | str | None = "download.log") -> None:
        self.silent = silent
        self.log_file = Path(log_file) if log_file else None
        self.console = Console(stderr=True)
        self.progress = None
        self.start_time = 0
        self.total_bytes = 0
        self.download_bytes = 0
        self.tasks: dict[str, TaskID] = {}
        self.dinamic_title = ""
        self.total_files = 0
        self.active_files: set[str] = set()
        self.files_completed = 0

        if not self.silent:
            # Настраиваем колонки: Имя, Бар, % , Размер, Скорость, Время
            self.progress = Progress(
                DynamicIconColumn(),
                TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
                GradientBar(bar_width=None, finished_style="bold green"),  # Заменили BarColumn
                GradientPercent(),
                "•",
                FileSizeColumn(),  # Сколько скачано
                "/",
                TotalFileSizeColumn(),  # Сколько всего
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

    def log(self, message: str, progress: bool = False) -> None:
        """Универсальный метод логирования"""
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        formatted_msg = f"{timestamp} {message}"

        # 1. Пишем в файл (всегда)
        self._write_log(formatted_msg)

        # 2. Вывод на экран
        if not self.silent:
            # Если работает прогресс-бар, пишем НАД ним
            if self.progress:
                if progress:
                    self.progress.console.print(message)  # Rich сам разберется с версткой
            else:
                self.console.print(message)
        else:
            # Если silent, пишем просто текст в stderr
            # (Rich умеет strip_styles, если надо убрать цвета для логов)
            self.console.print(message)

    def _write_log(self, msg: str) -> None:
        if self.log_file:
            # Открываем-закрываем (безопасно) или держим открытым (быстрее)
            # Для логов лучше 'a' (append)
            try:
                with self.log_file.open("a", encoding="utf-8") as f:
                    # Убираем Rich-теги из файла, если они есть (библиотека re или rich.text.Text)
                    from rich.text import Text

                    clean_msg = Text.from_markup(str(msg)).plain
                    f.write(f"{clean_msg}\n")
            except Exception:
                pass  # Логирование не должно ронять программу

    def add_file(self, filename: str, total_size: int) -> None:
        """Регистрирует новый файл в UI"""
        if self.progress:
            task_id = self.progress.add_task("download", filename=filename, total=total_size, visible=False)
            self.tasks[filename] = task_id
            self.total_bytes += total_size
            self.total_files += 1
            self._update_panel_title()

    def update(self, filename: str, advance_bytes: int) -> None:
        """
        Главный метод: обновляет прогресс конкретного файла.
        advance_bytes - сколько байт ТОЛЬКО ЧТО скачали (размер чанка)
        """
        # В тихом режиме мы НЕ пишем каждый чанк в лог (иначе диск лопнет от логов)
        self.download_bytes += advance_bytes
        if self.progress and filename in self.tasks:
            self.progress.update(self.tasks[filename], advance=advance_bytes, visible=True)
            if filename not in self.active_files:
                self.active_files.add(filename)
                self._update_panel_title()

    def _update_panel_title(self) -> None:
        """Updates the Panel title with real-time stats."""
        if not self.live or not self.progress:
            return

        # Count tasks
        active = len(self.active_files)

        # Create a dynamic string
        self.dinamic_title = f"[bold white]📥 Download Manager | [green]{self.files_completed}[/]/[blue]{self.total_files}[/] Files | [yellow]{active} Active[/]"

    def done(self, filename: str) -> None:
        if self.progress and filename in self.tasks:
            # Красивый финиш в UI
            task_id = self.tasks[filename]
            self.progress.update(task_id, completed=self.progress.tasks[task_id].total, visible=False)
            del self.tasks[filename]
            self.files_completed += 1
            self.active_files.remove(filename)
            self._update_panel_title()

        self.log(f"[bold green]✔ Done: {filename}[/]", True)

    def _make_panel(self) -> Panel | str:
        """This method now handles BOTH logic and UI creation 10x per second."""
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
            size_str = f"{self.download_bytes / 1024 / 1024 / 1024:.2f} / {self.total_bytes / 1024 / 1024 / 1024:.2f} GB"

        if not self.tasks and len(self.progress.tasks) > 0:
            # Показываем красивый финальный статус вместо пустых баров
            return Panel(
                "[bold green]✅ All downloads completed successfully![/]\n"
                f"[white]Total files:       [/][green3]{len(self.progress.tasks)}[/]\n"
                f"[white]Total Data:   [/][bold cyan]{size_str}[/]\n"
                f"[white]Average Speed:   [/][bold yellow]{speed_str}[/]\n"
                f"[white]Total Time:      [/][bold magenta]{time_str}[/]",
                title="[bold green]Final Report",
                border_style="green",
                expand=False,
            )
        dinamic_title2 = f"\nAvg: [yellow]{speed_str}[/] | Remainig Time: [green3]{remain_time_str}[/] | Time: [magenta]{time_str}[/] | Download: [bold cyan]{size_str}[/]"
        # Если задачи еще есть — рисуем стандартную панель
        return Panel(
            self.progress, title=self.dinamic_title + dinamic_title2, border_style="blue", padding=(1, 2)
        )

    def handle_exit(self, cancelled: bool = False) -> None:
        """Closes the UI and logs the session end."""
        if not self.silent:
            self._make_panel()
            self.live.stop()

        if not self.silent and cancelled:
            # Print a bold red panel to the console after the UI stops
            self.console.print("\n")  # Add some breathing room
            self.console.print(
                Panel(
                    "[bold red]⚠️ Operation Cancelled by User[/]\n"
                    "[dim white]Partial data may have been saved to the log file.",
                    title="[bold red]Interrupted",
                    border_style="red",
                    expand=False,
                )
            )

        self._write_log(
            "--- Session Terminated (User Interrupt) ---" if cancelled else "--- Session Finished ---"
        )

    def start(self) -> None:
        if self.progress:
            self.live.start()
        self.start_time = time.monotonic()
        self._write_log("--- Session Started ---")

    def stop(self) -> None:
        if self.progress:
            self._make_panel()
            self.live.stop()
        self._write_log("--- Session Finished ---")

    def __enter__(self) -> Self:
        if self.progress:
            self.live.start()
        self.start_time = time.monotonic()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        # If exc_type is KeyboardInterrupt, we mark it as cancelled
        is_interrupt = _exc_type is KeyboardInterrupt
        self.handle_exit(cancelled=is_interrupt)
        # Returning True would suppress the exception,
        # but usually we want to let it propagate or handle it in the main loop.


if __name__ == "__main__":
    filename = "my_file.none"
    with ProgressMonitor() as monitor:
        files: list[str] = []
        for i in range(0, 12):
            filename = f"filename{i!s}"
            monitor.add_file(filename, 1_000_000)
            files.append(filename)

        for filename in files:
            for j in range(1000, 1_000_001, 1000):
                monitor.update(filename, 1000)
                time.sleep(0.004)
            monitor.done(filename)
