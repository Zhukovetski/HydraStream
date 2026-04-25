import asyncio
import math
import time
from dataclasses import field

from hydrastream.actors.controller import ScaleDownSignal, ScaleUpSignal
from hydrastream.exceptions import LogStatus
from hydrastream.models import HydraContext, my_dataclass
from hydrastream.monitor import log
from hydrastream.utils import format_size


@my_dataclass
class TelemetryAnalyzer:
    ctx: HydraContext
    current_limit: int = field(init=False)
    controller_outbox: asyncio.Queue[object]
    worker_events: list[asyncio.Event]
    smoothed_speed: float = 0.0
    prev_speed: float = 0.0
    tau: float = 1.0
    min_window: int = 1024
    sensitivity: float = 0.05

    def __post_init__(self) -> None:
        self.current_limit = self.ctx.dynamic_limit

    async def run(self) -> None:
        while not self.ctx.sync.all_complete.is_set():
            try:
                await self.ctx.ui.speed.controller_checkpoint_event.wait()
                if self.ctx.sync.stop_adaptive_controller.is_set():
                    break
                self.ctx.ui.speed.controller_checkpoint_event.clear()

                # Просто делаем шаг
                await self._step()

            except Exception as e:
                if self.ctx.config.debug:
                    raise
                await log(
                    self.ctx.ui,
                    f"Adaptive controller failed: {e}",
                    status=LogStatus.ERROR,
                )

    def _calculate_ema(self, speed_now: float, elapsed: float) -> float:
        if self.smoothed_speed == 0.0:
            return speed_now
        alpha = 1.0 - math.exp(-elapsed / self.tau)
        return (alpha * speed_now) + ((1.0 - alpha) * self.smoothed_speed)

    def _update_window(self, speed_now: float, elapsed: float) -> None:
        safe_speed = max(speed_now, 0.001)
        coef = 1 / safe_speed**0.25
        new_bytes = int(self.ctx.ui.speed.bytes_to_check * (1 - coef + elapsed))
        self.ctx.ui.speed.bytes_to_check = max(self.min_window, new_bytes)

    async def _log_scale_event(self, direction: str, speed: float) -> None:
        """Вспомогательный метод для чистого логирования."""
        if direction == "up":
            msg = (
                f"Speed increased to {format_size(speed)}/s. "
                f"Scaling up to {self.current_limit} workers."
            )
            status = LogStatus.INFO
            key = "scale_up"
        else:
            msg = f"Network congested. Scaling down to {self.current_limit} workers."
            status = LogStatus.WARNING
            key = "scale_down"

        await log(self.ctx.ui, msg, status=status, throttle_key=key, throttle_sec=5.0)

    async def _step(self) -> None:
        """Один шаг адаптации."""
        now = time.monotonic()
        elapsed = min(1, now - self.ctx.ui.speed.last_checkpoint_time)
        if elapsed <= 0:
            return

        speed_now = self.ctx.ui.speed.bytes_to_check / elapsed
        self.smoothed_speed = self._calculate_ema(speed_now, elapsed)
        self._update_window(speed_now, elapsed)

        # Логика изменения лимита
        if self.smoothed_speed > self.prev_speed * (1 + self.sensitivity):
            if self.current_limit < self.ctx.config.threads:
                self.current_limit += 1
                self.prev_speed = self.smoothed_speed
                await self._log_scale_event("up", speed_now)

        elif (
            self.smoothed_speed < self.prev_speed * (1 - self.sensitivity)
            and self.current_limit > 2
        ):
            self.current_limit -= 1
            self.prev_speed = self.smoothed_speed
            await self._log_scale_event("down", speed_now)

        # Применяем изменения
        if self.ctx.dynamic_limit != self.current_limit:
            await self.controller_outbox.put(
                ScaleUpSignal()
                if self.current_limit > self.ctx.dynamic_limit
                else ScaleDownSignal()
            )
            self.ctx.dynamic_limit = self.current_limit

        self.ctx.ui.speed.last_checkpoint_time = time.monotonic()

        # if self.worker_id >= self.ctx.dynamic_limit:
        #     async with self.ctx.sync.dynamic_limit:
        #         await self.ctx.sync.dynamic_limit.wait_for(
        #             lambda: self.worker_id < self.ctx.dynamic_limit
        #         )
