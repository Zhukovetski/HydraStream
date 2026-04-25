import asyncio

from hydrastream.engine import send_poison_pills
from hydrastream.models import Chunk, Envelope, my_dataclass


@my_dataclass
class MemoryThrottler:
    chunk_inbox: asyncio.PriorityQueue[Envelope[Chunk | None]]
    credit_inbox: asyncio.Queue[int]
    worker_outbox: asyncio.PriorityQueue[Envelope[Chunk | None]]

    budget: int = 50 * 1024 * 1024

    num_workers: int

    async def run(self) -> None:
        pending_chunk: Envelope[Chunk | None] | None = None

        while True:
            # 1. Если у нас нет чанка на руках, берем его из очереди
            if pending_chunk is None:
                pending_chunk = await self.chunk_inbox.get()

                # Обработка пилюли (прокидываем воркерам и выходим)
                if pending_chunk.is_poison_pill:
                    await send_poison_pills(self.worker_outbox, self.num_workers)
                    break

            if not pending_chunk.payload:
                continue

            # 2. Если на этот чанк НЕ ХВАТАЕТ денег - ЖДЕМ ТОЛЬКО КРЕДИТОВ!
            if pending_chunk.payload.size > self.budget:
                credit = await self.credit_inbox.get()
                self.budget += credit
                continue  # Идем на новый круг проверять бюджет!

            # 3. ДЕНЬГИ ЕСТЬ! Отдаем чанк воркерам
            self.budget -= pending_chunk.payload.size
            await self.worker_outbox.put(pending_chunk)

            # Освобождаем руки для следующего чанка
            pending_chunk = None
