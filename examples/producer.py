"""Demo producer: enqueues a steady, slightly bursty mix of jobs forever."""

from __future__ import annotations

import asyncio
import logging
import os
import random

from examples.demo import (
    Charge,
    ResizeImage,
    Webhook,
    WelcomeEmail,
    app,
    charge,
    deliver_webhook,
    resize_image,
    send_welcome,
)

log = logging.getLogger("producer")


async def produce(rate: float) -> None:
    async with app:
        sequence = 0
        while True:
            # Bursty traffic: the rate drifts between 50% and 150% of the target.
            burst = max(1, round(rate * random.uniform(0.5, 1.5)))
            for _ in range(burst):
                sequence += 1
                kind = random.random()
                if kind < 0.40:
                    await send_welcome.enqueue(
                        WelcomeEmail(user_id=sequence, email=f"user{sequence}@example.com"),
                        idempotency_key=f"welcome:{sequence}",
                    )
                elif kind < 0.65:
                    await resize_image.enqueue(
                        ResizeImage(image_id=sequence, width=random.choice([320, 640, 1280]))
                    )
                elif kind < 0.85:
                    await charge.enqueue(
                        Charge(invoice_id=sequence, amount_cents=random.randint(500, 50_000))
                    )
                else:
                    host = random.choice(["hooks.example.com", "flaky.example.net"])
                    await deliver_webhook.enqueue(
                        Webhook(url=f"https://{host}/events", event="invoice.paid"),
                        delay=random.choice([0, 0, 0, 5, 30]),
                    )
            log.info("enqueued %d jobs (total %d)", burst, sequence)
            await asyncio.sleep(1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    asyncio.run(produce(float(os.environ.get("PRODUCER_RATE", "15"))))


if __name__ == "__main__":
    main()
