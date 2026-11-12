from __future__ import annotations

import asyncio
import logging
import signal

from .config import get_settings
from .db import Database
from .outbox import OutboxRelay

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("wms.outbox-relay")


async def main() -> None:
    settings = get_settings()
    db = Database(settings)
    await db.open()
    relay = OutboxRelay(db, settings)
    stop_event = asyncio.Event()

    def request_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            pass

    publisher = asyncio.create_task(relay.run_forever(), name="outbox-relay")
    logger.info("outbox relay started")
    try:
        await stop_event.wait()
    finally:
        publisher.cancel()
        try:
            await publisher
        except asyncio.CancelledError:
            pass
        await relay.stop()
        await db.close()
        logger.info("outbox relay stopped")


if __name__ == "__main__":
    asyncio.run(main())
