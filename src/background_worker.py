import asyncio
import logging
import signal

from .config import get_settings
from .main import app


logger = logging.getLogger(__name__)


async def _run_worker() -> None:
    settings = get_settings()
    if not bool(getattr(settings, "background_loops_enabled", True)):
        raise RuntimeError("Background worker started with background loops disabled")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    logger.info(
        "Starting Synapse background worker role=%s background_loops_enabled=%s",
        getattr(settings, "runtime_role", "worker"),
        bool(getattr(settings, "background_loops_enabled", True)),
    )
    async with app.router.lifespan_context(app):
        logger.info(
            "Synapse background worker ready started_loops=%s",
            ",".join(getattr(app.state, "background_loops_started", []) or []) or "none",
        )
        await stop_event.wait()
    logger.info("Synapse background worker stopped")


def main() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
