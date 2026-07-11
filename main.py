"""
Entry point – OKX Liquidity Sweep Telegram Bot.

Run locally:
  python main.py

Environment variables required:
  TELEGRAM_BOT_TOKEN  – from @BotFather
  TELEGRAM_CHAT_ID    – target chat / channel / group ID
"""

import asyncio
import logging
import sys

# Validate env vars early so we fail fast with a clear message.
try:
    from bot.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
except KeyError as e:
    print(f"[ERROR] Missing required environment variable: {e}", file=sys.stderr)
    sys.exit(1)

from bot.monitor import Monitor


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Quiet noisy libraries
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


async def _main() -> None:
    _configure_logging()
    logger = logging.getLogger("main")
    logger.info("Starting OKX Liquidity Sweep Bot …")
    monitor = Monitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(_main())
