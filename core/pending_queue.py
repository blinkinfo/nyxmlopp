"""Persistent retry queue for unresolved slots.

Slots that exhaust all polling attempts are written here.
A background reconciler retries them every 5 minutes indefinitely.
Survives Railway restarts — backed by data/pending_slots.json.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

_QUEUE_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_QUEUE_PATH = os.path.join(_QUEUE_DIR, "pending_slots.json")
_lock = asyncio.Lock()


def _load() -> list[dict[str, Any]]:
    if not os.path.exists(_QUEUE_PATH):
        return []
    try:
        with open(_QUEUE_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        log.warning("Could not read pending_slots.json \u2014 starting fresh")
        return []


def _save(items: list[dict[str, Any]]) -> None:
    os.makedirs(_QUEUE_DIR, exist_ok=True)
    with open(_QUEUE_PATH, "w") as f:
        json.dump(items, f, indent=2)


async def add_pending(
    signal_id: int,
    slug: str,
    side: str,
    entry_price: float,
    slot_start: str,
    slot_end: str,
    trade_id: int | None,
    amount_usdc: float | None,
) -> None:
    """Add a slot to the persistent retry queue."""
    async with _lock:
        items = _load()
        # Avoid duplicates
        if any(i["signal_id"] == signal_id for i in items):
            return
        items.append({
            "signal_id": signal_id,
            "slug": slug,
            "side": side,
            "entry_price": entry_price,
            "slot_start": slot_start,
            "slot_end": slot_end,
            "trade_id": trade_id,
            "amount_usdc": amount_usdc,
        })
        _save(items)
        log.info("Added signal %d to pending retry queue (slug=%s)", signal_id, slug)


async def remove_pending(signal_id: int) -> None:
    """Remove a slot from the queue once resolved."""
    async with _lock:
        items = _load()
        items = [i for i in items if i["signal_id"] != signal_id]
        _save(items)
        log.info("Removed signal %d from pending retry queue", signal_id)


async def list_pending() -> list[dict[str, Any]]:
    """Return all pending slots."""
    async with _lock:
        return _load()


async def clear_all() -> None:
    """Clear the entire queue (for testing/admin use)."""
    async with _lock:
        _save([])
