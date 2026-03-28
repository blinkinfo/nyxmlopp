"""Signal engine — check slot N prices at T-85 s and emit a signal for slot N+1."""

from __future__ import annotations

import logging
from typing import Any

import config as cfg
from polymarket.markets import (
    get_current_slot_info,
    get_next_slot_info,
    get_slot_prices,
)

log = logging.getLogger(__name__)


async def check_signal() -> dict[str, Any] | None:
    """Called at T-85 s before the current slot N ends.

    1. Fetch slot N live prices from Gamma API.
    2. If UP price >= threshold -> signal "Up".
    3. If DOWN price >= threshold -> signal "Down".
    4. If neither >= threshold -> return skip sentinel.
    5. Fetch slot N+1 token IDs for the signalled side.

    Returns a dict (signal or skip) or ``None`` on hard error.
    """
    slot_n = get_current_slot_info()
    slot_n1 = get_next_slot_info()

    log.debug(
        "Checking signal for slot N %s (%s-%s UTC)",
        slot_n["slug"],
        slot_n["slot_start_str"],
        slot_n["slot_end_str"],
    )

    prices = await get_slot_prices(slot_n["slug"])
    if prices is None:
        log.error("Could not fetch prices for slot N %s", slot_n["slug"])
        return None

    up_price = prices["up_price"]
    down_price = prices["down_price"]

    log.debug("Slot N prices  Up=%.4f  Down=%.4f  (threshold=%.2f)", up_price, down_price, cfg.SIGNAL_THRESHOLD)

    side: str | None = None
    entry_price: float | None = None
    opposite_price: float | None = None

    if up_price >= cfg.SIGNAL_THRESHOLD:
        side = "Up"
        entry_price = up_price
        opposite_price = down_price
    elif down_price >= cfg.SIGNAL_THRESHOLD:
        side = "Down"
        entry_price = down_price
        opposite_price = up_price

    # --- Build result --------------------------------------------------
    if side is None:
        # No signal — return a skip sentinel so the scheduler can log it
        return {
            "skipped": True,
            "up_price": up_price,
            "down_price": down_price,
            "slot_n1_start_full": slot_n1["slot_start_full"],
            "slot_n1_end_full": slot_n1["slot_end_full"],
            "slot_n1_start_str": slot_n1["slot_start_str"],
            "slot_n1_end_str": slot_n1["slot_end_str"],
            "slot_n1_ts": slot_n1["slot_start_ts"],
        }

    # We need N+1 token IDs for the signalled side
    n1_prices = await get_slot_prices(slot_n1["slug"])
    token_id: str | None = None
    if n1_prices:
        token_id = n1_prices["up_token_id"] if side == "Up" else n1_prices["down_token_id"]
    else:
        log.warning("Could not fetch N+1 prices for token_id — will use N token ID as fallback")
        token_id = prices["up_token_id"] if side == "Up" else prices["down_token_id"]

    log.info(
        "SIGNAL: %s @ $%.4f for slot N+1 %s-%s UTC  token=%s",
        side,
        entry_price,
        slot_n1["slot_start_str"],
        slot_n1["slot_end_str"],
        token_id,
    )

    return {
        "skipped": False,
        "side": side,
        "entry_price": entry_price,
        "opposite_price": opposite_price,
        "token_id": token_id,
        # Slot N (observation)
        "slot_n_start_full": slot_n["slot_start_full"],
        "slot_n_end_full": slot_n["slot_end_full"],
        "slot_n_start_str": slot_n["slot_start_str"],
        "slot_n_end_str": slot_n["slot_end_str"],
        # Slot N+1 (trade window)
        "slot_n1_start_full": slot_n1["slot_start_full"],
        "slot_n1_end_full": slot_n1["slot_end_full"],
        "slot_n1_start_str": slot_n1["slot_start_str"],
        "slot_n1_end_str": slot_n1["slot_end_str"],
        "slot_n1_ts": slot_n1["slot_start_ts"],
        "slot_n1_slug": slot_n1["slug"],
    }
