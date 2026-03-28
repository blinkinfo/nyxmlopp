"""APScheduler loop — syncs to 5-min slot boundaries, fires signals, trades, resolves."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config as cfg
from core import strategy, trader, resolver
from db import queries
from polymarket.markets import SLOT_DURATION, slot_info_from_ts

log = logging.getLogger(__name__)

SCHEDULER: AsyncIOScheduler | None = None

# Holds references so Telegram bot can send messages
_tg_app = None
_poly_client = None


def _next_check_time() -> datetime:
    """Calculate the next T-85s check time (slot_end - SIGNAL_LEAD_TIME).

    Slots align to :00, :05, :10 ... :55 of each hour.
    T-85s = slot_end - 85 seconds = slot_start + 300 - 85 = slot_start + 215 seconds.
    """
    now = datetime.now(timezone.utc)
    epoch = int(now.timestamp())
    current_slot_start = epoch - (epoch % SLOT_DURATION)
    check_epoch = current_slot_start + SLOT_DURATION - cfg.SIGNAL_LEAD_TIME

    if check_epoch <= epoch:
        # Already past this slot's check time — schedule for next slot
        check_epoch += SLOT_DURATION

    return datetime.fromtimestamp(check_epoch, tz=timezone.utc)


async def _send_telegram(text: str) -> None:
    """Send a message to the configured Telegram chat."""
    if _tg_app is None or cfg.TELEGRAM_CHAT_ID is None:
        return
    try:
        await _tg_app.bot.send_message(
            chat_id=int(cfg.TELEGRAM_CHAT_ID),
            text=text,
            parse_mode="HTML",
        )
    except Exception:
        log.exception("Failed to send Telegram message")


async def _resolve_and_notify(signal_id: int, slug: str, side: str, entry_price: float,
                               slot_start: str, slot_end: str, trade_id: int | None,
                               amount_usdc: float | None) -> None:
    """Poll for resolution, update DB, notify Telegram."""
    from bot.formatters import format_resolution

    winner = await resolver.resolve_slot(slug)
    if winner is None:
        log.warning("Could not resolve slot %s — leaving signal %d pending", slug, signal_id)
        await _send_telegram(f"\u26a0\ufe0f Could not resolve slot {slug} — manual check needed.")
        return

    is_win = winner == side
    await queries.resolve_signal(signal_id, winner, is_win)

    pnl: float | None = None
    if trade_id is not None and amount_usdc is not None:
        if is_win:
            pnl = round(amount_usdc * (1.0 / entry_price - 1.0), 4)
        else:
            pnl = -amount_usdc
        await queries.resolve_trade(trade_id, winner, is_win, pnl)

    # Extract HH:MM from slot_start/slot_end full strings
    s_start = slot_start.split(" ")[-1] if " " in slot_start else slot_start
    s_end = slot_end.split(" ")[-1] if " " in slot_end else slot_end

    msg = format_resolution(
        is_win=is_win,
        side=side,
        entry_price=entry_price,
        slot_start_str=s_start,
        slot_end_str=s_end,
        pnl=pnl,
    )
    await _send_telegram(msg)


async def _check_and_trade() -> None:
    """Core loop body — called at T-85s for each slot."""
    from bot.formatters import format_signal, format_skip

    # 1. Check signal
    signal = await strategy.check_signal()
    if signal is None:
        log.error("Strategy returned None (hard error) — skipping this slot")
        await _send_telegram("\u274c Strategy error — could not fetch prices. Skipping slot.")
        _schedule_next()
        return

    slot_start_full = signal["slot_n1_start_full"]
    slot_end_full = signal["slot_n1_end_full"]
    slot_start_str = signal["slot_n1_start_str"]
    slot_end_str = signal["slot_n1_end_str"]
    slot_ts = signal["slot_n1_ts"]

    # 2. Log signal to DB
    if signal["skipped"]:
        signal_id = await queries.insert_signal(
            slot_start=slot_start_full,
            slot_end=slot_end_full,
            slot_timestamp=slot_ts,
            side=None,
            entry_price=None,
            opposite_price=None,
            skipped=True,
        )
        msg = format_skip(slot_start_str=slot_start_str, slot_end_str=slot_end_str,
                          up_price=signal["up_price"], down_price=signal["down_price"])
        await _send_telegram(msg)
        _schedule_next()
        return

    side = signal["side"]
    entry_price = signal["entry_price"]
    opposite_price = signal["opposite_price"]
    token_id = signal["token_id"]
    slug = signal.get("slot_n1_slug", f"btc-updown-5m-{slot_ts}")

    signal_id = await queries.insert_signal(
        slot_start=slot_start_full,
        slot_end=slot_end_full,
        slot_timestamp=slot_ts,
        side=side,
        entry_price=entry_price,
        opposite_price=opposite_price,
        skipped=False,
    )

    # 3. Check autotrade
    autotrade = await queries.is_autotrade_enabled()
    trade_amount = await queries.get_trade_amount()

    # 4. Send signal notification
    msg = format_signal(
        side=side,
        entry_price=entry_price,
        slot_start_str=slot_start_str,
        slot_end_str=slot_end_str,
        autotrade=autotrade,
    )
    await _send_telegram(msg)

    # 5. Place trade if autotrade on
    trade_id: int | None = None
    amount_usdc: float | None = None
    if autotrade and _poly_client is not None and token_id:
        amount_usdc = round(trade_amount, 2)
        trade_id = await queries.insert_trade(
            signal_id=signal_id,
            slot_start=slot_start_full,
            slot_end=slot_end_full,
            side=side,
            entry_price=entry_price,
            amount_usdc=amount_usdc,
            status="pending",
        )
        try:
            response = await trader.place_fok_order(_poly_client, token_id, amount_usdc)
            order_id = None
            if isinstance(response, dict):
                order_id = response.get("orderID") or response.get("order_id")
            await queries.update_trade_status(trade_id, "filled", order_id=order_id)
            log.info("Trade filled: order_id=%s", order_id)
        except Exception:
            log.exception("FOK order failed")
            await queries.update_trade_status(trade_id, "failed")
            await _send_telegram(f"\u274c Trade FAILED for {side} slot {slot_start_str}-{slot_end_str} UTC")
            trade_id = None  # don't resolve a failed trade

    # 6. Schedule resolution after slot N+1 ends
    resolve_time = datetime.fromtimestamp(slot_ts + SLOT_DURATION + 15, tz=timezone.utc)
    if SCHEDULER is not None:
        SCHEDULER.add_job(
            _resolve_and_notify,
            trigger="date",
            run_date=resolve_time,
            kwargs={
                "signal_id": signal_id,
                "slug": slug,
                "side": side,
                "entry_price": entry_price,
                "slot_start": slot_start_full,
                "slot_end": slot_end_full,
                "trade_id": trade_id,
                "amount_usdc": amount_usdc,
            },
            id=f"resolve_{signal_id}",
            replace_existing=True,
        )
        log.debug("Scheduled resolution for signal %d at %s", signal_id, resolve_time.isoformat())

    # 7. Schedule next check
    _schedule_next()


def _schedule_next() -> None:
    """Add the next check_and_trade job to the scheduler."""
    if SCHEDULER is None:
        return
    next_time = _next_check_time()
    SCHEDULER.add_job(
        _check_and_trade,
        trigger="date",
        run_date=next_time,
        id="check_and_trade",
        replace_existing=True,
    )
    log.info("Next check: %s UTC", next_time.strftime("%H:%M:%S"))


async def recover_unresolved() -> None:
    """On startup, schedule resolution for any unresolved signals/trades."""
    signals = await queries.get_unresolved_signals()
    if not signals:
        log.debug("No unresolved signals to recover.")
        return

    log.info("Recovering %d unresolved signal(s)...", len(signals))
    for sig in signals:
        slug = f"btc-updown-5m-{sig['slot_timestamp']}"
        trade = await queries.get_trade_by_signal(sig["id"])
        trade_id = trade["id"] if trade else None
        amount_usdc = trade["amount_usdc"] if trade else None

        # Schedule immediate resolution (past slots should already be resolved)
        resolve_time = datetime.now(timezone.utc) + timedelta(seconds=5)
        if SCHEDULER is not None:
            SCHEDULER.add_job(
                _resolve_and_notify,
                trigger="date",
                run_date=resolve_time,
                kwargs={
                    "signal_id": sig["id"],
                    "slug": slug,
                    "side": sig["side"],
                    "entry_price": sig["entry_price"],
                    "slot_start": sig["slot_start"],
                    "slot_end": sig["slot_end"],
                    "trade_id": trade_id,
                    "amount_usdc": amount_usdc,
                },
                id=f"recover_{sig['id']}",
                replace_existing=True,
            )


def start_scheduler(tg_app, poly_client) -> AsyncIOScheduler:
    """Create, configure, and start the scheduler."""
    global SCHEDULER, _tg_app, _poly_client
    _tg_app = tg_app
    _poly_client = poly_client

    SCHEDULER = AsyncIOScheduler(timezone="UTC")
    SCHEDULER.start()

    # Schedule first check
    _schedule_next()

    log.info("Scheduler started.")
    return SCHEDULER
