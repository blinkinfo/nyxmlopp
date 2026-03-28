"""FOK market-order execution via py-clob-client (synchronous — wrapped for async)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

log = logging.getLogger(__name__)


async def place_fok_order(
    poly_client,
    token_id: str,
    amount_usdc: float,
) -> dict[str, Any]:
    """Place a Fill-Or-Kill market buy order.

    *amount_usdc* is rounded to 2 decimal places to work around
    py-clob-client issue #121 (precision error on fractional amounts).

    The py-clob-client is synchronous, so both steps run inside
    ``asyncio.to_thread`` to keep the event loop responsive.
    """
    amount = round(amount_usdc, 2)
    log.info("Placing FOK order: token=%s  amount=$%.2f", token_id, amount)

    order_args = MarketOrderArgs(
        token_id=token_id,
        amount=amount,
        side=BUY,
        order_type=OrderType.FOK,
    )

    # Step 1 — sign locally
    signed = await asyncio.to_thread(
        poly_client.client.create_market_order, order_args
    )

    # Step 2 — post to CLOB
    response = await asyncio.to_thread(
        poly_client.client.post_order, signed, OrderType.FOK
    )

    log.debug("Order response: %s", response)
    return response
