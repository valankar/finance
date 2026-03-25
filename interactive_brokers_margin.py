#!/usr/bin/env python3
"""Store IBKR margin rate history for USD, CHF."""

from typing import Optional

from pydantic import BaseModel

import common


class GetMarginRateError(Exception):
    """Failed to get margin rate."""


class MarginRate(BaseModel):
    currencies: dict[str, float]


async def get_from_browser_use() -> Optional[dict[str, float]]:
    t = await common.run_browser_use(
        task="On https://www.interactivebrokers.com/en/trading/margin-rates.php, get IBKR Pro rate for USD and CHF. Just use the first tier for the currency.",
        model=MarginRate,
    )
    if o := t.output:
        return o.currencies


async def main():
    """Writes IB margin rates to DB."""
    if not (interest_rates := await get_from_browser_use()):
        raise GetMarginRateError
    common.insert_sql("interactive_brokers_margin_rates", interest_rates)
