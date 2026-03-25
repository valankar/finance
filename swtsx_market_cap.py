#!/usr/bin/env python3
"""Get market caps of SWTSX."""

from operator import itemgetter
from typing import Optional

from pydantic import BaseModel

import common


class MarketCap(BaseModel):
    market_cap: dict[str, float]


class MarketCapError(Exception):
    """Bad market cap values."""


async def get_from_browser_use() -> Optional[dict[str, float]]:
    t = await common.run_browser_use(
        task="Get the market cap table from https://www.schwabassetmanagement.com/products/swtsx under the portfolio section, storing the market cap and percent in the market_cap dictionary",
        model=MarketCap,
    )
    if o := t.output:
        return o.market_cap


async def save_market_cap():
    """Writes SWTSX market cap weightings to swtsx_market_cap DB table."""
    if not (market_cap := await get_from_browser_use()):
        raise MarketCapError
    if sum(market_cap.values()) < 90:
        raise MarketCapError(f"Sum is < 90: {market_cap}")
    sorted_cap = sorted(market_cap.items(), key=itemgetter(1), reverse=True)
    # US_LARGE_CAP = first 2 elements
    # US_SMALL_CAP = the rest
    d = {
        "US_LARGE_CAP": sum([x[1] for x in sorted_cap[0:2]]),
        "US_SMALL_CAP": sum([x[1] for x in sorted_cap[2:]]),
    }
    common.insert_sql("swtsx_market_cap", d)


async def main():
    """Main."""
    await save_market_cap()
