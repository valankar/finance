#!/usr/bin/env python3
"""Store holdings of SWYGX."""

from typing import Optional

from loguru import logger
from pydantic import BaseModel

import common


class GetHoldingsError(Exception):
    """Error getting holdings."""


class Holdings(BaseModel):
    symbols: dict[str, float]


async def get_from_browser_use() -> Optional[dict[str, float]]:
    t = await common.run_browser_use(
        task="Get the table from https://www.schwabassetmanagement.com/allholdings/SWYGX, storing the symbol and percent in the symbols dictionary",
        model=Holdings,
    )
    if o := t.output:
        return o.symbols


async def save_holdings():
    """Writes SWYGX holdings to swygx_holdings DB table."""
    if not (holdings := await get_from_browser_use()):
        raise GetHoldingsError("Unable to get holdings from browser-use")
    expected_tickers = set(common.read_sql_last("swygx_holdings").columns)
    found_tickers = set(holdings)
    if found_tickers != expected_tickers:
        logger.error(f"Failed: {expected_tickers=} {found_tickers=} {holdings=}")
        logger.error(
            f"Symmetric difference: {expected_tickers.symmetric_difference(found_tickers)}"
        )
        raise GetHoldingsError
    common.insert_sql("swygx_holdings", holdings)


async def main():
    """Main."""
    await save_holdings()
