#!/usr/bin/env python3
"""Store Schwab SWVXX 7-day yield history."""

from typing import Optional

from pydantic import BaseModel

import common


class GetYieldError(Exception):
    """Error getting yield."""


class Yield(BaseModel):
    percent: float


async def get_from_browser_use() -> Optional[float]:
    t = await common.run_browser_use(
        task="On https://www.schwabassetmanagement.com/products/swvxx get the 7-Day Yield (with waivers) percent value",
        model=Yield,
    )
    if o := t.output:
        return o.percent


async def main():
    """Writes 7 day yield history to CSV file."""
    if not (y := await get_from_browser_use()):
        raise GetYieldError("Unable to get yield from browser-use")
    common.insert_sql("swvxx_yield", {"percent": y})
