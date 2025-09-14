#!/usr/bin/env python3
"""Run daily finance functions."""

from typing import Callable, NamedTuple

from loguru import logger

import common
import fedfunds
import interactive_brokers_margin
import swtsx_market_cap
import swvxx_yield
import swygx_holdings


class DailyMethod(NamedTuple):
    name: str
    method: Callable


class MethodFailed(Exception):
    """One of the methods failed with exception."""


def make_daily_methods() -> list[DailyMethod]:
    return [
        DailyMethod(name="Fedfunds", method=fedfunds.main),
        DailyMethod(
            name="Interactive Brokers Margin", method=interactive_brokers_margin.main
        ),
        DailyMethod(name="SWTSX Market Cap", method=swtsx_market_cap.main),
        DailyMethod(name="SWVXX Yield", method=swvxx_yield.main),
        DailyMethod(name="SWYGX Holdings", method=swygx_holdings.main),
        DailyMethod(name="Compact DuckDB", method=common.compact_db),
    ]


def run_all():
    failed_methods = []
    cache = common.walrus_db.db.cache("Daily Methods", default_timeout=24 * 60 * 60)
    for method in make_daily_methods():
        if cache.get(method.name):
            continue
        try:
            logger.info(f"Running {method.name}")
            method.method()
            cache.set(method.name, True)
        except Exception:
            logger.exception("Failed")
            failed_methods.append(method)
    if failed_methods:
        for m in failed_methods:
            logger.error(f"Failed method {m.name}")
        raise MethodFailed()
