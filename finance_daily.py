#!/usr/bin/env python3
"""Run daily finance functions."""

import inspect
from enum import Enum, auto
from typing import Callable, NamedTuple

from loguru import logger

import common
import fedfunds
import homes
import interactive_brokers_margin
import swtsx_market_cap
import swvxx_yield
import swygx_holdings


class Period(Enum):
    DAILY = auto()
    WEEKLY = auto()


class PeriodicMethod(NamedTuple):
    name: str
    method: Callable
    frequency: Period


class MethodFailed(Exception):
    """One of the methods failed with exception."""


METHODS: list[PeriodicMethod] = [
    PeriodicMethod(name="Fedfunds", method=fedfunds.main, frequency=Period.DAILY),
    PeriodicMethod(
        name="Interactive Brokers Margin",
        method=interactive_brokers_margin.main,
        frequency=Period.WEEKLY,
    ),
    PeriodicMethod(
        name="SWTSX Market Cap",
        method=swtsx_market_cap.main,
        frequency=Period.WEEKLY,
    ),
    PeriodicMethod(
        name="SWVXX Yield", method=swvxx_yield.main, frequency=Period.WEEKLY
    ),
    PeriodicMethod(
        name="SWYGX Holdings", method=swygx_holdings.main, frequency=Period.WEEKLY
    ),
    PeriodicMethod(
        name="Compact DuckDB", method=common.compact_db, frequency=Period.DAILY
    ),
    PeriodicMethod(
        name="Real Estate Prices", method=homes.main, frequency=Period.WEEKLY
    ),
]


async def run_all():
    failed_methods = []
    daily_cache = common.walrus_db.db.cache(
        "Daily Methods", default_timeout=24 * 60 * 60
    )
    weekly_cache = common.walrus_db.db.cache(
        "Weekly Methods", default_timeout=7 * 24 * 60 * 60
    )
    for method in METHODS:
        match method.frequency:
            case Period.DAILY:
                cache = daily_cache
            case Period.WEEKLY:
                cache = weekly_cache
        if cache.get(method.name):
            continue
        try:
            logger.info(f"Running {method.name}")
            r = method.method()
            if inspect.isawaitable(r):
                await r
            cache.set(method.name, True)
        except Exception:
            logger.exception(f"{method.name} failed")
            failed_methods.append(method)
    if failed_methods:
        for m in failed_methods:
            logger.error(f"Failed method {m.name}")
        raise MethodFailed()
