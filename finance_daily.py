#!/usr/bin/env python3
"""Run daily finance functions."""

import argparse
import sys
from functools import partial
from typing import Callable, NamedTuple

from loguru import logger

import common
import fedfunds
import homes
import interactive_brokers_margin
import swtsx_market_cap
import swvxx_yield
import swygx_holdings
import wealthfront_cash_yield


class DailyMethod(NamedTuple):
    name: str
    method: Callable


class MethodFailed(Exception):
    """One of the methods failed with exception."""


def make_property_daily_methods() -> list[DailyMethod]:
    methods = []
    for p in homes.PROPERTIES:
        methods.append(
            DailyMethod(
                name=f"Real Estate Redfin: {p.name}",
                method=partial(homes.process_redfin, p),
            )
        )
    return methods


def make_daily_methods() -> list[DailyMethod]:
    return [
        DailyMethod(name="Fedfunds", method=fedfunds.main),
        DailyMethod(
            name="Interactive Brokers Margin", method=interactive_brokers_margin.main
        ),
        *make_property_daily_methods(),
        DailyMethod(name="SWTSX Market Cap", method=swtsx_market_cap.main),
        DailyMethod(name="SWVXX Yield", method=swvxx_yield.main),
        DailyMethod(name="SWYGX Holdings", method=swygx_holdings.main),
        DailyMethod(name="Wealthfront Cash Yield", method=wealthfront_cash_yield.main),
        DailyMethod(name="Compact DuckDB", method=common.compact_db),
    ]


def methods_run_needed() -> bool:
    needs_run = []
    for method in make_daily_methods():
        if not common.WalrusDb().cache.get(method.name):
            logger.info(f"Method {method.name} needs to run")
            needs_run.append(method)
    return bool(needs_run)


def main():
    """Main."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--methods-run-needed", default=False, action=argparse.BooleanOptionalAction
    )
    args = parser.parse_args()
    if args.methods_run_needed:
        if methods_run_needed():
            sys.exit(1)
        else:
            sys.exit()
    with common.WalrusDb().db.lock(
        common.SCRIPT_LOCK_NAME, ttl=common.LOCK_TTL_SECONDS * 1000
    ):
        failed_methods = []
        for method in make_daily_methods():
            if common.WalrusDb().cache.get(method.name):
                logger.info(f"Method {method.name} ran recently")
                continue
            try:
                logger.info(f"Running {method.name}")
                method.method()
                common.WalrusDb().cache.set(method.name, True, timeout=24 * 60 * 60)
            except Exception:
                logger.exception("Failed")
                failed_methods.append(method)
        if failed_methods:
            for m in failed_methods:
                logger.error(f"Failed method {m.name}")
            raise MethodFailed()


if __name__ == "__main__":
    main()
