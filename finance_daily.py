#!/usr/bin/env python3
"""Run daily finance functions."""

import argparse
import sys
from functools import partial
from typing import Callable, NamedTuple

import portalocker
from loguru import logger

import common
import fedfunds
import homes
import interactive_brokers_margin
import resample_table
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
        DailyMethod(name="Resample Tables", method=resample_table.resample_all_tables),
    ]


@common.cache_daily_decorator
def run_method(name: str):
    for method in make_daily_methods():
        if method.name == name:
            logger.info(f"Running {name}")
            return method.method()
    logger.error(f"No method with name {name} found")


def methods_run_needed() -> bool:
    needs_run = []
    for method in make_daily_methods():
        # This is fixed in https://github.com/joblib/joblib/pull/1584, but not yet released.
        call_id = (run_method.func_id, run_method._get_args_id(method.name))
        if not run_method._is_in_cache_and_valid(call_id):
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
    with portalocker.Lock(common.LOCKFILE, timeout=common.LOCKFILE_TIMEOUT):
        failed_methods = []
        for method in make_daily_methods():
            try:
                run_method(method.name)
            except Exception:
                logger.exception("Failed")
                failed_methods.append(method)
        if failed_methods:
            for m in failed_methods:
                logger.error(f"Failed method {m.name}")
            raise MethodFailed()


if __name__ == "__main__":
    main()
