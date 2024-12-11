#!/usr/bin/env python3
"""Run daily finance functions."""

import portalocker
from loguru import logger

import common
import fedfunds
import homes
import interactive_brokers_margin
import swtsx_market_cap
import swvxx_yield
import swygx_holdings
import wealthfront_cash_yield


class OneMethodFailed(Exception):
    """One of the methods failed with exception."""


def main():
    """Main."""
    with portalocker.Lock(common.LOCKFILE, timeout=common.LOCKFILE_TIMEOUT):
        exceptions = False
        for method in [
            fedfunds.main,
            interactive_brokers_margin.main,
            homes.main,
            swtsx_market_cap.main,
            swvxx_yield.main,
            swygx_holdings.main,
            wealthfront_cash_yield.main,
        ]:
            logger.info(f"Running {method.__module__}.{method.__name__}")
            try:
                method()
            except Exception:
                logger.exception("Failed")
                exceptions = True
        if exceptions:
            raise OneMethodFailed()


if __name__ == "__main__":
    main()
