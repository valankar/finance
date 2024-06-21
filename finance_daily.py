#!/usr/bin/env python3
"""Run daily finance functions."""

import traceback

import fedfunds
import interactive_brokers_margin
import swtsx_market_cap
import swvxx_yield
import swygx_holdings
import wealthfront_cash_yield


class OneMethodFailed(Exception):
    """One of the methods failed with exception."""


def main():
    """Main."""
    exceptions = False
    for method in [
        fedfunds.main,
        interactive_brokers_margin.main,
        swtsx_market_cap.main,
        swvxx_yield.main,
        swygx_holdings.main,
        wealthfront_cash_yield.main,
    ]:
        try:
            method()
        # pylint: disable-next=broad-exception-caught
        except Exception:
            traceback.print_exc()
            exceptions = True
    if exceptions:
        raise OneMethodFailed()


if __name__ == "__main__":
    main()
