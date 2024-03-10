#!/usr/bin/env python3
"""Run daily finance functions."""

import fedfunds
import swtsx_market_cap
import swvxx_yield
import swygx_holdings
import wealthfront_cash_yield


def main():
    """Main."""
    fedfunds.main()
    swtsx_market_cap.main()
    swvxx_yield.main()
    swygx_holdings.main()
    wealthfront_cash_yield.main()


if __name__ == "__main__":
    main()
