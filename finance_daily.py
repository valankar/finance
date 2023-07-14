#!/usr/bin/env python3
"""Run daily finance functions."""

import homes
import fedfunds
import i_and_e
import schwab_ira
import swvxx_yield
import wealthfront_cash_yield


def main():
    """Main."""
    homes.main()
    fedfunds.main()
    schwab_ira.main()
    swvxx_yield.main()
    wealthfront_cash_yield.main()
    i_and_e.main()


if __name__ == "__main__":
    main()
