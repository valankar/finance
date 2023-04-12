#!/usr/bin/env python3
"""Run daily finance functions."""

import homes
import fedfunds
import swvxx_yield
import wealthfront_cash_yield


def main():
    """Main."""
    homes.main()
    fedfunds.main()
    swvxx_yield.main()
    wealthfront_cash_yield.main()


if __name__ == '__main__':
    main()
