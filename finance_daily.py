#!/usr/bin/env python3
"""Run daily finance functions."""

import common
import fedfunds
import homes
import i_and_e
import swvxx_yield
import wealthfront_cash_yield


def main():
    """Main."""
    for func in [
        homes.main,
        fedfunds.main,
        swvxx_yield.main,
        wealthfront_cash_yield.main,
        i_and_e.main,
    ]:
        common.run_and_save_performance(func)


if __name__ == "__main__":
    main()
