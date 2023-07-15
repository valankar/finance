#!/usr/bin/env python3
"""Run daily finance functions."""

import common
import fedfunds
import homes
import i_and_e
import schwab_ira
import swvxx_yield
import wealthfront_cash_yield


def main():
    """Main."""
    funcs = [
        homes.main,
        fedfunds.main,
        schwab_ira.main,
        swvxx_yield.main,
        wealthfront_cash_yield.main,
        i_and_e.main,
    ]
    common.run_and_save_performance(funcs, "performance_daily")


if __name__ == "__main__":
    main()
