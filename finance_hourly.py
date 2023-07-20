#!/usr/bin/env python3
"""Run hourly finance functions."""

import commodities
import common
import etfs
import history
import plot
import schwab_ira


def main():
    """Main."""
    for func in [
        commodities.main,
        etfs.main,
        schwab_ira.main,
        history.main,
        plot.main,
    ]:
        common.run_and_save_performance(func)


if __name__ == "__main__":
    main()
