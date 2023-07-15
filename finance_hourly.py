#!/usr/bin/env python3
"""Run hourly finance functions."""

import commodities
import common
import etfs
import history
import plot


def main():
    """Main."""
    funcs = [
        commodities.main,
        etfs.main,
        history.main,
        plot.main,
    ]
    common.run_and_save_performance(funcs, "performance_hourly")


if __name__ == "__main__":
    main()
