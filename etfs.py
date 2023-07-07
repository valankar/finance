#!/usr/bin/env python3
"""Calculate ETF values."""

import common

ETFS_PATH = common.PREFIX + "etfs_amounts.csv"
OUTPUT_PATH = common.PREFIX + "etfs_values.csv"


def main():
    """Main."""
    common.write_ticker_csv(ETFS_PATH, OUTPUT_PATH)


if __name__ == "__main__":
    main()
