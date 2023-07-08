#!/usr/bin/env python3
"""Calculate ETF values."""

import common

TABLE_PREFIX = "schwab_etfs"
CSV_OUTPUT_PATH = f"{common.PREFIX}schwab_etfs_values.csv"


def main():
    """Main."""
    common.write_ticker_csv(
        f"{TABLE_PREFIX}_amounts", f"{TABLE_PREFIX}_prices", CSV_OUTPUT_PATH
    )


if __name__ == "__main__":
    main()
