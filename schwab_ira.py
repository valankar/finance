#!/usr/bin/env python3
"""Get Schwab IRA value."""

import common

SCHWAB_IRA_AMOUNT_PATH = common.PREFIX + "schwab_ira_amounts.csv"
OUTPUT_PATH = common.PREFIX + "schwab_ira_values.csv"


def main():
    """Main."""
    common.write_ticker_csv(SCHWAB_IRA_AMOUNT_PATH, OUTPUT_PATH)


if __name__ == "__main__":
    main()
