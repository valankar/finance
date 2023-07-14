#!/usr/bin/env python3
"""Get Schwab IRA value."""

import common

TABLE_PREFIX = "schwab_ira"
CSV_OUTPUT_PATH = f"{common.PREFIX}schwab_ira_values.csv"


def main():
    """Main."""
    common.write_ticker_csv(
        f"{TABLE_PREFIX}_amounts",
        f"{TABLE_PREFIX}_prices",
        CSV_OUTPUT_PATH,
    )


if __name__ == "__main__":
    main()
