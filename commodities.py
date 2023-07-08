#!/usr/bin/env python3
"""Calculate commodity values."""

import common

TABLE_PREFIX = "commodities"
CSV_OUTPUT_PATH = common.PREFIX + "commodities_values.csv"


def main():
    """Main."""
    common.write_ticker_csv(
        f"{TABLE_PREFIX}_amounts",
        f"{TABLE_PREFIX}_prices",
        CSV_OUTPUT_PATH,
        ticker_col_name="commodity",
        ticker_amt_col="troy_oz",
        ticker_aliases={"GOLD": "GC=F", "SILVER": "SI=F"},
    )


if __name__ == "__main__":
    main()
