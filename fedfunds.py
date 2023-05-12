#!/usr/bin/env python3
"""Download fedfunds data."""

import pandas as pd
from fredapi import Fred

import authorization
import common


def get_fred(fred_api, series, output_file):
    """Download series from fred api."""
    fedfunds_df = pd.DataFrame({"percent": fred_api.get_series(series)})
    fedfunds_df.index.name = "date"
    fedfunds_df.to_csv(f"{common.PREFIX}{output_file}")


def main():
    """Download fedfunds rate data."""
    fred = Fred(api_key=authorization.FREDAPI_KEY)
    get_fred(fred, "FEDFUNDS", "fedfunds.csv")
    get_fred(fred, "SOFR", "sofr.csv")


if __name__ == "__main__":
    main()
