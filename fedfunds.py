#!/usr/bin/env python3
"""Download fedfunds data."""

import pandas as pd
from fredapi import Fred

from sqlalchemy import create_engine

import authorization
import common


def get_fred(fred_api, series, output_table):
    """Download series from fred api."""
    fedfunds_df = pd.DataFrame({"percent": fred_api.get_series(series)}).rename_axis(
        "date"
    )
    with create_engine(common.SQLITE_URI).connect() as conn:
        fedfunds_df.to_sql(output_table, conn, if_exists="replace", index_label="date")
        conn.commit()


def main():
    """Download fedfunds rate data."""
    fred = Fred(api_key=authorization.FREDAPI_KEY)
    get_fred(fred, "FEDFUNDS", "fedfunds")
    get_fred(fred, "SOFR", "sofr")


if __name__ == "__main__":
    main()
