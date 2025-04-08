#!/usr/bin/env python3

import duckdb
from cyclopts import App

import common

app = App()


@app.default
def main(
    ticker: str,
    is_index: bool = False,
):
    """Add new ticker/index.

    Parameters
    ----------
    ticker: str
        Ticker name. For example: IBKR, ^SSMI
    is_index: bool
        Whether the ticker is an index. Uses a different table if so.
    """
    if is_index:
        tables = ["index_prices"]
    else:
        tables = ["schwab_etfs_prices", "schwab_etfs_amounts"]
    with duckdb.connect(common.DUCKDB) as con:
        for table in tables:
            con.execute(f'ALTER TABLE {table} ADD COLUMN "{ticker}" DOUBLE')


if __name__ == "__main__":
    app()
