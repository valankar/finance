#!/usr/bin/env python3

from cyclopts import App

import common

app = App()


@app.default
def main(
    ticker: str,
):
    """Add new index.

    Parameters
    ----------
    ticker: str
        Ticker name. For example: ^SSMI
    """
    tables = ["index_prices"]
    with common.duckdb_lock() as con:
        for table in tables:
            con.execute(f'ALTER TABLE {table} ADD COLUMN "{ticker}" DOUBLE')


if __name__ == "__main__":
    app()
