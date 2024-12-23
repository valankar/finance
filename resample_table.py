#!/usr/bin/env python3

import argparse

from sqlalchemy import create_engine
from sqlalchemy import text as sqlalchemy_text

import common


def vacuum():
    with create_engine(common.SQLITE_URI).connect() as conn:
        conn.execute(sqlalchemy_text("VACUUM"))


def resample_table(table: str, drop_cols: list[str] | None = None):
    """Downsample table from hourly to daily."""
    df = common.read_sql_table(table)
    df = df.resample("D").last()
    if drop_cols:
        df = df.drop(columns=drop_cols)
    with create_engine(common.SQLITE_URI).connect() as conn:
        conn.execute(sqlalchemy_text(f"DELETE FROM {table}"))
        df.to_sql(table, conn, if_exists="append", index_label="date")
        conn.commit()


def resample_all_tables():
    """Resample all tables from hourly to daily."""
    tables_col_drop = {
        "history": ["total", "total_no_homes"],
        "forex": None,
        "schwab_etfs_amounts": None,
        "schwab_etfs_prices": None,
        "schwab_ira_amounts": None,
        "schwab_ira_prices": None,
        "index_prices": None,
    }
    for table, drop_cols in tables_col_drop.items():
        resample_table(table, drop_cols)
    vacuum()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table", help="Table to resample hourly to daily.", required=True
    )
    parser.add_argument(
        "--drop-columns",
        help="List of columns to drop before writing dataframe.",
        nargs="+",
        type=str,
        required=False,
    )
    args = parser.parse_args()
    if args.table == "all":
        resample_all_tables()
    else:
        resample_table(args.table, args.drop_columns)
        vacuum()


if __name__ == "__main__":
    main()
