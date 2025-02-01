#!/usr/bin/env python3


from typing import Optional

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy import text as sqlalchemy_text

import common

# Auto-generated columns in SQLite.
TABLES_DROP_COLUMNS = {
    "history": ["total", "total_no_homes"],
}

TABLES_WIDE = {
    "forex",
    "history",
    "index_prices",
    "interactive_brokers_margin_rates",
    "schwab_etfs_amounts",
    "schwab_etfs_prices",
    "schwab_ira_amounts",
    "schwab_ira_prices",
    "swtsx_market_cap",
    "swvxx_yield",
    "swygx_holdings",
    "wealthfront_cash_yield",
}

TABLES_LONG_GROUPBY = {
    "brokerage_totals": ["Brokerage"],
    "real_estate_prices": ["name", "site"],
    "real_estate_rents": ["name", "site"],
}


def vacuum():
    logger.info("Vacuuming database")
    with create_engine(common.SQLITE_URI).connect() as conn:
        conn.execute(sqlalchemy_text("VACUUM"))


def rewrite_table(table: str, df: pd.DataFrame):
    with create_engine(common.SQLITE_URI).connect() as conn:
        conn.execute(sqlalchemy_text(f"DELETE FROM {table}"))
        df.to_sql(table, conn, if_exists="append", index_label="date")
        conn.commit()


def resample_table(table: str, drop_cols: Optional[list[str]]):
    """Downsample table from hourly to daily."""
    logger.info(f"Resampling table {table}")
    df = common.read_sql_table(table)
    if drop_cols:
        df = df.drop(columns=drop_cols)
    df = df.resample("D").last()
    rewrite_table(table, df)


def resample_long_table(table: str, groupby: list[str], drop_cols: Optional[list[str]]):
    """Resample a dataframe which is in long format."""
    logger.info(f"Resampling long table {table}")
    df = common.read_sql_table(table)
    if drop_cols:
        df = df.drop(columns=drop_cols)
    df = (
        df.groupby(groupby)
        .resample("D")
        .last()
        .reset_index(len(groupby))
        .set_index("date")
        .dropna()
    )
    rewrite_table(table, df)


def resample_all_tables():
    """Resample all tables from hourly to daily."""
    for table in TABLES_WIDE:
        resample_table(table, TABLES_DROP_COLUMNS.get(table))
    for table, groupby in TABLES_LONG_GROUPBY.items():
        resample_long_table(table, groupby, TABLES_DROP_COLUMNS.get(table))
    vacuum()


def main():
    resample_all_tables()


if __name__ == "__main__":
    main()
