#!/usr/bin/env python3
"""Write brokerage balance history to database."""

import pandas as pd
from loguru import logger

import common
import margin_loan

TABLE_NAME = "brokerage_totals"


def load_df() -> pd.DataFrame:
    """Load the total brokerage dataframe."""
    return common.read_sql_table(TABLE_NAME).pivot(columns="Brokerage")


def main():
    dfs = []
    now = pd.Timestamp.now()
    for brokerage in margin_loan.LOAN_BROKERAGES:
        if (df := margin_loan.get_balances_broker(brokerage)) is not None:
            df["Brokerage"] = brokerage.name
            df["date"] = now
            df = df.set_index("date")
            dfs.append(df)
    if not dfs:
        logger.error("No brokerage data found.")
        return
    common.to_sql(pd.concat(dfs), TABLE_NAME)


if __name__ == "__main__":
    main()
