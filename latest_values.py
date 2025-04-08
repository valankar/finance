#!/usr/bin/env python3

from datetime import datetime, timedelta

import pandas as pd

import common


def main():
    history = common.read_sql_table("history")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    df = pd.concat([history.loc[yesterday].iloc[-1:], history.iloc[-1:]])
    print(df)
    print("\nDifference:")
    print(df.diff().iloc[-1:])


if __name__ == "__main__":
    main()
