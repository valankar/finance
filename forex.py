#!/usr/bin/env python3
"""Write forex history."""

import pandas as pd

import common


def main():
    """Get and update forex data."""
    forex_df_data = {
        "CHFUSD": common.get_ticker("CHFUSD=X"),
        "SGDUSD": common.get_ticker("SGDUSD=X"),
    }
    forex_df = pd.DataFrame(
        forex_df_data, index=[pd.Timestamp.now()], columns=forex_df_data.keys()
    )
    common.to_sql(forex_df, "forex")


if __name__ == "__main__":
    main()
