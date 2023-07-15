#!/usr/bin/env python3
"""Write finance history."""

import pandas as pd
from sqlalchemy import create_engine

import common


ACCOUNT_MAP = {
    "ally": "USD_Ally",
    "apple_card": "USD_Apple_Card",
    "apple_cash": "USD_Apple_Cash",
    "bofa_cash_rewards_visa": "USD_Bank of America_Cash Rewards Visa",
    "bofa_travel_rewards_visa": "USD_Bank of America_Travel Rewards Visa",
    "bofa": "USD_Bank of America_Checking",
    "cash_chf": "CHF_Cash",
    "treasury_direct": "USD_Treasury Direct",
    "mtvernon": "USD_Real Estate_Mt Vernon",
    "northlake": "USD_Real Estate_Northlake",
    "villamaria": "USD_Real Estate_Villa Maria",
    "SCHA": "USD_Charles Schwab_Brokerage_SCHA",
    "SCHF": "USD_Charles Schwab_Brokerage_SCHF",
    "SCHR": "USD_Charles Schwab_Brokerage_SCHR",
    "SCHX": "USD_Charles Schwab_Brokerage_SCHX",
    "SWYGX": "USD_Charles Schwab_IRA_SWYGX",
    "schwab_brokerage_cash": "USD_Charles Schwab_Brokerage_Cash",
    "schwab_ira_cash": "USD_Charles Schwab_IRA_Cash",
    "schwab_checking": "USD_Charles Schwab_Checking",
    "schwab_pledged_asset_line": "USD_Charles Schwab_Pledged Asset Line",
    "ubs_pillar2": "CHF_UBS_Pillar 2",
    "ubs_visa": "CHF_UBS_Visa",
    "ubs": "CHF_UBS_Primary",
    "wealthfront_cash": "USD_Wealthfront_Cash",
    "wise_chf": "CHF_Wise",
    "wise_sgd": "SGD_Wise",
    "wise_usd": "USD_Wise",
    "SILVER": "USD_Commodities_Silver",
    "GOLD": "USD_Commodities_Gold",
    "zurcher": "CHF_Zurcher",
}


def load_csv_sum_and_update(filename, index_col, accounts_df_data):
    """Sum ticker/commodity data from csv file."""
    dataframe = pd.read_csv(filename, index_col=index_col)
    for key, value in dataframe["value"].to_dict().items():
        accounts_df_data[ACCOUNT_MAP[key]] = value
    return float(dataframe["value"].sum())


def main():
    """Main."""
    now = pd.Timestamp.now()
    exchange_rates = common.get_tickers(["CHFUSD=X", "SGDUSD=X"])
    chfusd = exchange_rates["CHFUSD=X"]
    sgdusd = exchange_rates["SGDUSD=X"]
    accounts_df_data = {}

    # Commodities
    commodities = load_csv_sum_and_update(
        f"{common.PREFIX}commodities_values.csv", "commodity", accounts_df_data
    )
    # ETFs
    etfs = load_csv_sum_and_update(
        f"{common.PREFIX}schwab_etfs_values.csv", "ticker", accounts_df_data
    )
    treasury_direct = common.load_float_from_text_file(
        f"{common.PREFIX}treasury_direct.txt"
    )
    accounts_df_data[ACCOUNT_MAP["treasury_direct"]] = treasury_direct

    total_investing = commodities + etfs + treasury_direct

    total_liquid = 0.0
    for chf_account in ["ubs", "ubs_visa", "cash_chf", "wise_chf"]:
        value = common.load_float_from_text_file(f"{common.PREFIX}{chf_account}.txt")
        total_liquid += value * chfusd
        accounts_df_data[ACCOUNT_MAP[chf_account]] = value

    value = common.load_float_from_text_file(f"{common.PREFIX}wise_sgd.txt")
    total_liquid += value * sgdusd
    accounts_df_data[ACCOUNT_MAP["wise_sgd"]] = value

    for usd_account in [
        "wise_usd",
        "ally",
        "apple_card",
        "apple_cash",
        "bofa",
        "bofa_cash_rewards_visa",
        "bofa_travel_rewards_visa",
        "schwab_brokerage_cash",
        "schwab_checking",
        "schwab_pledged_asset_line",
        "wealthfront_cash",
    ]:
        value = common.load_float_from_text_file(f"{common.PREFIX}{usd_account}.txt")
        total_liquid += value
        accounts_df_data[ACCOUNT_MAP[usd_account]] = value

    total_real_estate = 0.0
    for estate in ["mtvernon", "northlake", "villamaria"]:
        value = common.load_float_from_text_file(f"{common.PREFIX}{estate}.txt")
        total_real_estate += value
        accounts_df_data[ACCOUNT_MAP[estate]] = value

    # Retirement
    schwab_ira = load_csv_sum_and_update(
        f"{common.PREFIX}schwab_ira_values.csv", "ticker", accounts_df_data
    )
    schwab_ira_cash = common.load_float_from_text_file(
        f"{common.PREFIX}schwab_ira_cash.txt"
    )
    schwab_ira += schwab_ira_cash
    accounts_df_data[ACCOUNT_MAP["schwab_ira_cash"]] = schwab_ira_cash

    value = common.load_float_from_text_file(f"{common.PREFIX}ubs_pillar2.txt")
    pillar2 = value * chfusd
    accounts_df_data[ACCOUNT_MAP["ubs_pillar2"]] = value

    value = common.load_float_from_text_file(f"{common.PREFIX}zurcher.txt")
    zurcher = value * chfusd
    accounts_df_data[ACCOUNT_MAP["zurcher"]] = value

    total_retirement = pillar2 + zurcher + schwab_ira

    history_df_data = {
        "total_real_estate": total_real_estate,
        "total_liquid": total_liquid,
        "total_investing": total_investing,
        "total_retirement": total_retirement,
        "etfs": etfs,
        "commodities": commodities,
        "ira": schwab_ira,
        "pillar2": pillar2,
    }
    forex_df_data = {
        "CHFUSD": chfusd,
        "SGDUSD": sgdusd,
    }
    history_df = pd.DataFrame(
        history_df_data,
        index=[now],
        columns=history_df_data.keys(),
    )
    accounts_df = pd.DataFrame(
        accounts_df_data,
        index=[now],
        columns=accounts_df_data.keys(),
    )
    forex_df = pd.DataFrame(forex_df_data, index=[now], columns=forex_df_data.keys())
    with create_engine(common.SQLITE_URI).connect() as conn:
        forex_df.to_sql("forex", conn, if_exists="append", index_label="date")
        history_df.to_sql("history", conn, if_exists="append", index_label="date")
        accounts_df.to_sql(
            "account_history", conn, if_exists="append", index_label="date"
        )
        conn.commit()


if __name__ == "__main__":
    main()
