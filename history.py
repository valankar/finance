#!/usr/bin/env python3
"""Write finance history."""

import csv
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

import common


def main():
    """Main."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exchange_rates = common.get_tickers(["CHFUSD=X", "SGDUSD=X"])
    chfusd = exchange_rates["CHFUSD=X"]
    sgdusd = exchange_rates["SGDUSD=X"]

    account_map = {
        "ally": "USD_Ally",
        "apple_card": "USD_Apple_Card",
        "apple_cash": "USD_Apple_Cash",
        "bofa_cash_rewards_visa": "USD_Bank of America_Cash Rewards Visa",
        "bofa_travel_rewards_visa": "USD_Bank of America_Travel Rewards Visa",
        "bofa": "USD_Bank of America_Checking",
        "cash_chf": "CHF_Cash",
        "healthequity": "USD_Healthequity HSA",
        "treasury_direct": "USD_Treasury Direct",
        "californiast": "USD_Real Estate_California St",
        "corallake": "USD_Real Estate_Coral Lake",
        "mtvernon": "USD_Real Estate_Mt Vernon",
        "northlake": "USD_Real Estate_Northlake",
        "villamaria": "USD_Real Estate_Villa Maria",
        "SCHA": "USD_Charles Schwab_Brokerage_SCHA",
        "SCHB": "USD_Charles Schwab_Brokerage_SCHB",
        "SCHE": "USD_Charles Schwab_Brokerage_SCHE",
        "SCHF": "USD_Charles Schwab_Brokerage_SCHF",
        "SCHO": "USD_Charles Schwab_Brokerage_SCHO",
        "SCHR": "USD_Charles Schwab_Brokerage_SCHR",
        "schwab_brokerage_cash": "USD_Charles Schwab_Brokerage_Cash",
        "schwab_ira_cash": "USD_Charles Schwab_IRA_Cash",
        "schwab_checking": "USD_Charles Schwab_Checking",
        "schwab_pledged_asset_line": "USD_Charles Schwab_Pledged Asset Line",
        "SCHX": "USD_Charles Schwab_Brokerage_SCHX",
        "SCHZ": "USD_Charles Schwab_Brokerage_SCHZ",
        "ubs_pillar2": "CHF_UBS_Pillar 2",
        "ubs_visa": "CHF_UBS_Visa",
        "ubs": "CHF_UBS_Primary",
        # pylint: disable-next=line-too-long
        "Vanguard Target Retirement 2040 Trust": "USD_Vanguard 401k_Vanguard Target Retirement 2040 Trust",
        "VWIAX": "USD_Vanguard 401k_VWIAX",
        "wealthfront_cash": "USD_Wealthfront_Cash",
        "wise_chf": "CHF_Wise",
        "wise_sgd": "SGD_Wise",
        "wise_usd": "USD_Wise",
        "SILVER": "USD_Commodities_Silver",
        "GOLD": "USD_Commodities_Gold",
        "zurcher": "CHF_Zurcher",
    }

    accounts_df_data = {}
    commodities = 0.0
    with open(
        common.PREFIX + "commodities_values.csv", newline="", encoding="utf-8"
    ) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = float(row["value"])
            commodities += value
            accounts_df_data[account_map[row["commodity"]]] = value

    etfs = 0.0
    with open(
        common.PREFIX + "etfs_values.csv", newline="", encoding="utf-8"
    ) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = float(row["value"])
            etfs += value
            accounts_df_data[account_map[row["ticker"]]] = value

    with open(common.PREFIX + "treasury_direct.txt", encoding="utf-8") as input_file:
        treasury_direct = float(input_file.read())
        accounts_df_data[account_map["treasury_direct"]] = treasury_direct

    total_investing = commodities + etfs + treasury_direct

    total_liquid = 0.0
    for chf_account in ["ubs", "ubs_visa", "cash_chf", "wise_chf"]:
        with open(common.PREFIX + chf_account + ".txt", encoding="utf-8") as input_file:
            value = float(input_file.read())
            total_liquid += value * chfusd
            accounts_df_data[account_map[chf_account]] = value

    with open(common.PREFIX + "wise_sgd.txt", encoding="utf-8") as input_file:
        value = float(input_file.read())
        total_liquid += value * sgdusd
        accounts_df_data[account_map["wise_sgd"]] = value

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
        with open(common.PREFIX + usd_account + ".txt", encoding="utf-8") as input_file:
            value = float(input_file.read())
            total_liquid += value
            accounts_df_data[account_map[usd_account]] = value

    total_real_estate = 0.0
    for estate in ["mtvernon", "northlake", "villamaria"]:
        with open(common.PREFIX + estate + ".txt", encoding="utf-8") as input_file:
            value = float(input_file.read())
            total_real_estate += value
            accounts_df_data[account_map[estate]] = value
    # Sold properties
    for estate in ["californiast", "corallake"]:
        accounts_df_data[account_map[estate]] = 0.0

    # Retirement
    with open(
        common.PREFIX + "schwab_ira_values.csv", newline="", encoding="utf-8"
    ) as csvfile:
        schwab_ira = 0.0
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = float(row["value"])
            schwab_ira += value
            accounts_df_data[account_map[row["ticker"]]] = value
    with open(common.PREFIX + "schwab_ira_cash.txt", encoding="utf-8") as input_file:
        schwab_ira_cash = float(input_file.read())
        schwab_ira += schwab_ira_cash
        accounts_df_data[account_map["schwab_ira_cash"]] = schwab_ira_cash

    # HSA closed October 2022
    accounts_df_data[account_map["healthequity"]] = 0.0

    with open(common.PREFIX + "ubs_pillar2.txt", encoding="utf-8") as input_file:
        value = float(input_file.read())
        pillar2 = value * chfusd
        accounts_df_data[account_map["ubs_pillar2"]] = value

    with open(common.PREFIX + "zurcher.txt", encoding="utf-8") as input_file:
        value = float(input_file.read())
        zurcher = value * chfusd
        accounts_df_data[account_map["zurcher"]] = value

    total_retirement = pillar2 + zurcher + schwab_ira
    total = total_investing + total_retirement + total_real_estate + total_liquid
    total_no_homes = total_investing + total_retirement + total_liquid

    history_df_data = {
        "total": total,
        "total_no_homes": total_no_homes,
        "total_real_estate": total_real_estate,
        "total_liquid": total_liquid,
        "total_investing": total_investing,
        "total_retirement": total_retirement,
        "etfs": etfs,
        "commodities": commodities,
        "401k": schwab_ira,
        "pillar2": pillar2,
    }
    forex_df_data = {
        "CHFUSD": chfusd,
        "SGDUSD": sgdusd,
    }

    with create_engine(common.SQLITE_URI).connect() as conn:
        history_df = pd.read_sql_table("history", conn, index_col="date")
        accounts_df = pd.read_sql_table("account_history", conn, index_col="date")
        forex_df = pd.read_sql_table("forex", conn, index_col="date")
        history_df = pd.concat(
            [
                history_df,
                pd.DataFrame(
                    history_df_data,
                    index=pd.DatetimeIndex([now]),
                    columns=history_df_data.keys(),
                ),
            ]
        )
        accounts_df = pd.concat(
            [
                accounts_df,
                pd.DataFrame(
                    accounts_df_data,
                    index=pd.DatetimeIndex([now]),
                    columns=accounts_df_data.keys(),
                ),
            ]
        )
        forex_df = pd.concat(
            [
                forex_df,
                pd.DataFrame(
                    forex_df_data,
                    index=pd.DatetimeIndex([now]),
                    columns=forex_df_data.keys(),
                ),
            ]
        )
        forex_df[-1:].to_sql("forex", conn, if_exists="append", index_label="date")
        history_df[-1:].to_sql("history", conn, if_exists="append", index_label="date")
        accounts_df[-1:].to_sql(
            "account_history", conn, if_exists="append", index_label="date"
        )
        conn.commit()


if __name__ == "__main__":
    main()
