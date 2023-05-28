#!/usr/bin/env python3
"""Write finance history."""

import csv
from datetime import datetime

import pandas as pd

import common


def main():
    """Main."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exchange_rates = common.get_tickers(["CHFUSD=X", "SGDUSD=X"])
    chfusd = exchange_rates["CHFUSD=X"]
    sgdusd = exchange_rates["SGDUSD=X"]

    account_map = {
        "bank7": ("USD", "Bank 7"),
        "bank1_liability2": ("USD", "Bank 1", "Liability 2"),
        "bank1_liability3": ("USD", "Bank 1", "Liability 3"),
        "bank1": ("USD", "Bank 1", "Checking"),
        "cash_chf": ("CHF", "Cash"),
        "health": ("USD", "Health HSA"),
        "treasury_direct": ("USD", "Treasury Direct"),
        "property1": ("USD", "Real Estate", "Property 1"),
        "property2": ("USD", "Real Estate", "Property 2"),
        "property3": ("USD", "Real Estate", "Property 3"),
        "property4": ("USD", "Real Estate", "Property 4"),
        "property5": ("USD", "Real Estate", "Property 5"),
        "ETF1": ("USD", "Bank 2", "Brokerage", "ETF1"),
        "ETF2": ("USD", "Bank 2", "Brokerage", "ETF2"),
        "ETF3": ("USD", "Bank 2", "Brokerage", "ETF3"),
        "ETF4": ("USD", "Bank 2", "Brokerage", "ETF4"),
        "ETF5": ("USD", "Bank 2", "Brokerage", "ETF5"),
        "brokerage_brokerage_cash": ("USD", "Bank 2", "Brokerage", "Cash"),
        "brokerage_checking": ("USD", "Bank 2", "Checking"),
        "brokerage_liability_1": ("USD", "Bank 2", "Liability 1"),
        "ETF6": ("USD", "Bank 2", "Brokerage", "ETF6"),
        "ETF7": ("USD", "Bank 2", "Brokerage", "ETF7"),
        "bank_chf_pillar2": ("CHF", "Bank 3", "Pillar 2"),
        "bank_chf_visa": ("CHF", "Bank 3", "Visa"),
        "bank_chf": ("CHF", "Bank 3", "Primary"),
        "Vanguard Target Retirement 2040 Trust": (
            "USD",
            "Vanguard 401k",
            "Vanguard Target Retirement 2040 Trust",
        ),
        "VWIAX": ("USD", "Vanguard 401k", "VWIAX"),
        "bank4_cash": ("USD", "Bank 4", "Cash"),
        "bank5_chf": ("CHF", "Bank 5"),
        "bank5_sgd": ("SGD", "Bank 5"),
        "bank5_usd": ("USD", "Bank 5"),
        "SILVER": ("USD", "Commodities", "Silver"),
        "GOLD": ("USD", "Commodities", "Gold"),
        "bank6": ("CHF", "Bank 6"),
    }

    accounts_df = pd.read_csv(
        common.PREFIX + "account_history.csv",
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True,
        header=[0, 1, 2, 3],
        nrows=1,
    )

    commodities = 0.0
    with open(
        common.PREFIX + "commodities_values.csv", newline="", encoding="utf-8"
    ) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = float(row["value"])
            commodities += value
            accounts_df.loc[now, account_map[row["commodity"]]] = value

    etfs = 0.0
    with open(
        common.PREFIX + "etfs_values.csv", newline="", encoding="utf-8"
    ) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = float(row["value"])
            etfs += value
            accounts_df.loc[now, account_map[row["ticker"]]] = value

    with open(common.PREFIX + "treasury_direct.txt", encoding="utf-8") as input_file:
        treasury_direct = float(input_file.read())
        accounts_df.loc[now, account_map["treasury_direct"]] = treasury_direct

    total_investing = commodities + etfs + treasury_direct

    total_liquid = 0.0
    for chf_account in ["bank_chf", "bank_chf_visa", "cash_chf", "bank5_chf"]:
        with open(common.PREFIX + chf_account + ".txt", encoding="utf-8") as input_file:
            value = float(input_file.read())
            total_liquid += value * chfusd
            accounts_df.loc[now, account_map[chf_account]] = value

    with open(common.PREFIX + "bank5_sgd.txt", encoding="utf-8") as input_file:
        value = float(input_file.read())
        total_liquid += value * sgdusd
        accounts_df.loc[now, account_map["bank5_sgd"]] = value

    for usd_account in [
        "bank5_usd",
        "bank7",
        "bank1",
        "bank1_liability2",
        "bank1_liability3",
        "brokerage_brokerage_cash",
        "brokerage_checking",
        "brokerage_liability_1",
        "bank4_cash",
    ]:
        with open(common.PREFIX + usd_account + ".txt", encoding="utf-8") as input_file:
            value = float(input_file.read())
            total_liquid += value
            accounts_df.loc[now, account_map[usd_account]] = value

    total_real_estate = 0.0
    for estate in ["property3", "property4", "property5"]:
        with open(common.PREFIX + estate + ".txt", encoding="utf-8") as input_file:
            value = float(input_file.read())
            total_real_estate += value
            accounts_df.loc[now, account_map[estate]] = value
    # Sold properties
    for estate in ["property1", "property2"]:
        accounts_df.loc[now, account_map[estate]] = 0

    with open(
        common.PREFIX + "401k_values.csv", newline="", encoding="utf-8"
    ) as csvfile:
        vanguard = 0.0
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = float(row["value"])
            vanguard += value
            accounts_df.loc[now, account_map[row["ticker"]]] = value

    # HSA closed October 2022
    accounts_df.loc[now, account_map["health"]] = 0

    with open(common.PREFIX + "bank_chf_pillar2.txt", encoding="utf-8") as input_file:
        value = float(input_file.read())
        pillar2 = value * chfusd
        accounts_df.loc[now, account_map["bank_chf_pillar2"]] = value

    with open(common.PREFIX + "bank6.txt", encoding="utf-8") as input_file:
        value = float(input_file.read())
        bank6 = value * chfusd
        accounts_df.loc[now, account_map["bank6"]] = value

    total_retirement = pillar2 + bank6 + vanguard
    total = total_investing + total_retirement + total_real_estate + total_liquid
    total_no_homes = total_investing + total_retirement + total_liquid

    fieldnames = [
        "total",
        "total_no_homes",
        "total_liquid",
        "total_real_estate",
        "total_retirement",
        "total_investing",
        "etfs",
        "commodities",
        "401k",
        "pillar2",
    ]
    data = {
        "total": total,
        "total_no_homes": total_no_homes,
        "total_real_estate": total_real_estate,
        "total_liquid": total_liquid,
        "total_investing": total_investing,
        "total_retirement": total_retirement,
        "etfs": etfs,
        "commodities": commodities,
        "401k": vanguard,
        "pillar2": pillar2,
    }
    history_df = pd.DataFrame(data, index=[now], columns=fieldnames)
    history_df.to_csv(
        common.PREFIX + "history.csv", mode="a", header=False, float_format="%.2f"
    )
    accounts_df[-1:].to_csv(
        common.PREFIX + "account_history.csv",
        mode="a",
        header=False,
        float_format="%.2f",
    )
    store_forex(chfusd, sgdusd, now)
    # To read in account history:
    # df = pd.read_csv(
    #     'account_history.csv', index_col=0, parse_dates=True,
    #     infer_datetime_format=True, header=[0,1,2,3])


def store_forex(chfusd, sgdusd, timestamp):
    """Store forex data in a CSV file."""
    fieldnames = ["CHFUSD", "SGDUSD"]
    data = {
        "CHFUSD": chfusd,
        "SGDUSD": sgdusd,
    }
    forex_df = pd.DataFrame(data, index=[timestamp], columns=fieldnames)
    forex_df.to_csv(
        common.PREFIX + "forex.csv",
        mode="a",
        header=False,
    )


if __name__ == "__main__":
    main()
