#!/usr/bin/env python3
"""Methods for stock options."""

import io
import subprocess

import pandas as pd

import common
import etfs


def options_df_raw() -> pd.DataFrame:
    cmd = (
        f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/ (CALL|PUT)/' bal "
        + '--no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
    )
    entries = []
    for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
        if line[0].isalpha():
            account = line.strip().split(":")[-1]
            continue
        count = line.split(maxsplit=1)[0]
        call_name = line.split(maxsplit=1)[1].strip().strip('"')
        ticker = call_name.split()[0]
        option_type = call_name.split()[-1]
        strike = call_name.split()[-2]
        expiration = call_name.split()[-3]
        entries.append(
            {
                "name": call_name,
                "type": option_type,
                "ticker": ticker,
                "count": int(count),
                "strike": float(strike),
                "expiration": pd.to_datetime(expiration),
                "account": account,
            }
        )
    return pd.DataFrame(entries)


def options_df():
    """Get call and put dataframe."""
    calls_puts_df = options_df_raw()
    etfs_df = pd.read_csv(
        etfs.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "current_price"]
    ).fillna(0)
    joined_df = pd.merge(calls_puts_df, etfs_df, on="ticker").set_index(
        ["account", "name", "expiration"]
    )
    joined_df.loc[joined_df["type"] == "CALL", "in_the_money"] = (
        joined_df["strike"] < joined_df["current_price"]
    )
    joined_df.loc[joined_df["type"] == "PUT", "in_the_money"] = (
        joined_df["strike"] > joined_df["current_price"]
    )
    joined_df["exercise_value"] = joined_df["strike"] * joined_df["count"] * 100

    joined_df.loc[
        (joined_df["type"] == "CALL") & (joined_df["count"] < 0), "exercise_value"
    ] = abs(joined_df["strike"] * joined_df["count"] * 100)
    joined_df.loc[
        (joined_df["type"] == "PUT") & (joined_df["count"] < 0),
        "exercise_value",
    ] = abs(joined_df["strike"] * joined_df["count"] * 100) * -1
    joined_df["intrinsic_value"] = 0.0
    joined_df.loc[
        (joined_df["type"] == "CALL") & joined_df["in_the_money"],
        "intrinsic_value",
    ] = abs(
        (joined_df["current_price"] - joined_df["strike"]) * joined_df["count"] * 100
    )
    joined_df.loc[
        (joined_df["type"] == "PUT") & joined_df["in_the_money"],
        "intrinsic_value",
    ] = abs(
        (joined_df["strike"] - joined_df["current_price"]) * joined_df["count"] * 100
    )
    joined_df["min_contract_price"] = 0.0
    joined_df.loc[joined_df["in_the_money"], "min_contract_price"] = joined_df[
        "intrinsic_value"
    ] / (joined_df["count"] * 100)
    return joined_df.sort_values(["account", "expiration", "name"]).round(2)


def short_put_exposure(dataframe, broker):
    """Get exposure of short puts along with long puts."""
    try:
        broker_puts = dataframe.xs(broker, level="account").loc[
            lambda df: df["type"] == "PUT"
        ]
    except KeyError:
        return 0
    broker_short_puts = broker_puts[broker_puts["count"] < 0]
    total = 0
    for index, _ in broker_short_puts.iterrows():
        ticker_date = " ".join(index[0].split()[0:2])
        total += sum(broker_puts.filter(like=ticker_date, axis=0)["exercise_value"])
    return total


def after_assignment(itm_df):
    """Output balances after assignment."""
    etfs_df = etfs.get_etfs_df()
    etfs_df["shares_change"] = 0
    etfs_df["liquidity_change"] = 0
    for _, cols in itm_df.iterrows():
        match cols["type"]:
            case "CALL":
                multiplier = 1
            case "PUT":
                multiplier = -1
        etfs_df.loc[cols["ticker"], "shares"] += multiplier * cols["count"] * 100
        etfs_df.loc[cols["ticker"], "shares_change"] += multiplier * cols["count"] * 100
        etfs_df.loc[cols["ticker"], "liquidity_change"] += cols["exercise_value"]

    etfs_df = etfs_df[etfs_df["shares_change"] != 0]
    etfs_df["original_value"] = etfs_df["value"]
    etfs_df["value"] = etfs_df["shares"] * etfs_df["current_price"]
    etfs_df["value_change"] = etfs_df["value"] - etfs_df["original_value"]

    # Figure out trade profits
    for ticker, cols in etfs_df.iterrows():
        try:
            etf_options_trade_income = float(
                subprocess.check_output(
                    f'{common.LEDGER_PREFIX} -J -n --limit "commodity=~/{ticker}.+(CALL|PUT)/" bal',
                    text=True,
                    shell=True,
                ).split()[1]
            )
            etfs_df.loc[ticker, "options_trade_income"] = -etf_options_trade_income  # type: ignore
        except IndexError:
            pass
    etfs_df["profit_or_loss"] = (
        etfs_df["value_change"]
        + etfs_df["liquidity_change"]
        + etfs_df["options_trade_income"]
    )
    print(etfs_df.round(2))
    etfs_value_change = etfs_df["value_change"].sum()
    options_trade_income = etfs_df["options_trade_income"].sum()
    liquidity_change = etfs_df["liquidity_change"].sum()
    profit_or_loss = etfs_df["profit_or_loss"].sum()
    total_no_homes = common.read_sql_table("history").iloc[-1]["total_no_homes"]
    total_no_homes_new = total_no_homes + etfs_value_change + liquidity_change
    print(f"ETFs value change: {etfs_value_change:.0f}")
    print(f"Liquidity change: {liquidity_change}")
    print("  Ending balance:")
    for broker in ["Charles Schwab Brokerage", "Interactive Brokers"]:
        if broker in itm_df.index.get_level_values(0):
            print(f"    {broker}: {itm_df.xs(broker)['exercise_value'].sum()}")
    print(f"Options trade income: {options_trade_income:.0f}")
    print(f"Profit/loss compared to sale at current price: {profit_or_loss:.0f}")
    print(
        f"total_no_homes: {total_no_homes_new:.0f} (original: {total_no_homes:.0f}, "
        f"difference: {total_no_homes_new - total_no_homes:.0f})\n"
    )


def main():
    """Main."""
    options = options_df()
    print("Out of the money")
    print(
        options[options["in_the_money"] == False].drop(  # noqa: E712
            columns=["intrinsic_value", "min_contract_price"]
        )
    )
    print("\nIn the money")
    print(options[options["in_the_money"]], "\n")
    print("Balances after in the money options assigned")
    try:
        after_assignment(options[options["in_the_money"]])
    except KeyError:
        pass
    for broker in ["Charles Schwab Brokerage", "Interactive Brokers"]:
        print(f"{broker}")
        print(f"  Short put exposure: {short_put_exposure(options, broker)}")
        if broker in options.index.get_level_values(0):
            print(
                f"  Total exercise value: {options.xs(broker, level='account')['exercise_value'].sum()}"
            )
            print(options.xs(broker, level="account"))
            print("\n")


if __name__ == "__main__":
    main()
