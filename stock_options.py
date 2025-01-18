#!/usr/bin/env python3
"""Methods for stock options."""

import io
import subprocess
import typing
from collections import defaultdict
from datetime import date

import pandas as pd

import common
import etfs


def options_df_raw() -> pd.DataFrame:
    cmd = (
        f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/ (CALL|PUT)/' bal "
        + '--no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
    )
    chfusd = common.read_sql_last("forex")["CHFUSD"].iloc[-1]
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
        multiplier = 100
        if ticker == "SMI":
            multiplier = 10 * chfusd
        entries.append(
            {
                "name": call_name,
                "type": option_type,
                "ticker": ticker,
                "count": int(count),
                "multiplier": multiplier,
                "strike": float(strike),
                "expiration": pd.to_datetime(expiration),
                "account": account,
            }
        )
    return pd.DataFrame(entries)


def add_options_quotes(options_df: pd.DataFrame):
    tickers = options_df["ticker"].unique()
    if not len(tickers):
        return options_df
    prices = []
    for idx, row in options_df.iterrows():
        idx = typing.cast(tuple, idx)
        if (
            price := common.get_ticker_option(
                row["ticker"],
                idx[2],
                row["type"],
                row["strike"],
            )
        ) is None:
            price = 0
        prices.append(price)
    options_df["quote"] = prices
    options_df["value"] = (
        options_df["count"] * options_df["quote"] * options_df["multiplier"]
    )
    return options_df


def add_value(options_df: pd.DataFrame) -> pd.DataFrame:
    df = add_options_quotes(options_df)
    # Take the maximum of intrinsic_value and value, keeping sign.
    df["value"] = df[["intrinsic_value", "value"]].abs().max(axis=1) * (
        df["count"] / df["count"].abs()
    )
    df["profit"] = df["value"] - (df["contract_price"] * df["count"] * df["multiplier"])
    return df


def add_contract_price(options_df: pd.DataFrame) -> pd.DataFrame:
    prices = []
    for idx, row in options_df.iterrows():
        broker: str = typing.cast(tuple, idx)[0]
        name = typing.cast(tuple, idx)[1].replace("/", r"\/")
        total = common.get_ledger_balance(
            f"""{common.LEDGER_PREFIX} -J -s reg --limit='commodity=~/"{name}"/' '{broker}'"""
        )
        prices.append(total / (row["count"] * row["multiplier"]))
    options_df["contract_price"] = prices
    return options_df


def add_index_prices(etfs_df: pd.DataFrame) -> pd.DataFrame:
    index_df = common.read_sql_last("index_prices")
    for ticker, index_ticker in (("SPX", "^SPX"), ("SMI", "^SSMI")):
        etfs_df.loc[ticker, "current_price"] = index_df[index_ticker].iloc[-1]
    return etfs_df


def options_df(with_value: bool = False) -> pd.DataFrame:
    """Get call and put dataframe."""
    calls_puts_df = options_df_raw()
    etfs_df = add_index_prices(etfs.get_etfs_df()[["current_price"]])
    joined_df = pd.merge(calls_puts_df, etfs_df, on="ticker").set_index(
        ["account", "name", "expiration"]
    )
    joined_df["in_the_money"] = False
    joined_df.loc[joined_df["type"] == "CALL", "in_the_money"] = (
        joined_df["strike"] < joined_df["current_price"]
    )
    joined_df.loc[joined_df["type"] == "PUT", "in_the_money"] = (
        joined_df["strike"] > joined_df["current_price"]
    )
    joined_df["exercise_value"] = (
        joined_df["strike"] * joined_df["count"] * joined_df["multiplier"]
    )
    joined_df.loc[joined_df["ticker"].isin(["SPX", "SMI"]), "exercise_value"] = (
        (joined_df["strike"] - joined_df["current_price"])
        * joined_df["count"]
        * joined_df["multiplier"]
    )
    joined_df.loc[joined_df["type"] == "CALL", "exercise_value"] = -joined_df[
        "exercise_value"
    ]
    joined_df.loc[
        (joined_df["type"] == "PUT")
        & (joined_df["count"] < 0)
        & (~joined_df["ticker"].isin(["SPX", "SMI"])),
        "exercise_value",
    ] = abs(joined_df["strike"] * joined_df["count"] * joined_df["multiplier"]) * -1
    joined_df["intrinsic_value"] = 0.0
    joined_df.loc[
        (joined_df["type"] == "CALL") & joined_df["in_the_money"],
        "intrinsic_value",
    ] = (
        (joined_df["current_price"] - joined_df["strike"])
        * joined_df["count"]
        * joined_df["multiplier"]
    )
    joined_df.loc[
        (joined_df["type"] == "PUT") & joined_df["in_the_money"],
        "intrinsic_value",
    ] = (
        (joined_df["strike"] - joined_df["current_price"])
        * joined_df["count"]
        * joined_df["multiplier"]
    )
    joined_df["min_contract_price"] = 0.0
    joined_df.loc[joined_df["in_the_money"], "min_contract_price"] = joined_df[
        "intrinsic_value"
    ] / (joined_df["count"] * joined_df["multiplier"])
    joined_df = joined_df.sort_values(["account", "expiration", "name"])
    joined_df = add_contract_price(joined_df)
    if with_value:
        joined_df = add_value(joined_df)
    return joined_df.round(2)


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


def after_assignment_df(itm_df: pd.DataFrame) -> pd.DataFrame:
    etfs_df = add_index_prices(etfs.get_etfs_df())
    etfs_df["shares_change"] = 0.0
    etfs_df["liquidity_change"] = 0.0
    for _, cols in itm_df.iterrows():
        match cols["type"]:
            case "CALL":
                multiplier = 1
            case "PUT":
                multiplier = -1
        etfs_df.loc[cols["ticker"], "shares"] += (
            multiplier * cols["count"] * cols["multiplier"]
        )
        etfs_df.loc[cols["ticker"], "shares_change"] += (
            multiplier * cols["count"] * cols["multiplier"]
        )
        etfs_df.loc[cols["ticker"], "liquidity_change"] += cols["exercise_value"]

    etfs_df = etfs_df[etfs_df["shares_change"] != 0]
    etfs_df["original_value"] = etfs_df["value"]
    etfs_df["value"] = etfs_df["shares"] * etfs_df["current_price"]
    etfs_df["value_change"] = etfs_df["value"] - etfs_df["original_value"]
    return etfs_df.dropna()


def get_expiration_values(
    itm_df: pd.DataFrame,
) -> typing.Mapping[str, list[tuple[date, float]]]:
    expiration_values = defaultdict(list)
    for broker in common.BROKERAGES:
        if broker in itm_df.index.get_level_values(0):
            broker_df = itm_df.xs(broker)
            for expiration in broker_df.index.get_level_values(1).unique():
                expiration_values[broker].append(
                    (
                        expiration.date(),
                        broker_df.xs(expiration, level="expiration")[
                            "exercise_value"
                        ].sum(),
                    )
                )
    return expiration_values


def after_assignment(itm_df):
    """Output balances after assignment."""
    if len(etfs_df := after_assignment_df(itm_df)):
        print(etfs_df.round(2))
        etfs_value_change = etfs_df["value_change"].sum()
        liquidity_change = etfs_df["liquidity_change"].sum()
        print(f"ETFs value change: {etfs_value_change:.0f}")
        print(f"ETFs liquidity change: {liquidity_change:.0f}")
    print("  Balance change:")
    for broker in sorted(values := get_expiration_values(itm_df)):
        expiration_values = values[broker]
        print(f"    {broker}")
        for expiration, value in expiration_values:
            print(f"      Expiration: {expiration}: {value:.0f}")
    print()


def find_bull_put_spreads(options_df: pd.DataFrame) -> list[pd.DataFrame]:
    """Find bull put spreads. Remove box spreads before calling."""
    dataframes = []
    for index, row in options_df.iterrows():
        ticker = row["ticker"]
        if ticker == "SPX":
            # Find a long PUT
            if row["type"] == "PUT" and row["count"] > 0:
                # The long PUT
                low_long_put = options_df.query(
                    'ticker == @ticker & type == "PUT" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count > 0'
                )
                # Find a short PUT at higher strike, same expiration and broker
                high_short_put = options_df.query(
                    'ticker == @ticker & type == "PUT" & strike > @row["strike"] & expiration == @index[2] & account == @index[0] & count < 0'
                )
                found = pd.concat([low_long_put, high_short_put])
                if len(found) == 2:
                    dataframes.append(found)
    return dataframes


def find_box_spreads(options_df: pd.DataFrame) -> list[pd.DataFrame]:
    """Find box spreads."""
    box_dataframes = []
    for index, row in options_df.iterrows():
        ticker = row["ticker"]
        if ticker in (["SPX", "SMI"]):
            # Find a short CALL
            if row["type"] == "CALL" and row["count"] < 0:
                # The short call
                low_short_call = options_df.query(
                    'ticker == @ticker & type == "CALL" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count < 0'
                )
                # Find a long PUT at same strike, expiration and broker
                low_long_put = options_df.query(
                    'ticker == @ticker & type == "PUT" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count > 0'
                )
                # Find a long CALL at higher strike, same expiration and broker
                high_long_call = options_df.query(
                    'ticker == @ticker & type == "CALL" & strike > @row["strike"] & expiration == @index[2] & account == @index[0] & count > 0'
                )
                # Find a short PUT at higher strike, same expiration and broker
                high_short_put = options_df.query(
                    'ticker == @ticker & type == "PUT" & strike > @row["strike"] & expiration == @index[2] & account == @index[0] & count < 0'
                )
                found = pd.concat(
                    [low_short_call, low_long_put, high_long_call, high_short_put]
                )
                if len(found) == 4:
                    box_dataframes.append(found)
    return box_dataframes


def remove_spreads(
    options_df: pd.DataFrame, spreads: list[pd.DataFrame]
) -> pd.DataFrame:
    if len(spreads) == 0:
        return options_df
    return options_df[~options_df.isin(pd.concat(spreads))].dropna()


def remove_box_spreads(options_df: pd.DataFrame) -> pd.DataFrame:
    """Remove box spreads."""
    return remove_spreads(options_df, find_box_spreads(options_df))


def get_spread_details(
    spread_df: pd.DataFrame,
) -> tuple[str, int, str, date, float, float]:
    low_strike = spread_df["strike"].min()
    high_strike = spread_df["strike"].max()
    count = int(spread_df["count"].max())
    row = spread_df.iloc[0]
    index = spread_df.index[0]
    return index[0], count, row["ticker"], index[2].date(), low_strike, high_strike


def summarize_box(box_df: pd.DataFrame):
    account, count, ticker, expiration, low_strike, high_strike = get_spread_details(
        box_df
    )
    print(f"{account}")
    print(f"{count} {ticker} {expiration} {low_strike:.0f}/{high_strike:.0f} Box")
    total = box_df.query("in_the_money == True")["exercise_value"].sum()
    print(f"Exercise value: {total:.0f}", end="")
    if count > 1:
        print(f" ({total / count:.0f} per contract)", end="")
    print("\n")


def summarize_bull_put(bull_put_df: pd.DataFrame):
    account, count, ticker, expiration, low_strike, high_strike = get_spread_details(
        bull_put_df
    )
    print(f"{account}")
    print(f"{count} {ticker} {expiration} {low_strike:.0f}/{high_strike:.0f} Bull Put")
    total = bull_put_df.query("in_the_money == True")["exercise_value"].sum()
    print(f"Exercise value: {total:.0f}")
    print(f"Maximum risk: {bull_put_df['exercise_value'].sum():.0f}\n")


def get_options_and_spreads() -> tuple[
    pd.DataFrame, pd.DataFrame, list[pd.DataFrame], list[pd.DataFrame]
]:
    all_options = options_df(with_value=True)
    box_spreads = find_box_spreads(all_options)
    pruned_options = remove_spreads(all_options, box_spreads)
    bull_put_spreads = find_bull_put_spreads(pruned_options)
    pruned_options = remove_spreads(pruned_options, bull_put_spreads)
    return all_options, pruned_options, box_spreads, bull_put_spreads


def main(show_spreads: bool = True):
    """Main."""
    all_options, options, box_spreads, bull_put_spreads = get_options_and_spreads()
    if len(otm_df := options.query("in_the_money == False")):
        print("Out of the money")
        print(otm_df.drop(columns=["intrinsic_value", "min_contract_price"]), "\n")
    if len(itm_df := options.query("in_the_money == True")):
        print("In the money")
        print(itm_df, "\n")
    print(
        "Balances after in the money options assigned (includes spreads not shown above)"
    )
    after_assignment(all_options.query("in_the_money == True"))
    for broker in common.BROKERAGES:
        if broker in options.index.get_level_values(0):
            print(f"{broker}")
            print(f"  Short put exposure: {short_put_exposure(options, broker):.0f}")
            print(
                f"  Total exercise value: {options.xs(broker, level='account')['exercise_value'].sum():.0f}"
            )
            print(options.xs(broker, level="account"), "\n")

    if show_spreads:
        if bull_put_spreads:
            print("Bull put spreads")
            for spread in bull_put_spreads:
                summarize_bull_put(spread)

        if box_spreads:
            print("Box spreads")
            for box in box_spreads:
                summarize_box(box)


if __name__ == "__main__":
    main()
