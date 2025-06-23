#!/usr/bin/env python3
"""Methods for stock options."""

import contextlib
import io
import itertools
import pickle
import re
import subprocess
import typing
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from functools import partial

import pandas as pd
from loguru import logger

import common
import etfs
import ledger_ops

REDIS_KEY = "OptionsData"


class CommonDetails(typing.NamedTuple):
    account: str
    amount: int
    ticker: str
    ticker_price: float
    expiration: date
    contract_price: float
    contract_price_per_share: float
    half_mark: float
    double_mark: float
    quote: float
    profit: float
    intrinsic_value: float


class ShortCallDetails(typing.NamedTuple):
    details: CommonDetails
    strike: float
    profit_stock: float


class SpreadDetails(typing.NamedTuple):
    details: CommonDetails
    low_strike: float
    high_strike: float
    risk: float


class Spread(typing.NamedTuple):
    df: pd.DataFrame
    details: SpreadDetails


class BoxSpreadDetails(typing.NamedTuple):
    details: SpreadDetails
    earliest_transaction_date: date
    loan_term_days: int
    apy: float


class BoxSpread(typing.NamedTuple):
    df: pd.DataFrame
    details: BoxSpreadDetails


class IronCondorDetails(typing.NamedTuple):
    details: CommonDetails
    low_put_strike: float
    high_put_strike: float
    low_call_strike: float
    high_call_strike: float
    risk: float


class IronCondor(typing.NamedTuple):
    df: pd.DataFrame
    details: IronCondorDetails


class ExpirationValue(typing.NamedTuple):
    expiration: date
    value: float


class BrokerExpirationValues(typing.NamedTuple):
    broker: str
    values: list[ExpirationValue]


class OptionsValue(typing.NamedTuple):
    value: float
    notional_value: float


class OptionsAndSpreads(typing.NamedTuple):
    all_options: pd.DataFrame
    # Options with box, bull put, and bear call spreads removed.
    pruned_options: pd.DataFrame
    short_calls: list[ShortCallDetails]
    box_spreads: list[BoxSpread]
    iron_condors: list[IronCondor]
    # These include spreads part of iron condors.
    bull_put_spreads: list[Spread]
    bear_call_spreads: list[Spread]
    # These do not include spreads part of iron condors.
    bull_put_spreads_no_ic: list[Spread]
    bear_call_spreads_no_ic: list[Spread]
    synthetics: list[Spread]
    options_value_by_brokerage: dict[str, OptionsValue]


class OptionsData(typing.NamedTuple):
    opts: OptionsAndSpreads
    bev: list[BrokerExpirationValues]
    main_output: str
    updated: datetime


def options_df_raw() -> pd.DataFrame:
    search = " (CALL|PUT)"
    cmd = (
        f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/{search}/' "
        + 'bal --no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
    )
    chfusd = common.get_latest_forex()["CHFUSD"]
    entries = []
    account = ""
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
        if account:
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
    deltas = []
    for idx, row in options_df.iterrows():
        expiration = typing.cast(tuple, idx)[2].date()
        ticker = row["ticker"]
        quote = common.get_option_quote(
            common.TickerOption(
                ticker=ticker,
                expiration=expiration,
                contract_type=row["type"],
                strike=row["strike"],
            ),
        )
        price = delta = 0
        if quote:
            price = quote.mark
            delta = quote.delta
        prices.append(price)
        deltas.append(delta)
    options_df["quote"] = prices
    options_df["delta"] = deltas
    options_df["value"] = (
        options_df["count"] * options_df["quote"] * options_df["multiplier"]
    )
    options_df["notional_value"] = (
        options_df["delta"]
        * options_df["strike"]
        * options_df["count"]
        * options_df["multiplier"]
    )
    return options_df


def add_value(options_df: pd.DataFrame) -> pd.DataFrame:
    df = add_options_quotes(options_df)
    # Take the maximum of intrinsic_value and value, keeping sign.
    df["value"] = df[["intrinsic_value", "value"]].abs().max(axis=1) * (
        df["count"] / df["count"].abs()
    )
    df["profit_option_value"] = df["value"] - (
        df["contract_price"] * df["count"] * df["multiplier"]
    )
    df["profit_stock_price"] = abs(
        df["count"] * df["multiplier"] * (df["strike"] + df["contract_price"])
    ) - abs(df["count"] * df["multiplier"] * df["current_price"])
    df.loc[(df["type"] == "PUT") & (df["count"] < 0), "profit_stock_price"] *= -1
    df.loc[(df["type"] == "CALL") & (df["count"] > 0), "profit_stock_price"] *= -1
    df.loc[~df["in_the_money"], "profit_stock_price"] = 0
    return df


def convert_to_usd(amount: str) -> float:
    if amount.startswith("$"):
        return float(amount[1:])
    elif amount.endswith(" CHF"):
        return float(amount.split()[0]) * common.get_latest_forex()["CHFUSD"]
    return 0


def get_ledger_entries_command(broker: str, option_name: str) -> str:
    return (
        f"""{common.LEDGER_PREFIX} print expr 'any(commodity == "\\"{option_name}\\"" and account =~ /{broker}/)'"""
        """ --limit 'payee != "Options assignment"'"""
    )


def get_ledger_earliest_date(spread_df: pd.DataFrame) -> typing.Optional[datetime]:
    earliest_entry_date: typing.Optional[datetime] = None
    for index, _ in spread_df.iterrows():
        broker: str = typing.cast(tuple, index)[0]
        option_name: str = typing.cast(tuple, index)[1]
        for entry in ledger_ops.get_ledger_entries_from_command(
            get_ledger_entries_command(broker, option_name)
        ):
            entry_date = entry.parse_date()
            if earliest_entry_date is None or entry_date < earliest_entry_date:
                earliest_entry_date = entry_date
    return earliest_entry_date


def get_ledger_total(broker: str, option_name: str) -> float:
    total = 0.0
    ledger_cmd = get_ledger_entries_command(broker, option_name)
    for entry in ledger_ops.get_ledger_entries_from_command(ledger_cmd):
        found_name = False
        for line in entry.full_list():
            # Only count expenses after the option name
            if "Expenses:Broker:Fees" in line and found_name:
                amount = line.split(maxsplit=1)[-1]
                total += convert_to_usd(amount)
                # Reset found state
                found_name = False
            elif option_name in line:
                if " CALL" not in line and " PUT" not in line:
                    continue
                s = re.split(r"\s{2,}", line.lstrip())
                count = int(s[1].split()[0])
                amount = s[1].split("@")[-1].lstrip()
                total += convert_to_usd(amount) * count
                found_name = True
    return total


def add_contract_price(options_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding contract prices")
    ops = []

    def divide_total(broker: str, name: str, divisor: float) -> float:
        return get_ledger_total(broker, name) / divisor

    for idx, row in options_df.iterrows():
        broker: str = typing.cast(tuple, idx)[0]
        name: str = typing.cast(tuple, idx)[1]
        ops.append(
            partial(divide_total, broker, name, row["count"] * row["multiplier"])
        )
    with ThreadPoolExecutor() as e:
        prices = list(e.map(lambda op: op(), ops))
    options_df["contract_price"] = prices
    logger.info("Finished adding contract prices")
    return options_df


def add_current_price(calls_puts_df: pd.DataFrame) -> pd.DataFrame:
    for ticker in calls_puts_df["ticker"].unique():
        query_ticker = ticker
        if ticker == "SPX":
            query_ticker = "^SPX"
        calls_puts_df.loc[lambda df: df["ticker"] == ticker, "current_price"] = (  # type: ignore
            common.get_ticker(query_ticker)
        )
    return calls_puts_df


def options_df() -> pd.DataFrame:
    """Get call and put dataframe."""
    calls_puts_df = options_df_raw()
    if not len(calls_puts_df):
        return calls_puts_df
    df = add_current_price(calls_puts_df)
    df = df.set_index(["account", "name", "expiration"])
    df["in_the_money"] = False
    df.loc[df["type"] == "CALL", "in_the_money"] = df["strike"] < df["current_price"]
    df.loc[df["type"] == "PUT", "in_the_money"] = df["strike"] > df["current_price"]
    df["exercise_value"] = df["strike"] * df["count"] * df["multiplier"]
    df.loc[df["ticker"].isin(["SPX", "SMI"]), "exercise_value"] = (
        (df["strike"] - df["current_price"]) * df["count"] * df["multiplier"]
    )
    df.loc[df["type"] == "CALL", "exercise_value"] = -df["exercise_value"]
    df.loc[
        (df["type"] == "PUT")
        & (df["count"] < 0)
        & (~df["ticker"].isin(["SPX", "SMI"])),
        "exercise_value",
    ] = abs(df["strike"] * df["count"] * df["multiplier"]) * -1
    df["intrinsic_value"] = 0.0
    df.loc[
        (df["type"] == "CALL") & df["in_the_money"],
        "intrinsic_value",
    ] = (df["current_price"] - df["strike"]) * df["count"] * df["multiplier"]
    df.loc[
        (df["type"] == "PUT") & df["in_the_money"],
        "intrinsic_value",
    ] = (df["strike"] - df["current_price"]) * df["count"] * df["multiplier"]
    df["min_contract_price"] = 0.0
    df.loc[df["in_the_money"], "min_contract_price"] = df["intrinsic_value"] / (
        df["count"] * df["multiplier"]
    )
    df = df.sort_values(["account", "expiration", "name"])
    df = add_contract_price(df)
    df = add_value(df)
    return df


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
    etfs_df = etfs.get_etfs_df()
    for ticker in itm_df["ticker"].unique():
        if ticker not in etfs_df.index:
            for f in ("current_price", "shares", "value"):
                etfs_df.loc[ticker, f] = 0
    etfs_df = add_current_price(etfs_df.reset_index()).set_index("ticker")
    etfs_df["shares_change"] = 0.0
    etfs_df["liquidity_change"] = 0.0
    for _, cols in itm_df.iterrows():
        match cols["type"]:
            case "CALL":
                multiplier = 1
            case "PUT":
                multiplier = -1
            case _:
                continue
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
) -> list[BrokerExpirationValues]:
    expiration_values: list[BrokerExpirationValues] = []
    for broker in sorted(common.BROKERAGES):
        values: list[ExpirationValue] = []
        if broker in itm_df.index.get_level_values(0):
            broker_df = itm_df.xs(broker)
            for expiration in broker_df.index.get_level_values(1).unique():
                values.append(
                    ExpirationValue(
                        expiration.date(),
                        broker_df.xs(expiration, level="expiration")[
                            "exercise_value"
                        ].sum(),
                    )
                )
            expiration_values.append(BrokerExpirationValues(broker, values))
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
    for ev in get_expiration_values(itm_df):
        print(f"    {ev.broker}")
        for v in ev.values:
            print(
                f"      Expiration: {v.expiration} ({(v.expiration - date.today()).days}d): {v.value:.0f}"
            )
    print()


def find_synthetics(options_df: pd.DataFrame) -> list[Spread]:
    spreads: list[Spread] = []
    for index, row in options_df.iterrows():
        ticker = row["ticker"]  # noqa: F841
        if ticker == "SPX":
            continue
        # Find a long CALL
        if row["type"] == "CALL" and row["count"] > 0:
            # The long CALL
            long_call = options_df.query(
                'ticker == @ticker & type == "CALL" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count == @row["count"]'
            )
            # Find a short PUT at same strike, count, expiration and broker
            short_put = options_df.query(
                'ticker == @ticker & type == "PUT" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count < 0 & count >= -@row["count"]'
            )
            found = pd.concat([long_call, short_put])
            if len(found) == 2:
                found.loc[found.index[0], "count"] = found["count"].abs().min()
                found.loc[found.index[1], "count"] = -found["count"].abs().min()
                spreads.append(Spread(df=found, details=get_spread_details(found)))
    return spreads


def find_bull_put_spreads(options_df: pd.DataFrame) -> list[Spread]:
    """Find bull put spreads. Remove box spreads before calling."""
    spreads: list[Spread] = []
    for index, row in options_df.iterrows():
        ticker = row["ticker"]  # noqa: F841
        # Find a long PUT
        if row["type"] == "PUT" and row["count"] > 0:
            # The long PUT
            low_long_put = options_df.query(
                'ticker == @ticker & type == "PUT" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count == @row["count"]'
            )
            # Find a short PUT at higher strike, same expiration and broker
            high_short_put = options_df.query(
                'ticker == @ticker & type == "PUT" & strike > @row["strike"] & expiration == @index[2] & account == @index[0] & count == -@row["count"]'
            )
            found = pd.concat([low_long_put, high_short_put])
            if len(found) == 2:
                spreads.append(Spread(df=found, details=get_spread_details(found)))
    return spreads


def find_bear_call_spreads(options_df: pd.DataFrame) -> list[Spread]:
    """Find bear call spreads. Remove box spreads before calling."""
    spreads = []
    for index, row in options_df.iterrows():
        ticker = row["ticker"]  # noqa: F841
        # Find a long CALL
        if row["type"] == "CALL" and row["count"] > 0:
            # The long CALL
            high_long_call = options_df.query(
                'ticker == @ticker & type == "CALL" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count == @row["count"]'
            )
            # Find a short CALL at lower strike, same expiration and broker
            low_short_call = options_df.query(
                'ticker == @ticker & type == "CALL" & strike < @row["strike"] & expiration == @index[2] & account == @index[0] & count == -@row["count"]'
            )
            found = pd.concat([low_short_call, high_long_call])
            if len(found) == 2:
                spreads.append(Spread(df=found, details=get_spread_details(found)))
    return spreads


def find_iron_condors(
    bull_put_spreads: list[Spread], bear_call_spreads: list[Spread]
) -> list[IronCondor]:
    iron_condors: list[IronCondor] = []
    for bull_put_df in bull_put_spreads:
        for bear_call_df in bear_call_spreads:
            low_long_put = bull_put_df.df.query('type == "PUT" & count > 0')
            ticker = low_long_put["ticker"].iloc[0]  # noqa: F841
            expiration = low_long_put.index.get_level_values("expiration")[0]  # noqa: F841
            account = low_long_put.index.get_level_values("account")[0]  # noqa: F841
            high_short_put = bull_put_df.df.query('type == "PUT" & count < 0')
            high_short_put_strike = high_short_put["strike"].max()  # noqa: F841
            matcher = (
                "account == @account & ticker == @ticker & expiration == @expiration"
            )
            high_long_call = bear_call_df.df.query(
                f'{matcher} & type == "CALL" & count > 0 & strike > @high_short_put_strike'
            )
            high_long_call_strike = high_long_call["strike"].max()  # noqa: F841
            low_short_call = bear_call_df.df.query(
                f'{matcher} & type == "CALL" & count < 0 & strike < @high_long_call_strike'
            )
            found = pd.concat(
                [low_long_put, high_short_put, low_short_call, high_long_call]
            )
            if len(found) == 4:
                iron_condors.append(
                    IronCondor(df=found, details=get_iron_condor_details(found))
                )
    return iron_condors


def find_box_spreads(options_df: pd.DataFrame) -> list[BoxSpread]:
    """Find box spreads."""
    box_spreads: list[BoxSpread] = []
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
                    box_spreads.append(
                        BoxSpread(df=found, details=get_box_spread_details(found))
                    )
    return box_spreads


def remove_spreads(
    options_df: pd.DataFrame, spreads: list[pd.DataFrame]
) -> pd.DataFrame:
    if len(spreads) == 0:
        return options_df
    return options_df[~options_df.isin(pd.concat(spreads))].dropna()


def get_short_call_details(options_df: pd.DataFrame) -> list[ShortCallDetails]:
    short_calls = options_df.query("type == 'CALL' & count < 0")
    short_call_details = []
    for i in range(len(short_calls)):
        row = short_calls.iloc[i]
        account = row.name[0]  # type: ignore
        expiration = row.name[2].date()  # type: ignore
        contract_price = row["contract_price"] * row["count"] * row["multiplier"]
        contract_price_per_share = row["contract_price"]
        short_call_details.append(
            ShortCallDetails(
                details=CommonDetails(
                    account=account,
                    amount=row["count"],
                    ticker=row["ticker"],
                    ticker_price=row["current_price"],
                    expiration=expiration,
                    contract_price=contract_price,
                    contract_price_per_share=contract_price_per_share,
                    half_mark=contract_price_per_share / 2,
                    double_mark=contract_price_per_share * 2,
                    quote=row["value"],
                    profit=row["value"] - contract_price,
                    intrinsic_value=row["intrinsic_value"],
                ),
                strike=row["strike"],
                profit_stock=row["profit_stock_price"],
            )
        )
    return short_call_details


def get_box_spread_details(spread_df: pd.DataFrame) -> BoxSpreadDetails:
    earliest_entry_date = get_ledger_earliest_date(spread_df)
    if earliest_entry_date is None:
        raise ValueError("No date found for box spread entry")
    details = get_spread_details(spread_df)
    cd = details.details
    loan_term_days = (cd.expiration - earliest_entry_date.date()).days
    apy = (
        ((cd.intrinsic_value - cd.contract_price) / cd.contract_price)
        / loan_term_days
        * 365
    )
    return BoxSpreadDetails(
        details=details,
        earliest_transaction_date=earliest_entry_date.date(),
        loan_term_days=loan_term_days,
        apy=apy,
    )


def get_spread_details(spread_df: pd.DataFrame) -> SpreadDetails:
    low_strike = spread_df["strike"].min()
    high_strike = spread_df["strike"].max()
    count = int(spread_df["count"].max())
    multiplier = spread_df["multiplier"].max()
    row = spread_df.iloc[0]
    index = spread_df.index[0]
    c = spread_df.copy()
    c["price"] = c["contract_price"] * c["count"] * c["multiplier"]
    contract_price = c["price"].sum()
    half_mark = contract_price / count / multiplier / 2
    double_mark = contract_price / count / multiplier * 2
    quote = spread_df["value"].sum()
    return SpreadDetails(
        details=CommonDetails(
            account=index[0],
            amount=count,
            ticker=row["ticker"],
            ticker_price=row["current_price"],
            expiration=index[2].date(),
            contract_price=contract_price,
            contract_price_per_share=c["contract_price"].sum(),
            half_mark=half_mark,
            double_mark=double_mark,
            intrinsic_value=spread_df["intrinsic_value"].sum(),
            quote=quote,
            profit=quote - contract_price,
        ),
        low_strike=low_strike,
        high_strike=high_strike,
        risk=spread_df["exercise_value"].sum() - contract_price,
    )


def get_iron_condor_details(
    iron_condor_df: pd.DataFrame,
) -> IronCondorDetails:
    low_put_strike = iron_condor_df.query("type == 'PUT'")["strike"].min()
    high_put_strike = iron_condor_df.query("type == 'PUT'")["strike"].max()
    low_call_strike = iron_condor_df.query("type == 'CALL'")["strike"].min()
    high_call_strike = iron_condor_df.query("type == 'CALL'")["strike"].max()
    width = max(high_call_strike - low_call_strike, high_put_strike - low_put_strike)
    count = int(iron_condor_df["count"].max())
    multiplier = iron_condor_df["multiplier"].max()
    row = iron_condor_df.iloc[0]
    index = iron_condor_df.index[0]
    c = iron_condor_df.copy()
    c["price"] = c["contract_price"] * c["count"] * c["multiplier"]
    contract_price = c["price"].sum()
    account = index[0]
    ticker = row["ticker"]
    half_mark = contract_price / count / multiplier / 2
    double_mark = contract_price / count / multiplier * 2
    quote = iron_condor_df["value"].sum()
    risk = -((width * count * multiplier) + contract_price)
    return IronCondorDetails(
        details=CommonDetails(
            account=account,
            amount=count,
            ticker=ticker,
            ticker_price=row["current_price"],
            expiration=index[2].date(),
            contract_price=contract_price,
            contract_price_per_share=c["contract_price"].sum(),
            half_mark=half_mark,
            double_mark=double_mark,
            intrinsic_value=iron_condor_df["intrinsic_value"].sum(),
            quote=quote,
            profit=quote - contract_price,
        ),
        low_put_strike=low_put_strike,
        high_put_strike=high_put_strike,
        low_call_strike=low_call_strike,
        high_call_strike=high_call_strike,
        risk=risk,
    )


def summarize_iron_condor(iron_condor: IronCondor):
    d = iron_condor.details
    cd = d.details
    print(f"{cd.account}")
    print(
        f"{cd.amount} {cd.ticker} {cd.expiration} {d.low_put_strike:.0f}/{d.high_put_strike:.0f}/{d.low_call_strike:.0f}/{d.high_call_strike:.0f} Iron Condor"
    )
    print(f"Contract price: {cd.contract_price:.0f}")
    print(f"Half mark: {cd.half_mark:.2f}")
    print(f"Double mark: {cd.double_mark:.2f}")
    print(f"Maximum risk: {d.risk:.0f}")
    print(f"Ticker price: {cd.ticker_price}")
    print(f"Profit: {cd.profit:.0f}\n")


def summarize_box(box: BoxSpread):
    d = box.details
    cd = d.details.details
    print(f"{cd.account}")
    print(
        f"{cd.amount} {cd.ticker} {cd.expiration} {d.details.low_strike:.0f}/{d.details.high_strike:.0f} Box"
    )
    print(f"Earliest transaction date: {d.earliest_transaction_date}")
    print(f"Loan term: {d.loan_term_days} days")
    print(f"APY: {d.apy:.2%}")
    print(f"Contract price: {cd.contract_price:.0f}")
    print(f"Exercise value: {cd.intrinsic_value:.0f}", end="")
    if cd.amount > 1:
        print(f" ({cd.intrinsic_value / cd.amount:.0f} per contract)", end="")
    print("\n")


def modify_otm_leg(spread_df: pd.DataFrame) -> typing.Optional[pd.DataFrame]:
    leg = spread_df.query("in_the_money == False").copy()
    if len(leg) == 1:
        # Keep sign
        sign = leg["exercise_value"].iloc[0] / abs(leg["exercise_value"].iloc[0])
        leg["exercise_value"] = (
            leg["count"] * leg["multiplier"] * leg["current_price"] * sign
        )
        return leg
    return None


def summarize_spread(spread: Spread, title: str):
    d = spread.details
    cd = d.details
    print(f"{cd.account}")
    print(
        f"{cd.amount} {cd.ticker} {cd.expiration} {d.low_strike:.0f}/{d.high_strike:.0f} {title}"
    )
    total = spread.df.query("in_the_money == True")["exercise_value"].sum()
    if (otm_leg := modify_otm_leg(spread.df)) is not None:
        total += otm_leg["exercise_value"].sum()
    print(f"Contract price: {cd.contract_price:.0f}")
    print(f"Half mark: {cd.half_mark:.2f}")
    print(f"Double mark: {cd.double_mark:.2f}")
    print(f"Exercise value: {total:.0f}")
    print(f"Maximum risk: {spread.df['exercise_value'].sum():.0f}")
    print(f"Ticker price: {cd.ticker_price}")
    print(f"Profit: {cd.profit:.0f}\n")


def get_options_value_by_brokerage(
    pruned_options: pd.DataFrame,
    bull_put_spreads: list[Spread],
    bear_call_spreads: list[Spread],
) -> dict[str, OptionsValue]:
    values: dict[str, OptionsValue] = {}
    for broker in common.BROKERAGES:
        options_value = pruned_options.query(f"account == '{broker}'")["value"].sum()
        options_notional_value = pruned_options.query(f"account == '{broker}'")[
            "notional_value"
        ].sum()
        dfs = [s.df for s in bull_put_spreads + bear_call_spreads]
        if dfs:
            spread_df = pd.concat(dfs)
            options_value += spread_df.query(
                f"account == '{broker}' and ticker != 'SPX'"
            )["value"].sum()
            # SPX spreads handled differently.
            options_value += spread_df.query(
                f"account == '{broker}' and ticker == 'SPX'"
            )["intrinsic_value"].sum()
        values[broker] = OptionsValue(
            value=options_value, notional_value=options_notional_value
        )
    return values


def get_options_and_spreads() -> OptionsAndSpreads:
    all_options = options_df()
    box_spreads = find_box_spreads(all_options)
    pruned_options = remove_spreads(all_options, [s.df for s in box_spreads])
    bull_put_spreads = find_bull_put_spreads(pruned_options)
    pruned_options = remove_spreads(pruned_options, [s.df for s in bull_put_spreads])
    bear_call_spreads = find_bear_call_spreads(pruned_options)
    pruned_options = remove_spreads(pruned_options, [s.df for s in bear_call_spreads])
    iron_condors = find_iron_condors(bull_put_spreads, bear_call_spreads)
    synthetics = find_synthetics(pruned_options)

    def get_spread_no_ic(spreads: list[Spread]) -> list[Spread]:
        pruned_spreads: list[Spread] = []
        for spread in spreads:
            spread_df = remove_spreads(spread.df, [s.df for s in iron_condors])
            if not len(spread_df):
                continue
            pruned_spreads.append(spread)
        return pruned_spreads

    bull_put_spreads_no_ic = get_spread_no_ic(bull_put_spreads)
    bear_call_spreads_no_ic = get_spread_no_ic(bear_call_spreads)
    short_calls = get_short_call_details(pruned_options)
    options_value_by_brokerage = get_options_value_by_brokerage(
        pruned_options,
        bull_put_spreads,
        bear_call_spreads,
    )
    return OptionsAndSpreads(
        all_options=all_options,
        pruned_options=pruned_options,
        short_calls=short_calls,
        box_spreads=box_spreads,
        bull_put_spreads=bull_put_spreads,
        bear_call_spreads=bear_call_spreads,
        iron_condors=iron_condors,
        bull_put_spreads_no_ic=bull_put_spreads_no_ic,
        bear_call_spreads_no_ic=bear_call_spreads_no_ic,
        synthetics=synthetics,
        options_value_by_brokerage=options_value_by_brokerage,
    )


def get_itm_df(opts: OptionsAndSpreads) -> pd.DataFrame:
    itm_df = opts.all_options.query("in_the_money == True")
    # Handle spreads where one leg is in the money
    for spread in itertools.chain(opts.bull_put_spreads, opts.bear_call_spreads):
        if (otm_leg := modify_otm_leg(spread.df)) is not None:
            itm_df = pd.concat([itm_df, otm_leg])
    return itm_df


def remove_zero_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, df.any()]


def generate_options_data():
    logger.info("Generating options data")
    opts = get_options_and_spreads()
    itm_df = get_itm_df(opts)
    expiration_values = get_expiration_values(itm_df)
    with contextlib.redirect_stdout(io.StringIO()) as output:
        with common.pandas_options():
            text_output(opts=opts, show_spreads=False)
            main_output = output.getvalue()
    data = OptionsData(
        opts=opts,
        bev=expiration_values,
        main_output=main_output,
        updated=datetime.now(),
    )
    common.walrus_db.db[REDIS_KEY] = pickle.dumps(data)


def get_options_data() -> typing.Optional[OptionsData]:
    db = common.walrus_db.db
    if REDIS_KEY not in db:
        logger.error("No options data found")
        return None
    return pickle.loads(db[REDIS_KEY])


def text_output(opts: typing.Optional[OptionsAndSpreads], show_spreads: bool):
    if not opts:
        opts = get_options_and_spreads()
    if len(otm_df := opts.pruned_options.query("in_the_money == False")):
        print("Out of the money")
        print(
            remove_zero_columns(
                otm_df.drop(
                    columns=[
                        "in_the_money",
                        "intrinsic_value",
                        "min_contract_price",
                        "profit_stock_price",
                    ]
                )
            ),
            "\n",
        )
    if len(itm_df := opts.pruned_options.query("in_the_money == True")):
        print("In the money")
        print(remove_zero_columns(itm_df.drop(columns="in_the_money")), "\n")
    itm_df = get_itm_df(opts)
    print(
        "Balances after in the money options assigned (includes spreads not shown above)"
    )
    after_assignment(itm_df)
    for broker in common.BROKERAGES:
        if broker in opts.pruned_options.index.get_level_values(0):
            print(f"{broker}")
            print(
                f"  Short put exposure: {short_put_exposure(opts.pruned_options, broker):.0f}"
            )
            print(
                f"  Total exercise value: {opts.pruned_options.xs(broker, level='account')['exercise_value'].sum():.0f}"
            )
            print(
                remove_zero_columns(opts.pruned_options.xs(broker, level="account")),  # type: ignore
                "\n",
            )
    if show_spreads:
        if opts.bull_put_spreads:
            print("Bull put spreads")
            for spread in opts.bull_put_spreads:
                summarize_spread(spread, "Bull Put")
        if opts.bear_call_spreads:
            print("Bear call spreads")
            for spread in opts.bear_call_spreads:
                summarize_spread(spread, "Bear Call")
        if opts.iron_condors:
            print("Iron condors")
            for ic in opts.iron_condors:
                summarize_iron_condor(ic)
        if opts.box_spreads:
            print("Box spreads")
            for box in opts.box_spreads:
                summarize_box(box)


if __name__ == "__main__":
    if (options_data := get_options_data()) is None:
        raise ValueError("No options data found")
    text_output(options_data.opts, show_spreads=True)
