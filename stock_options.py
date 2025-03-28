#!/usr/bin/env python3
"""Methods for stock options."""

import io
import itertools
import re
import subprocess
import typing
from datetime import date, datetime

import pandas as pd
import yahooquery
from joblib import Parallel, delayed
from loguru import logger

import common
import etfs
import ledger_ops


class TickerOption(typing.NamedTuple):
    ticker: str
    expiration: date
    contract_type: str
    strike: float


class CommonDetails(typing.NamedTuple):
    account: str
    count: int
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


class BoxSpreadDetails(typing.NamedTuple):
    details: SpreadDetails
    earliest_transaction_date: date
    loan_term_days: int
    apy: float


class IronCondorDetails(typing.NamedTuple):
    details: CommonDetails
    low_put_strike: float
    high_put_strike: float
    low_call_strike: float
    high_call_strike: float
    risk: float


class OptionsAndSpreads(typing.NamedTuple):
    all_options: pd.DataFrame
    pruned_options: pd.DataFrame
    box_spreads: list[pd.DataFrame]
    iron_condors: list[pd.DataFrame]
    bull_put_spreads: list[pd.DataFrame]
    bear_call_spreads: list[pd.DataFrame]


class ExpirationValue(typing.NamedTuple):
    expiration: date
    value: float


class BrokerExpirationValues(typing.NamedTuple):
    broker: str
    values: list[ExpirationValue]


def get_option_chains(
    tickers: typing.Sequence[str],
) -> typing.Mapping[str, typing.Optional[pd.DataFrame]]:
    option_dfs = Parallel(n_jobs=-1)(delayed(get_option_chain)(t) for t in tickers)
    ticker_dict = {}
    for ticker, df in zip(tickers, option_dfs):
        ticker_dict[ticker] = df
    return ticker_dict


@common.cache_half_hourly_decorator
def get_option_chain(ticker: str) -> pd.DataFrame | None:
    """Get option chain for a ticker."""
    if ticker == "SPX":
        ticker = "^SPX"
    if ticker == "SMI":
        return None
    logger.info(f"Retrieving option chain for {ticker=}")
    if not isinstance(
        option_chain := yahooquery.Ticker(ticker).option_chain, pd.DataFrame
    ):
        logger.error(f"No option chain data found for {ticker=}")
        return None
    return option_chain


def get_ticker_option(
    t: TickerOption, option_chain: typing.Optional[pd.DataFrame]
) -> float | None:
    ticker = t.ticker
    option_tickers = [ticker]
    if ticker == "SPX":
        option_tickers.append(f"{ticker}W")
    if option_chain is None:
        return None
    for option_ticker in option_tickers:
        name = t.expiration.strftime(
            f"{option_ticker}%y%m%d{t.contract_type[0]}{int(t.strike * 1000):08}"
        )
        try:
            return option_chain.loc[lambda df: df["contractSymbol"] == name][
                "lastPrice"
            ].iloc[-1]
        except (IndexError, KeyError):
            pass
    return None


def options_df_raw(
    commodity_regex: str = "", additional_args: str = ""
) -> pd.DataFrame:
    search = " (CALL|PUT)"
    if commodity_regex:
        search = f"{commodity_regex} .*{search}"
    cmd = (
        f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/{search}/' {additional_args} "
        + 'bal --no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
    )
    chfusd = common.get_latest_forex()["CHFUSD"]
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
    option_chains = get_option_chains(tickers.tolist())
    prices = []
    for idx, row in options_df.iterrows():
        expiration = typing.cast(tuple, idx)[2].date()
        ticker = row["ticker"]
        price = get_ticker_option(
            TickerOption(
                ticker=ticker,
                expiration=expiration,
                contract_type=row["type"],
                strike=row["strike"],
            ),
            option_chains[ticker],
        )
        if price is None:
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
    chfusd = common.get_latest_forex()["CHFUSD"]
    if amount.startswith("$"):
        return float(amount[1:])
    elif amount.endswith(" CHF"):
        return float(amount.split()[0]) * chfusd
    return 0


def find_old_box_spreads(current_box_spreads: list[pd.DataFrame]) -> list[pd.DataFrame]:
    logger.info("Finding old box spreads")
    commodity_regex = "(SMI|SPX)"
    unassigned_df = options_df(
        commodity_regex=commodity_regex,
        additional_args=r"""--limit 'payee != "Options assignment"'""",
    )
    old_boxes = find_box_spreads(remove_spreads(unassigned_df, current_box_spreads))
    return old_boxes


def get_ledger_entries_command(broker: str, option_name: str) -> str:
    return (
        f"""{common.LEDGER_PREFIX} print expr 'any(commodity == "\\"{option_name}\\"" and account =~ /{broker}/)'"""
        """ --limit 'payee != "Options assignment"'"""
    )


def get_ledger_entries_command_ticker(broker: str, ticker: str) -> str:
    return (
        f"""{common.LEDGER_PREFIX} print expr 'any(commodity =~ /\\"{ticker} .* (CALL|PUT)/ and account =~ /{broker}/)'"""
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


def get_ledger_total_ticker(broker: str, ticker: str) -> float:
    logger.info(f"Getting ledger total for {broker=} {ticker=}")
    ledger_cmd = get_ledger_entries_command_ticker(broker, ticker)
    return get_ledger_total(broker, ticker, ledger_cmd)


def get_ledger_total(
    broker: str, option_name: str, ledger_cmd: typing.Optional[str] = None
) -> float:
    total = 0.0
    if not ledger_cmd:
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
    prices = []
    logger.info("Adding contract prices")
    for idx, row in options_df.iterrows():
        broker: str = typing.cast(tuple, idx)[0]
        name: str = typing.cast(tuple, idx)[1]
        prices.append(
            get_ledger_total(broker, name) / (row["count"] * row["multiplier"])
        )
    options_df["contract_price"] = prices
    logger.info("Finished adding contract prices")
    return options_df


def add_index_prices(etfs_df: pd.DataFrame) -> pd.DataFrame:
    index_df = common.read_sql_last("index_prices")
    for ticker, index_ticker in (("SPX", "^SPX"), ("SMI", "^SSMI")):
        etfs_df.loc[ticker, "current_price"] = index_df[index_ticker].iloc[-1]
    return etfs_df


@common.cache_half_hourly_decorator
def options_df(commodity_regex: str = "", additional_args: str = "") -> pd.DataFrame:
    """Get call and put dataframe."""
    calls_puts_df = options_df_raw(
        commodity_regex=commodity_regex, additional_args=additional_args
    )
    if not len(calls_puts_df):
        return calls_puts_df
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
    joined_df = add_value(joined_df)
    return joined_df


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


def find_bull_put_spreads(options_df: pd.DataFrame) -> list[pd.DataFrame]:
    """Find bull put spreads. Remove box spreads before calling."""
    dataframes = []
    for index, row in options_df.iterrows():
        ticker = row["ticker"]  # noqa: F841
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


def find_bear_call_spreads(options_df: pd.DataFrame) -> list[pd.DataFrame]:
    """Find bear call spreads. Remove box spreads before calling."""
    dataframes = []
    for index, row in options_df.iterrows():
        ticker = row["ticker"]  # noqa: F841
        # Find a long CALL
        if row["type"] == "CALL" and row["count"] > 0:
            # The long CALL
            high_long_call = options_df.query(
                'ticker == @ticker & type == "CALL" & strike == @row["strike"] & expiration == @index[2] & account == @index[0] & count > 0'
            )
            # Find a short CALL at lower strike, same expiration and broker
            low_short_call = options_df.query(
                'ticker == @ticker & type == "CALL" & strike < @row["strike"] & expiration == @index[2] & account == @index[0] & count < 0'
            )
            found = pd.concat([low_short_call, high_long_call])
            if len(found) == 2:
                dataframes.append(found)
    return dataframes


def find_iron_condors(
    bull_put_spreads: list[pd.DataFrame], bear_call_spreads: list[pd.DataFrame]
) -> list[pd.DataFrame]:
    dataframes = []
    for bull_put_df in bull_put_spreads:
        for bear_call_df in bear_call_spreads:
            low_long_put = bull_put_df.query('type == "PUT" & count > 0')
            ticker = low_long_put["ticker"].iloc[0]  # noqa: F841
            expiration = low_long_put.index.get_level_values("expiration")[0]  # noqa: F841
            account = low_long_put.index.get_level_values("account")[0]  # noqa: F841
            high_short_put = bull_put_df.query('type == "PUT" & count < 0')
            high_short_put_strike = high_short_put["strike"].max()  # noqa: F841
            matcher = (
                "account == @account & ticker == @ticker & expiration == @expiration"
            )
            high_long_call = bear_call_df.query(
                f'{matcher} & type == "CALL" & count > 0 & strike > @high_short_put_strike'
            )
            high_long_call_strike = high_long_call["strike"].max()  # noqa: F841
            low_short_call = bear_call_df.query(
                f'{matcher} & type == "CALL" & count < 0 & strike < @high_long_call_strike'
            )
            found = pd.concat(
                [low_long_put, high_short_put, low_short_call, high_long_call]
            )
            if len(found) == 4:
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


def get_short_call_details(options_df: pd.DataFrame) -> list[ShortCallDetails]:
    short_calls = options_df.query("type == 'CALL' & count < 0")
    short_call_details = []
    for index, row in short_calls.iterrows():
        index = typing.cast(tuple, index)
        contract_price = row["contract_price"] * row["count"] * row["multiplier"]
        short_call_details.append(
            ShortCallDetails(
                details=CommonDetails(
                    account=index[0],
                    count=row["count"],
                    ticker=row["ticker"],
                    ticker_price=row["current_price"],
                    expiration=index[2].date(),
                    contract_price=contract_price,
                    contract_price_per_share=row["contract_price"],
                    half_mark=row["contract_price"] / 2,
                    double_mark=row["contract_price"] * 2,
                    quote=row["value"],
                    profit=row["profit_option_value"],
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
            count=count,
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
    # Find adjustments and update contract price
    if (all_trades_total := get_ledger_total_ticker(account, ticker)) != 0:
        contract_price = all_trades_total
    half_mark = contract_price / count / multiplier / 2
    double_mark = contract_price / count / multiplier * 2
    quote = iron_condor_df["value"].sum()
    risk = -((width * count * multiplier) + contract_price)
    return IronCondorDetails(
        details=CommonDetails(
            account=account,
            count=count,
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


def summarize_iron_condor(iron_condor_df: pd.DataFrame):
    d = get_iron_condor_details(iron_condor_df)
    cd = d.details
    print(f"{cd.account}")
    print(
        f"{cd.count} {cd.ticker} {cd.expiration} {d.low_put_strike:.0f}/{d.high_put_strike:.0f}/{d.low_call_strike:.0f}/{d.high_call_strike:.0f} Iron Condor"
    )
    print(f"Contract price: {cd.contract_price:.0f}")
    print(f"Half mark: {cd.half_mark:.2f}")
    print(f"Double mark: {cd.double_mark:.2f}")
    print(f"Maximum risk: {d.risk:.0f}")
    print(f"Ticker price: {cd.ticker_price}")
    print(f"Profit: {cd.profit:.0f}\n")


def summarize_box(box_df: pd.DataFrame):
    d = get_box_spread_details(box_df)
    cd = d.details.details
    print(f"{cd.account}")
    print(
        f"{cd.count} {cd.ticker} {cd.expiration} {d.details.low_strike:.0f}/{d.details.high_strike:.0f} Box"
    )
    print(f"Earliest transaction date: {d.earliest_transaction_date}")
    print(f"Loan term: {d.loan_term_days} days")
    print(f"APY: {d.apy:.2%}")
    print(f"Contract price: {cd.contract_price:.0f}")
    print(f"Exercise value: {cd.intrinsic_value:.0f}", end="")
    if cd.count > 1:
        print(f" ({cd.intrinsic_value / cd.count:.0f} per contract)", end="")
    print("\n")


def modify_otm_leg(spread_df: pd.DataFrame) -> typing.Optional[pd.DataFrame]:
    leg = spread_df.query("in_the_money == False").copy()
    if len(leg) == 1:
        leg["exercise_value"] = leg["count"] * leg["multiplier"] * leg["current_price"]
        return leg
    return None


def summarize_spread(spread_df: pd.DataFrame, title: str):
    d = get_spread_details(spread_df)
    cd = d.details
    print(f"{cd.account}")
    print(
        f"{cd.count} {cd.ticker} {cd.expiration} {d.low_strike:.0f}/{d.high_strike:.0f} {title}"
    )
    total = spread_df.query("in_the_money == True")["exercise_value"].sum()
    if (otm_leg := modify_otm_leg(spread_df)) is not None:
        total += otm_leg["exercise_value"].sum()
    print(f"Contract price: {cd.contract_price:.0f}")
    print(f"Half mark: {cd.half_mark:.2f}")
    print(f"Double mark: {cd.double_mark:.2f}")
    print(f"Exercise value: {total:.0f}")
    print(f"Maximum risk: {spread_df['exercise_value'].sum():.0f}")
    print(f"Ticker price: {cd.ticker_price}")
    print(f"Profit: {cd.profit:.0f}\n")


def get_options_and_spreads() -> OptionsAndSpreads:
    all_options = options_df()
    box_spreads = find_box_spreads(all_options)
    pruned_options = remove_spreads(all_options, box_spreads)
    bull_put_spreads = find_bull_put_spreads(pruned_options)
    pruned_options = remove_spreads(pruned_options, bull_put_spreads)
    bear_call_spreads = find_bear_call_spreads(pruned_options)
    pruned_options = remove_spreads(pruned_options, bear_call_spreads)
    iron_condors = find_iron_condors(bull_put_spreads, bear_call_spreads)
    # Do not prune iron condors
    return OptionsAndSpreads(
        all_options=all_options,
        pruned_options=pruned_options,
        box_spreads=box_spreads,
        bull_put_spreads=bull_put_spreads,
        bear_call_spreads=bear_call_spreads,
        iron_condors=iron_condors,
    )


def get_itm_df(opts: OptionsAndSpreads) -> pd.DataFrame:
    itm_df = opts.all_options.query("in_the_money == True")
    # Handle spreads where one leg is in the money
    for spread in itertools.chain(opts.bull_put_spreads, opts.bear_call_spreads):
        if (otm_leg := modify_otm_leg(spread)) is not None:
            itm_df = pd.concat([itm_df, otm_leg])
    return itm_df


def remove_zero_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, df.any()]


def main(show_spreads: bool = True, opts: typing.Optional[OptionsAndSpreads] = None):
    """Main."""
    if opts is None:
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
    main()
