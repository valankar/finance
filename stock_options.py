#!/usr/bin/env python3
"""Methods for stock options."""

import csv
import io
import re
import subprocess
import typing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

import common
import ledger_ops


@dataclass
class CommonDetails:
    account: str
    amount: int
    ticker: str
    ticker_price: float
    expiration: date
    contract_price: float
    contract_price_per_share: float
    quote: float
    profit: float
    intrinsic_value: float
    notional_value: float


@dataclass
class OptionDetails:
    details: CommonDetails
    option_type: typing.Literal["CALL", "PUT"]
    strike: float
    profit_stock: float
    df: pd.DataFrame


@dataclass
class SpreadDetails:
    details: CommonDetails
    low_strike: float
    high_strike: float
    delta: float


@dataclass
class Spread:
    df: pd.DataFrame
    details: SpreadDetails


@dataclass
class BoxSpreadDetails:
    details: SpreadDetails
    earliest_transaction_date: date
    loan_term_days: int
    apy: float


@dataclass
class BoxSpread:
    df: pd.DataFrame
    details: BoxSpreadDetails


@dataclass
class IronCondorDetails:
    details: CommonDetails
    low_put_strike: float
    high_put_strike: float
    low_call_strike: float
    high_call_strike: float
    risk: float


@dataclass
class IronCondor:
    df: pd.DataFrame
    details: IronCondorDetails


@dataclass
class OptionsValue:
    value: float
    notional_value: float


@dataclass
class OptionsAndSpreads:
    all_options: pd.DataFrame
    # Options outside of groups.
    pruned_options: pd.DataFrame
    short_options: list[OptionDetails]
    long_options: list[OptionDetails]
    box_spreads: list[BoxSpread]
    iron_condors: list[IronCondor]
    # These include spreads part of iron condors.
    bull_put_spreads: list[Spread]
    bear_call_spreads: list[Spread]
    bull_call_spreads: list[Spread]
    synthetics: list[Spread]
    strangles: list[Spread]

    def get_all_without_box_spreads(self) -> pd.DataFrame:
        return remove_spreads_with_quantities(self.all_options, self.box_spreads)

    def get_options_value_by_brokerage(self) -> dict[str, OptionsValue]:
        values: dict[str, OptionsValue] = {}
        for broker in common.OPTIONS_BROKERAGES:
            options_value = self.all_options.query(f"account == '{broker}'")[
                "value"
            ].sum()
            options_notional_value = (
                self.get_all_without_box_spreads()
                .query(f"account == '{broker}'")["notional_value"]
                .sum()
            )
            values[broker] = OptionsValue(
                value=options_value, notional_value=options_notional_value
            )
        return values


def options_df_raw() -> pd.DataFrame:
    search = ' (CALL|PUT)"'
    cmd = (
        f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/{search}/' "
        + 'bal --no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
    )
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
            multiplier = 10 * common.get_tickers(["CHFUSD"])["CHFUSD"]
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


def get_ticker_option(idx, row) -> common.TickerOption:
    expiration = typing.cast(tuple, idx)[2].date()
    ticker = row["ticker"]
    return common.TickerOption(
        ticker=ticker,
        expiration=expiration,
        contract_type=row["type"],
        strike=row["strike"],
    )


def add_options_quotes(options_df: pd.DataFrame):
    tickers = options_df["ticker"].unique()
    if not len(tickers):
        return options_df
    prices = []
    deltas = []
    underlying_prices = []
    tos: set[common.TickerOption] = set()
    for idx, row in options_df.iterrows():
        tos.add(get_ticker_option(idx, row))
    qs = common.get_option_quotes(tos)
    for idx, row in options_df.iterrows():
        if quote := qs.get(get_ticker_option(idx, row)):
            prices.append(quote.mark)
            deltas.append(quote.delta)
            underlying_prices.append(quote.underlying_price)
        else:
            raise ValueError(f"No option quote found for {idx=} {row=}")
    options_df["quote"] = prices
    options_df["delta"] = deltas
    options_df["current_price"] = underlying_prices
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


def add_value(df: pd.DataFrame) -> pd.DataFrame:
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
        return float(amount.split()[0]) * common.get_tickers(["CHFUSD"])["CHFUSD"]
    return 0


def get_ledger_entries_command(broker: str, option_name: str) -> str:
    return (
        f"""{common.LEDGER_PREFIX} print expr 'any(commodity == "{option_name}" and account =~ /{broker}/)'"""
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


def get_trade_price_override(broker: str, option_name: str) -> float:
    if (p := Path(f"{common.PUBLIC_HTML}options_trade_price_overrides")).exists():
        with p.open("r") as f:
            reader = csv.reader(f, delimiter=":")
            for row in reader:
                if row[0] == broker and row[1] == option_name:
                    return float(row[2])
    return 0


def add_contract_price(options_df: pd.DataFrame) -> pd.DataFrame:
    def get_contract_price(
        broker: str, name: str, count: int, multiplier: int
    ) -> float:
        divisor = count * multiplier
        if override := get_trade_price_override(broker, name):
            return override
        return get_ledger_total(broker, name) / divisor

    with ThreadPoolExecutor() as executor:
        futures = []
        for idx, row in options_df.iterrows():
            broker: str = typing.cast(tuple, idx)[0]
            name: str = typing.cast(tuple, idx)[1]
            futures.append(
                executor.submit(
                    get_contract_price, broker, name, row["count"], row["multiplier"]
                )
            )
        prices = [f.result() for f in futures]

    options_df["contract_price"] = prices
    return options_df


def options_df() -> pd.DataFrame:
    """Get call and put dataframe."""
    df = options_df_raw()
    if not len(df):
        return df
    df = df.set_index(["account", "name", "expiration"])
    df = add_options_quotes(df)
    df["in_the_money"] = False
    df.loc[df["type"] == "CALL", "in_the_money"] = df["strike"] < df["current_price"]
    df.loc[df["type"] == "PUT", "in_the_money"] = df["strike"] > df["current_price"]
    df["exercise_value"] = df["strike"] * df["count"] * df["multiplier"]
    df.loc[df["type"] == "CALL", "exercise_value"] = -df["exercise_value"]
    df["intrinsic_value"] = 0.0
    df.loc[
        (df["type"] == "CALL") & df["in_the_money"],
        "intrinsic_value",
    ] = (df["current_price"] - df["strike"]) * df["count"] * df["multiplier"]
    df.loc[
        (df["type"] == "PUT") & df["in_the_money"],
        "intrinsic_value",
    ] = (df["strike"] - df["current_price"]) * df["count"] * df["multiplier"]
    df = df.sort_values(["account", "expiration", "name"])
    df = add_contract_price(df)
    df = add_value(df)
    return df


def short_put_exposure(dataframe: pd.DataFrame, broker: str):
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


def _query_spread_leg(
    options_df: pd.DataFrame,
    ticker: str,
    option_type: str,
    strike_cond: str,
    expiration: pd.Timestamp,
    account: str,
    count_cond: str,
) -> pd.DataFrame:
    """Query for a spread leg matching the given criteria."""
    # Use eval for conditions, but direct comparison for index levels
    # First filter by index levels
    filtered = options_df[
        (options_df.index.get_level_values("account") == account)
        & (options_df.index.get_level_values("expiration") == expiration)
    ]
    # Then apply remaining query conditions
    query_str = (
        f'ticker == "{ticker}" & type == "{option_type}" & {strike_cond} & {count_cond}'
    )
    return filtered.query(query_str)


def _create_spread(
    leg1_df: pd.DataFrame,
    leg2_df: pd.DataFrame,
    normalize_counts: bool = False,
) -> typing.Optional[Spread]:
    """Create a Spread from two leg DataFrames if both exist."""
    found = pd.concat([leg1_df, leg2_df])
    if len(found) != 2:
        return None

    if normalize_counts:
        min_count = found["count"].abs().min()
        # Store original counts before modifying
        found = found.copy()
        found["_original_count"] = found["count"].copy()
        found.loc[found.index[0], "count"] = min_count
        found.loc[found.index[1], "count"] = -min_count
        # Recalculate notional_value based on new count: delta * strike * count * multiplier
        found["notional_value"] = (
            found["delta"] * found["strike"] * found["count"] * found["multiplier"]
        )
        # Recalculate value based on new count: count * quote * multiplier
        found["value"] = found["count"] * found["quote"] * found["multiplier"]
        # Recalculate profit_option_value: value - (contract_price * count * multiplier)
        found["profit_option_value"] = found["value"] - (
            found["contract_price"] * found["count"] * found["multiplier"]
        )

    return Spread(df=found, details=get_spread_details(found))


def find_synthetics(options_df: pd.DataFrame) -> list[Spread]:
    """Find synthetic positions: Long CALL + Short PUT or Long PUT + Short CALL at same strike."""
    spreads: list[Spread] = []

    for idx, row in options_df.iterrows():
        account, name, expiration = typing.cast(tuple, idx)
        ticker = row["ticker"]
        if ticker in ("SPX", "SPXW"):
            continue

        if row["type"] == "CALL" and row["count"] > 0:
            # Long CALL + Short PUT at same strike
            leg1 = _query_spread_leg(
                options_df,
                ticker,
                "CALL",
                f"strike == {row['strike']}",
                expiration,
                account,
                f"count == {row['count']}",
            )
            leg2 = _query_spread_leg(
                options_df,
                ticker,
                "PUT",
                f"strike == {row['strike']}",
                expiration,
                account,
                f"count < 0 & count >= {-row['count']}",
            )
            if spread := _create_spread(leg1, leg2, normalize_counts=True):
                spreads.append(spread)

        elif row["type"] == "PUT" and row["count"] > 0:
            # Long PUT + Short CALL at same strike
            leg1 = _query_spread_leg(
                options_df,
                ticker,
                "PUT",
                f"strike == {row['strike']}",
                expiration,
                account,
                f"count == {row['count']}",
            )
            leg2 = _query_spread_leg(
                options_df,
                ticker,
                "CALL",
                f"strike == {row['strike']}",
                expiration,
                account,
                f"count < 0 & count >= {-row['count']}",
            )
            if spread := _create_spread(leg1, leg2, normalize_counts=True):
                spreads.append(spread)

    return spreads


def find_bull_put_spreads(options_df: pd.DataFrame) -> list[Spread]:
    """Find bull put spreads: Long PUT at lower strike + Short PUT at higher strike."""
    spreads: list[Spread] = []

    for idx, row in options_df.iterrows():
        account, name, expiration = typing.cast(tuple, idx)
        if row["type"] != "PUT" or row["count"] <= 0:
            continue

        ticker = row["ticker"]
        # Long PUT (identity leg)
        leg1 = _query_spread_leg(
            options_df,
            ticker,
            "PUT",
            f"strike == {row['strike']}",
            expiration,
            account,
            f"count == {row['count']}",
        )
        # Short PUT at higher strike
        leg2 = _query_spread_leg(
            options_df,
            ticker,
            "PUT",
            f"strike > {row['strike']}",
            expiration,
            account,
            f"count == {-row['count']}",
        )
        if spread := _create_spread(leg1, leg2):
            spreads.append(spread)

    return spreads


def find_bear_call_spreads(options_df: pd.DataFrame) -> list[Spread]:
    """Find bear call spreads: Short CALL at lower strike + Long CALL at higher strike."""
    spreads: list[Spread] = []

    for idx, row in options_df.iterrows():
        account, name, expiration = typing.cast(tuple, idx)
        if row["type"] != "CALL" or row["count"] <= 0:
            continue

        ticker = row["ticker"]
        # Short CALL at lower strike
        leg1 = _query_spread_leg(
            options_df,
            ticker,
            "CALL",
            f"strike < {row['strike']}",
            expiration,
            account,
            f"count == {-row['count']}",
        )
        # Long CALL (identity leg)
        leg2 = _query_spread_leg(
            options_df,
            ticker,
            "CALL",
            f"strike == {row['strike']}",
            expiration,
            account,
            f"count == {row['count']}",
        )
        if spread := _create_spread(leg1, leg2):
            spreads.append(spread)

    return spreads


def find_bull_call_spreads(options_df: pd.DataFrame) -> list[Spread]:
    """Find bull call spreads: Long CALL at lower strike + Short CALL at higher strike."""
    spreads: list[Spread] = []

    for idx, row in options_df.iterrows():
        account, name, expiration = typing.cast(tuple, idx)
        if row["type"] != "CALL" or row["count"] <= 0:
            continue

        ticker = row["ticker"]
        # Long CALL (identity leg)
        leg1 = _query_spread_leg(
            options_df,
            ticker,
            "CALL",
            f"strike == {row['strike']}",
            expiration,
            account,
            f"count == {row['count']}",
        )
        # Short CALL at higher strike - can have multiple matches
        leg2_candidates = _query_spread_leg(
            options_df,
            ticker,
            "CALL",
            f"strike > {row['strike']}",
            expiration,
            account,
            f"count == {-row['count']}",
        )

        for i in range(len(leg2_candidates)):
            leg2 = leg2_candidates.iloc[i : i + 1]
            if spread := _create_spread(leg1, leg2):
                spreads.append(spread)

    return spreads


def find_strangles(options_df: pd.DataFrame) -> list[Spread]:
    """Find strangles: Short CALL + Short PUT at lower strike."""
    spreads: list[Spread] = []

    for idx, row in options_df.iterrows():
        account, name, expiration = typing.cast(tuple, idx)
        if row["type"] != "CALL" or row["count"] >= 0:
            continue

        ticker = row["ticker"]
        # Short CALL (identity leg)
        leg1 = _query_spread_leg(
            options_df,
            ticker,
            "CALL",
            f"strike == {row['strike']}",
            expiration,
            account,
            f"count == {row['count']}",
        )
        # Short PUT at lower strike
        leg2 = _query_spread_leg(
            options_df,
            ticker,
            "PUT",
            f"strike < {row['strike']}",
            expiration,
            account,
            f"count == {row['count']}",
        )
        if spread := _create_spread(leg1, leg2):
            spreads.append(spread)

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
        if ticker in ("SPX", "SPXW", "SMI"):
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


def remove_spreads_with_quantities(
    options_df: pd.DataFrame, spreads: typing.Sequence[typing.Any]
) -> pd.DataFrame:
    """Remove options used in spreads/spreads, keeping excess quantities as separate rows.

    This properly handles cases where a spread uses only part of an option position,
    leaving the remainder in the pruned options.
    """
    if not spreads or options_df.empty:
        return options_df

    # Track how much of each option index is used by spreads
    usage: dict[tuple, int] = {}
    for spread in spreads:
        df = spread.df if hasattr(spread, "df") else spread.df
        for idx, row in df.iterrows():
            key = tuple(idx) if isinstance(idx, tuple) else (idx,)
            count = abs(row["count"])
            usage[key] = usage.get(key, 0) + count

    # Build new DataFrame with adjusted quantities
    new_rows = []
    new_index = []
    for idx, row in options_df.iterrows():
        key = tuple(idx) if isinstance(idx, tuple) else (idx,)
        original_count = row["count"]
        used_count = usage.get(key, 0)

        # Determine remaining count based on sign
        if original_count > 0:
            remaining_count = original_count - used_count
        else:
            remaining_count = original_count + used_count

        if remaining_count == 0:
            continue  # Fully used, skip

        # Create adjusted row
        new_row = row.copy()
        new_row["count"] = remaining_count

        # Recalculate all derived values based on new count
        ratio = remaining_count / original_count
        new_row["value"] = row["value"] * ratio
        new_row["notional_value"] = row["notional_value"] * ratio
        new_row["intrinsic_value"] = row["intrinsic_value"] * ratio
        new_row["exercise_value"] = row["exercise_value"] * ratio
        new_row["profit_stock_price"] = row["profit_stock_price"] * ratio
        new_row["profit_option_value"] = row["profit_option_value"] * ratio

        new_rows.append(new_row)
        new_index.append(idx)

    if not new_rows:
        return options_df.iloc[0:0]  # Return empty DataFrame with same columns

    result = pd.DataFrame(new_rows)
    # Preserve the original index
    result.index = pd.MultiIndex.from_tuples(new_index, names=options_df.index.names)
    return result


def get_option_details(
    options_df: pd.DataFrame, option_type: typing.Literal["long", "short"]
) -> list[OptionDetails]:
    match option_type:
        case "long":
            q = "count > 0"
        case "short":
            q = "count < 0"
    options = options_df.query(q)
    option_details = []
    for i in range(len(options)):
        row = options.iloc[i]
        account = row.name[0]  # type: ignore
        expiration = row.name[2].date()  # type: ignore
        contract_price = row["contract_price"] * row["count"] * row["multiplier"]
        contract_price_per_share = row["contract_price"]
        option_details.append(
            OptionDetails(
                details=CommonDetails(
                    account=account,
                    amount=row["count"],
                    ticker=row["ticker"],
                    ticker_price=row["current_price"],
                    expiration=expiration,
                    contract_price=contract_price,
                    contract_price_per_share=contract_price_per_share,
                    quote=row["value"],
                    profit=row["value"] - contract_price,
                    intrinsic_value=row["intrinsic_value"],
                    notional_value=row["notional_value"],
                ),
                option_type=row["type"],
                strike=row["strike"],
                profit_stock=row["profit_stock_price"],
                df=options.iloc[[i]],
            )
        )
    return option_details


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
    row = spread_df.iloc[0]
    index = spread_df.index[0]
    c = spread_df.copy()
    c["price"] = c["contract_price"] * c["count"] * c["multiplier"]
    contract_price = c["price"].sum()
    quote = spread_df["value"].sum()
    delta = sum(spread_df["delta"] * (spread_df["count"] / abs(spread_df["count"])))
    return SpreadDetails(
        details=CommonDetails(
            account=index[0],
            amount=count,
            ticker=row["ticker"],
            ticker_price=row["current_price"],
            expiration=index[2].date(),
            contract_price=contract_price,
            contract_price_per_share=c["contract_price"].sum(),
            intrinsic_value=spread_df["intrinsic_value"].sum(),
            notional_value=spread_df["notional_value"].sum(),
            quote=quote,
            profit=quote - contract_price,
        ),
        low_strike=low_strike,
        high_strike=high_strike,
        delta=delta,
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
            intrinsic_value=iron_condor_df["intrinsic_value"].sum(),
            notional_value=iron_condor_df["notional_value"].sum(),
            quote=quote,
            profit=quote - contract_price,
        ),
        low_put_strike=low_put_strike,
        high_put_strike=high_put_strike,
        low_call_strike=low_call_strike,
        high_call_strike=high_call_strike,
        risk=risk,
    )


@common.walrus_db.db.lock("get_options_and_spreads", ttl=common.LOCK_TTL_SECONDS * 1000)
@common.walrus_db.cache.cached()
def get_options_and_spreads() -> OptionsAndSpreads:
    all_options = options_df()
    box_spreads = find_box_spreads(all_options)
    pruned_options = remove_spreads_with_quantities(all_options, box_spreads)
    bull_put_spreads = find_bull_put_spreads(pruned_options)
    pruned_options = remove_spreads_with_quantities(pruned_options, bull_put_spreads)
    bear_call_spreads = find_bear_call_spreads(pruned_options)
    pruned_options = remove_spreads_with_quantities(pruned_options, bear_call_spreads)
    iron_condors = find_iron_condors(bull_put_spreads, bear_call_spreads)
    bull_call_spreads = find_bull_call_spreads(pruned_options)
    pruned_options = remove_spreads_with_quantities(pruned_options, bull_call_spreads)
    synthetics = find_synthetics(pruned_options)
    pruned_options = remove_spreads_with_quantities(pruned_options, synthetics)
    strangles = find_strangles(pruned_options)
    pruned_options = remove_spreads_with_quantities(pruned_options, strangles)
    short_options = get_option_details(pruned_options, "short")
    pruned_options = remove_spreads_with_quantities(pruned_options, short_options)
    long_options = get_option_details(pruned_options, "long")
    pruned_options = remove_spreads_with_quantities(pruned_options, long_options)
    return OptionsAndSpreads(
        all_options=all_options,
        pruned_options=pruned_options,
        short_options=short_options,
        long_options=long_options,
        box_spreads=box_spreads,
        bull_put_spreads=bull_put_spreads,
        bear_call_spreads=bear_call_spreads,
        iron_condors=iron_condors,
        bull_call_spreads=bull_call_spreads,
        synthetics=synthetics,
        strangles=strangles,
    )
