#!/usr/bin/env python3
"""Plot finance graphs."""

import io
import subprocess
from functools import reduce

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from prefixed import Float

import amortize_pal
import common
import find_collar_options
import i_and_e
import margin_interest
from balance_etfs import get_desired_df

COLOR_GREEN = "DarkGreen"
COLOR_RED = "DarkRed"
HOMES = [
    "Mt Vernon",
    "Northlake",
    "Villa Maria",
]


def add_hline_current(
    fig,
    data,
    df_col,
    row,
    col,
    annotation_position="top left",
    secondary_y=False,
    precision=0,
):
    """Add hline to represent total."""
    current = data[[df_col]].iloc[-1][df_col]
    fig.add_hline(
        y=current,
        annotation_text=f"{current:,.{precision}f}",
        line_dash="dot",
        line_color="gray",
        annotation_position=annotation_position,
        row=row,
        col=col,
        secondary_y=secondary_y,
    )


def make_assets_breakdown_section(daily_df):
    """Make assets trend section."""
    columns = [
        "total",
        "total_real_estate",
        "total_no_homes",
        "total_retirement",
        "total_investing",
        "total_liquid",
    ]
    section = px.line(
        daily_df,
        x=daily_df.index,
        y=columns,
        title="Assets Breakdown",
        facet_col="variable",
        facet_col_wrap=2,
        category_orders={"variable": columns},
    )
    section.update_yaxes(matches=None, title_text="")
    section.update_yaxes(col=2, showticklabels=True)
    section.update_yaxes(col=1, title_text="USD")
    section.update_xaxes(title_text="", matches="x", showticklabels=True)
    section.update_traces(showlegend=False)
    # (0, 1) = total
    # (0, 2) = total_real_estate
    # (2, 1) = total_no_homes
    # (2, 2) = total_retirement
    # (1, 2) = total_liquid
    # (1, 1) = total_investing
    add_hline_current(section, daily_df, "total", 0, 1)
    add_hline_current(section, daily_df, "total_real_estate", 0, 2)
    add_hline_current(section, daily_df, "total_no_homes", 2, 1)
    add_hline_current(section, daily_df, "total_retirement", 2, 2)
    add_hline_current(section, daily_df, "total_investing", 1, 1)
    add_hline_current(section, daily_df, "total_liquid", 1, 2)
    return section


def get_investing_retirement_df(daily_df):
    """Get merged df with other investment accounts."""
    invret_cols = ["pillar2", "ira", "commodities", "etfs"]
    invret_df = daily_df[invret_cols]
    return invret_df


def make_investing_retirement_section(invret_df):
    """Make investing and retirement section."""
    section = px.line(
        invret_df,
        x=invret_df.index,
        y=invret_df.columns,
        facet_col="variable",
        facet_col_wrap=2,
        labels={"value": "USD"},
        title="Investing & Retirement",
    )
    section.update_xaxes(title_text="", matches="x", showticklabels=True)
    section.update_yaxes(title_text="")
    section.update_yaxes(matches=None)
    section.update_yaxes(col=2, showticklabels=True)
    section.update_yaxes(col=1, title_text="USD")
    section.update_traces(showlegend=False)
    add_hline_current(section, invret_df, "pillar2", 0, 1)
    add_hline_current(section, invret_df, "ira", 0, 2)
    add_hline_current(section, invret_df, "commodities", 1, 1)
    add_hline_current(section, invret_df, "etfs", 1, 2)
    return section


def make_real_estate_section(real_estate_df):
    """Line graph of real estate."""
    section = px.line(
        real_estate_df,
        x=real_estate_df.index,
        y=[x for x in real_estate_df.columns if "Percent" not in x],
        facet_col="variable",
        facet_col_wrap=2,
        labels={"value": "USD"},
        title="Real Estate",
    )
    section.update_xaxes(title_text="", matches="x", showticklabels=True)
    section.update_yaxes(title_text="")
    section.update_yaxes(matches=None)
    section.update_yaxes(col=2, showticklabels=True)
    section.update_yaxes(col=1, title_text="USD")
    section.update_traces(showlegend=False)
    add_hline_current(section, real_estate_df, "Mt Vernon Price", 3, 1)
    add_hline_current(section, real_estate_df, "Mt Vernon Rent", 3, 2)
    add_hline_current(section, real_estate_df, "Northlake Price", 2, 1)
    add_hline_current(section, real_estate_df, "Northlake Rent", 2, 2)
    add_hline_current(section, real_estate_df, "Villa Maria Price", 1, 1)
    add_hline_current(section, real_estate_df, "Villa Maria Rent", 1, 2)
    return section


def make_real_estate_profit_bar(real_estate_df):
    """Bar chart of real estate profit."""
    values = []
    percent = []
    cols = [f"{home} Price" for home in HOMES]
    for home in cols:
        values.append(
            real_estate_df.iloc[-1][home]
            - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]
        )
        percent.append(real_estate_df.iloc[-1][f"{home} Percent Change"])
    profit_bar = go.Bar(
        x=cols,
        y=values,
        marker_color=[COLOR_GREEN if x > 0 else COLOR_RED for x in values],
        text=[f"{Float(x):.2h}<br>{y:.2f}%" for x, y in zip(values, percent)],
    )
    return profit_bar


def make_real_estate_profit_bar_yearly(real_estate_df):
    """Bar chart of real estate profit yearly."""
    values = []
    percent = []
    cols = [f"{home} Price" for home in HOMES]
    for home in cols:
        time_diff = (
            real_estate_df[home].index[-1] - real_estate_df[home].first_valid_index()
        )
        value_diff = (
            real_estate_df.iloc[-1][home]
            - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]
        )
        percent_diff = real_estate_df.iloc[-1][f"{home} Percent Change"]
        values.append((value_diff / time_diff.days) * 365)
        percent.append((percent_diff / time_diff.days) * 365)
    profit_bar = go.Bar(
        x=cols,
        y=values,
        marker_color=[COLOR_GREEN if x > 0 else COLOR_RED for x in values],
        text=[f"{Float(x):.2h}<br>{y:.2f}%" for x, y in zip(values, percent)],
    )
    return profit_bar


def make_investing_allocation_section():
    """Make investing current and desired allocation pie graphs."""
    changes_section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "Current",
            "Desired",
        ),
        specs=[[{"type": "pie"}, {"type": "pie"}]],
    )
    dataframe = get_desired_df(0)

    # Current allocation
    labels = [
        "US Large Cap",
        "US Small Cap",
        "US Bonds",
        "International Equities",
        "Commodities",
    ]
    values = [
        dataframe.loc["US_LARGE_CAP"]["value"],
        dataframe.loc["US_SMALL_CAP"]["value"],
        dataframe.loc["US_BONDS"]["value"],
        dataframe.loc["INTERNATIONAL_EQUITIES"]["value"],
        dataframe.loc["COMMODITIES"]["value"],
    ]
    pie_total = go.Figure(data=[go.Pie(labels=labels, values=values)])
    for trace in pie_total.data:
        changes_section.add_trace(trace, row=1, col=1)

    # Desired allocation
    values = [
        dataframe.loc["US_LARGE_CAP"]["value"]
        + dataframe.loc["US_LARGE_CAP"]["usd_to_reconcile"],
        dataframe.loc["US_SMALL_CAP"]["value"]
        + dataframe.loc["US_SMALL_CAP"]["usd_to_reconcile"],
        dataframe.loc["US_BONDS"]["value"]
        + dataframe.loc["US_BONDS"]["usd_to_reconcile"],
        dataframe.loc["INTERNATIONAL_EQUITIES"]["value"]
        + dataframe.loc["INTERNATIONAL_EQUITIES"]["usd_to_reconcile"],
        dataframe.loc["COMMODITIES"]["value"]
        + dataframe.loc["COMMODITIES"]["usd_to_reconcile"],
    ]
    pie_total = go.Figure(data=[go.Pie(labels=labels, values=values)])
    for trace in pie_total.data:
        changes_section.add_trace(trace, row=1, col=2)

    changes_section.update_traces(textinfo="percent+value")
    changes_section.update_layout(title="Investing Allocation")
    return changes_section


def make_allocation_profit_section(daily_df, real_estate_df):
    """Make asset allocation and day changes section."""
    changes_section = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Asset Allocation",
            "Real Estate Change Since Purchase",
            "Real Estate Yearly Average Change Since Purchase",
        ),
        specs=[[{"type": "pie", "colspan": 2}, None], [{"type": "xy"}, {"type": "xy"}]],
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )

    # Pie chart breakdown of total
    labels = ["Investing", "Liquid", "Real Estate", "Retirement"]
    liquid = daily_df.iloc[-1]["total_liquid"]
    if liquid < 0:
        liquid = 0
    values = [
        daily_df.iloc[-1]["total_investing"],
        liquid,
        daily_df.iloc[-1]["total_real_estate"],
        daily_df.iloc[-1]["total_retirement"],
    ]
    pie_total = go.Figure(data=[go.Pie(labels=labels, values=values)])
    pie_total.update_layout(title="Asset Allocation", title_x=0.5)
    for trace in pie_total.data:
        changes_section.add_trace(trace, row=1, col=1)

    cols = [f"{home} Price" for home in HOMES]
    for home in cols:
        real_estate_df[f"{home} Percent Change"] = (
            (
                real_estate_df[home]
                - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]
            )
            / real_estate_df.loc[real_estate_df[home].first_valid_index(), home]
            * 100
        )
    changes_section.add_trace(make_real_estate_profit_bar(real_estate_df), row=2, col=1)
    changes_section.add_trace(
        make_real_estate_profit_bar_yearly(real_estate_df), row=2, col=2
    )
    changes_section.update_yaxes(row=2, col=1, title_text="USD")
    changes_section.update_traces(showlegend=False)
    changes_section.update_traces(
        row=1, col=1, textinfo="percent+label", textposition="inside"
    )
    return changes_section


def make_prices_section(prices_df):
    """Make section with prices graphs."""
    fig = px.line(
        prices_df,
        x=prices_df.index,
        y=prices_df.columns,
        facet_col="variable",
        facet_col_wrap=2,
        facet_col_spacing=0.04,
        title="Prices",
    )
    fig.update_yaxes(matches=None, title_text="")
    fig.update_yaxes(col=2, showticklabels=True)
    fig.update_yaxes(col=1, title_text="USD")
    fig.update_xaxes(title_text="", matches="x", showticklabels=True)
    fig.update_traces(showlegend=False)
    add_hline_current(fig, prices_df, "CHFUSD", 0, 1, precision=2)
    add_hline_current(fig, prices_df, "SGDUSD", 0, 2, precision=2)
    return fig


def make_interest_rate_section(interest_df):
    """Make interest rate section."""
    section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "Interest Rates",
            "IBKR Forex Margin Interest Comparison",
        ),
    )
    for trace in px.line(
        interest_df,
        x=interest_df.index,
        y=interest_df.columns,
    ).data:
        section.add_trace(trace, row=1, col=1)
    for trace in make_margin_comparison_chart().data:
        section.add_trace(trace, row=1, col=2)
    section.update_yaxes(title_text="Percent", col=1)
    section.update_xaxes(title_text="")
    return section


def load_ledger_equity_balance_df(ledger_balance_cmd):
    """Get dataframe of equity balance."""
    equity_balance_df = pd.read_csv(
        io.StringIO(subprocess.check_output(ledger_balance_cmd, shell=True, text=True)),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Equity Balance"],
    )
    equity_balance_latest_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(
                ledger_balance_cmd.replace(" reg ", " bal "), shell=True, text=True
            )
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Equity Balance"],
    )
    equity_balance_df = pd.concat([equity_balance_df, equity_balance_latest_df])
    equity_balance_df["30% Equity Balance"] = equity_balance_df["Equity Balance"] * 0.3
    equity_balance_df["50% Equity Balance"] = equity_balance_df["Equity Balance"] * 0.5
    return equity_balance_df


def load_loan_balance_df(ledger_loan_balance_cmd):
    """Get dataframe of margin loan balance."""
    loan_balance_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(ledger_loan_balance_cmd, shell=True, text=True)
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Loan Balance"],
    )
    loan_balance_latest_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(
                ledger_loan_balance_cmd.replace(" reg ", " bal "), shell=True, text=True
            )
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Loan Balance"],
    )
    loan_balance_df = pd.concat([loan_balance_df, loan_balance_latest_df])
    loan_balance_df.loc[loan_balance_df["Loan Balance"] > 0, "Loan Balance"] = 0
    return loan_balance_df


def load_margin_loan_df(ledger_loan_balance_cmd, ledger_balance_cmd):
    """Get dataframe of margin loan balance with equity balance."""
    loan_balance_df = load_loan_balance_df(ledger_loan_balance_cmd)
    equity_balance_df = load_ledger_equity_balance_df(ledger_balance_cmd)
    combined_df = equity_balance_df
    if len(loan_balance_df) > 0:
        combined_df = pd.merge(
            loan_balance_df.abs(),
            equity_balance_df,
            left_index=True,
            right_index=True,
            how="outer",
        )
    return combined_df.resample("D").last().interpolate().fillna(0).clip(lower=0)


def make_loan_section(range_func):
    """Make section with margin loans."""
    section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "Interactive Brokers",
            "Charles Schwab",
        ),
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )
    balance_df = load_margin_loan_df(
        ledger_loan_balance_cmd=amortize_pal.LEDGER_LOAN_BALANCE_HISTORY_IBKR,
        ledger_balance_cmd=amortize_pal.LEDGER_BALANCE_HISTORY_IBKR,
    )
    start, end = range_func(balance_df)
    balance_df = balance_df[start:end]
    for trace in px.line(
        balance_df,
        x=balance_df.index,
        y=balance_df.columns,
    ).data:
        section.add_trace(trace, row=1, col=1)
    balance_df = load_margin_loan_df(
        ledger_loan_balance_cmd=amortize_pal.LEDGER_LOAN_BALANCE_HISTORY_SCHWAB_NONPAL,
        ledger_balance_cmd=amortize_pal.LEDGER_BALANCE_HISTORY_SCHWAB_NONPAL,
    )
    start, end = range_func(balance_df)
    balance_df = balance_df[start:end]
    for trace in px.line(
        balance_df,
        x=balance_df.index,
        y=balance_df.columns,
    ).data:
        section.add_trace(trace, row=1, col=2)
    section.update_yaxes(title_text="USD", col=1)
    section.update_traces(row=1, col=2, showlegend=False)
    section.update_xaxes(title_text="")
    section.update_layout(title="Margin/Box Loans")
    return section


def make_change_section(daily_df, column, title):
    """Make section with change in different timespans."""
    changes_section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "Year Over Year",
            "Month Over Month",
        ),
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )
    for trace in make_total_bar_yoy(daily_df, column).data:
        trace.marker.color = [COLOR_GREEN if y > 0 else COLOR_RED for y in trace.y]
        changes_section.add_trace(trace, row=1, col=1)
    for trace in make_total_bar_mom(daily_df, column).data:
        trace.marker.color = [COLOR_GREEN if y > 0 else COLOR_RED for y in trace.y]
        changes_section.add_trace(trace, row=1, col=2)
    changes_section.update_yaxes(title_text="USD", col=1)
    changes_section.update_xaxes(title_text="")
    changes_section.update_xaxes(tickformat="%Y", row=1, col=1)
    changes_section.update_layout(title=title)
    return changes_section


def make_total_bar_mom(daily_df, column):
    """Make month over month total profit bar graphs."""
    diff_df = daily_df.resample("ME").last().interpolate().diff().dropna().iloc[-36:]
    monthly_bar = px.bar(diff_df, x=diff_df.index, y=column)
    return monthly_bar


def make_total_bar_yoy(daily_df, column):
    """Make year over year total profit bar graphs."""
    diff_df = daily_df.resample("YE").last().interpolate().diff().dropna()
    # Re-align at beginning of year.
    diff_df.index = pd.DatetimeIndex(diff_df.index.strftime("%Y-01-01"))
    yearly_bar = px.bar(diff_df, x=diff_df.index, y=column, text_auto=".3s")
    return yearly_bar


def make_margin_comparison_chart():
    """Make margin comparison bar chart."""
    dataframe = margin_interest.interest_comparison_df().abs()
    chart = px.histogram(
        dataframe,
        x=dataframe.index,
        y=dataframe.columns,
        barmode="group",
        title="IBKR Forex Margin Interest Comparison",
    )
    i_and_e.configure_monthly_chart(chart)
    return chart


def make_short_call_chart():
    """Make short call moneyness/loss bar chart."""
    section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "Short call option values (red = ITM)",
            "ITM exercise values",
        ),
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )
    dataframe = find_collar_options.options_df()
    short_calls_df = dataframe[(dataframe["type"] == "CALL") & (dataframe["count"] < 0)]
    chart = px.bar(
        short_calls_df,
        x=short_calls_df.index,
        y=["current_price_minus_strike"],
    )
    for trace in chart.data:
        trace.marker.color = [COLOR_GREEN if y > 0 else COLOR_RED for y in trace.y]
        section.add_trace(trace, row=1, col=1)
    itm_df = (
        dataframe[dataframe["in_the_money"]].sort_values("exercise_value").reset_index()
    )
    itm_df.loc[itm_df["count"] < 0, "name"] = itm_df["name"].astype(str) + " (SHORT)"
    itm_df.loc[itm_df["count"] > 0, "name"] = itm_df["name"].astype(str) + " (LONG)"
    itm_df = itm_df.set_index("name")
    chart = px.bar(
        itm_df,
        x=itm_df.index,
        y=["exercise_value"],
    )
    for trace in chart.data:
        trace.marker.color = [COLOR_GREEN if y > 0 else COLOR_RED for y in trace.y]
        section.add_trace(trace, row=1, col=2)
    section.update_yaxes(title_text="USD", col=1)
    section.update_xaxes(title_text="")
    section.update_layout(title="Options")
    section.update_traces(showlegend=False)
    return section


def get_interest_rate_df(frequency):
    """Merge interest rate data."""
    fedfunds_df = common.load_sqlite_and_rename_col(
        "fedfunds", rename_cols={"percent": "Fed Funds"}, frequency=frequency
    )["2019":]
    sofr_df = common.load_sqlite_and_rename_col(
        "sofr", rename_cols={"percent": "SOFR"}, frequency=frequency
    )["2019":]
    swvxx_df = common.load_sqlite_and_rename_col(
        "swvxx_yield", rename_cols={"percent": "Schwab SWVXX"}, frequency=frequency
    )
    wealthfront_df = common.load_sqlite_and_rename_col(
        "wealthfront_cash_yield",
        rename_cols={"percent": "Wealthfront Cash"},
        frequency=frequency,
    )
    ibkr_df = common.load_sqlite_and_rename_col(
        "interactive_brokers_margin_rates",
        rename_cols={"USD": "USD IBKR Margin", "CHF": "CHF IBKR Margin"},
        frequency=frequency,
    )
    merged = reduce(
        lambda l, r: pd.merge(l, r, left_index=True, right_index=True, how="outer"),
        [
            fedfunds_df,
            sofr_df,
            swvxx_df,
            wealthfront_df,
            ibkr_df,
        ],
    )
    return merged.interpolate()
