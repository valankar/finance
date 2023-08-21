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

import common
from common import reduce_merge_asof

COLOR_GREEN = "DarkGreen"
COLOR_RED = "DarkRed"
HOMES = [
    "Mt Vernon",
    "Northlake",
    "Villa Maria",
]

LEDGER_IBONDS_CMD = (
    f'{common.LEDGER_PREFIX} -J -D reg ^"Assets:Investments:Treasury Direct"'
)


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
    ibonds_df = pd.read_csv(
        io.StringIO(subprocess.check_output(LEDGER_IBONDS_CMD, shell=True, text=True)),
        delim_whitespace=True,
        index_col=0,
        parse_dates=True,
        names=["date", "ibonds"],
    )
    return reduce_merge_asof([invret_df, ibonds_df])


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
    add_hline_current(section, invret_df, "pillar2", 3, 1)
    add_hline_current(section, invret_df, "ira", 3, 2)
    add_hline_current(section, invret_df, "commodities", 2, 1)
    add_hline_current(section, invret_df, "etfs", 2, 2)
    add_hline_current(section, invret_df, "ibonds", 1, 1)
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


def make_profit_bar(invret_df):
    """Profit from cost basis."""
    commodities_df = pd.read_csv(
        f"{common.PREFIX}commodities_values.csv", index_col="commodity"
    )
    commodities_gold_cost_basis = common.load_float_from_text_file(
        f"{common.PREFIX}commodities_gold_cost_basis.txt"
    )
    commodities_gold_profit = (
        commodities_df.loc["GOLD"]["value"] - commodities_gold_cost_basis
    )
    commodities_silver_cost_basis = common.load_float_from_text_file(
        f"{common.PREFIX}commodities_silver_cost_basis.txt"
    )
    commodities_silver_profit = (
        commodities_df.loc["SILVER"]["value"] - commodities_silver_cost_basis
    )
    commodities_cost_basis = commodities_gold_cost_basis + commodities_silver_cost_basis
    commodities_profit = (
        invret_df[["commodities"]].iloc[-1]["commodities"] - commodities_cost_basis
    )
    etfs_cost_basis = common.load_float_from_text_file(
        f"{common.PREFIX}schwab_etfs_cost_basis.txt"
    )
    etfs_profit = invret_df[["etfs"]].iloc[-1]["etfs"] - etfs_cost_basis

    ibonds_cost_basis = common.load_float_from_text_file(
        f"{common.PREFIX}treasury_direct_cost_basis.txt"
    )
    ibonds_profit = invret_df[["ibonds"]].iloc[-1]["ibonds"] - ibonds_cost_basis

    values = [
        commodities_profit,
        commodities_gold_profit,
        commodities_silver_profit,
        etfs_profit,
        ibonds_profit,
    ]
    percent = [
        commodities_profit / commodities_cost_basis * 100,
        commodities_gold_profit / commodities_gold_cost_basis * 100,
        commodities_silver_profit / commodities_silver_cost_basis * 100,
        etfs_profit / etfs_cost_basis * 100,
        ibonds_profit / ibonds_cost_basis * 100,
    ]
    profit_bar = go.Figure(
        go.Bar(
            x=["Commodities", "Gold", "Silver", "ETFs", "I Bonds"],
            y=values,
            marker_color=[COLOR_GREEN if x >= 0 else COLOR_RED for x in values],
            text=[f"{Float(x):.2h}<br>{y:.2f}%" for x, y in zip(values, percent)],
        )
    )
    return profit_bar


def make_allocation_profit_section(daily_df, invret_df, real_estate_df):
    """Make asset allocation and day changes section."""
    changes_section = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Asset Allocation",
            "Investing Profit or Loss (cost-based)",
            "Real Estate Change Since Purchase",
            "Real Estate Yearly Average Change Since Purchase",
        ),
        specs=[[{"type": "pie"}, {"type": "xy"}], [{"type": "xy"}, {"type": "xy"}]],
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )

    # Pie chart breakdown of total
    labels = ["Investing", "Liquid", "Real Estate", "Retirement"]
    values = [
        daily_df.iloc[-1]["total_investing"],
        daily_df.iloc[-1]["total_liquid"],
        daily_df.iloc[-1]["total_real_estate"],
        daily_df.iloc[-1]["total_retirement"],
    ]
    pie_total = go.Figure(data=[go.Pie(labels=labels, values=values)])
    pie_total.update_layout(title="Asset Allocation", title_x=0.5)
    for trace in pie_total.data:
        changes_section.add_trace(trace, row=1, col=1)

    for trace in make_profit_bar(invret_df).data:
        changes_section.add_trace(trace, row=1, col=2)
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
    changes_section.update_yaxes(row=1, col=2, title_text="USD")
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
    add_hline_current(fig, prices_df, "GOLD", 1, 1, precision=2)
    add_hline_current(fig, prices_df, "SILVER", 1, 2, precision=2)
    return fig


def make_interest_rate_section(interest_df):
    """Make interest rate section."""
    fig = px.line(
        interest_df, x=interest_df.index, y=interest_df.columns, title="Interest Rates"
    )
    fig.update_yaxes(title_text="Percent")
    fig.update_xaxes(title_text="")
    return fig


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
    diff_df = daily_df.resample("M").last().interpolate().diff().dropna().iloc[-36:]
    monthly_bar = px.bar(diff_df, x=diff_df.index, y=column)
    return monthly_bar


def make_total_bar_yoy(daily_df, column):
    """Make year over year total profit bar graphs."""
    diff_df = daily_df.resample("Y").last().interpolate().diff().dropna()
    # Re-align at beginning of year.
    diff_df.index = pd.DatetimeIndex(diff_df.index.strftime("%Y-01-01"))
    yearly_bar = px.bar(diff_df, x=diff_df.index, y=column, text_auto=".3s")
    return yearly_bar


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
    merged = reduce(
        lambda l, r: pd.merge(l, r, left_index=True, right_index=True, how="outer"),
        [
            fedfunds_df,
            sofr_df,
            swvxx_df,
            wealthfront_df,
        ],
    )
    return merged[sorted(merged.columns)].interpolate()
