#!/usr/bin/env python3
"""Plot finance graphs."""

from functools import reduce

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objects import Figure
from plotly.subplots import make_subplots
from prefixed import Float

import balance_etfs
import common
import homes
import margin_loan

COLOR_GREEN = "DarkGreen"
COLOR_RED = "DarkRed"


def set_bar_chart_color(trace, fig: Figure, row, col):
    trace.marker.color = [COLOR_GREEN if y > 0 else COLOR_RED for y in trace.y]
    fig.add_trace(trace, row=row, col=col)


def add_hline_current(
    fig: Figure,
    data: pd.DataFrame,
    df_col: str,
    row: int,
    col: int,
    annotation_position: str = "top left",
    secondary_y: bool = False,
    precision: int = 0,
):
    """Add hline to represent total and change percent."""
    current = data[df_col].loc[data[df_col].last_valid_index()]
    percent_change = 0
    if (earliest := data[df_col].loc[data[df_col].first_valid_index()]) != 0:
        percent_change = (current - earliest) / earliest * 100
        if earliest < 0:
            percent_change *= -1
    percent_precision = 0
    if 1 > percent_change > 0 or 0 > percent_change > -1:
        percent_precision = 2
    percent_annotation = f"{percent_change:.{percent_precision}f}%"
    match percent_change:
        case percent_change if percent_change > 0:
            percent_annotation = "+" + percent_annotation
        case percent_change if percent_change == 0:
            percent_annotation = ""
    fig.add_hline(
        y=current,
        annotation_text=f"{current:,.{precision}f} {percent_annotation} ({current - earliest:,.0f})",
        line_dash="dot",
        line_color="gray",
        annotation_position=annotation_position,
        row=row,  # type: ignore
        col=col,  # type: ignore
        secondary_y=secondary_y,
    )


def update_facet_titles(fig: Figure, columns: list[tuple[str, str]]):
    def col_to_name(facet):
        col = facet.text.split("=")[-1]
        for c, name in columns:
            if c == col:
                facet.update(text=name)

    fig.for_each_annotation(col_to_name)


def centered_title(fig: Figure, title: str):
    fig.update_layout(title={"text": title, "x": 0.5, "xanchor": "center"})


def make_assets_breakdown_section(
    daily_df: pd.DataFrame, margin: dict[str, int]
) -> Figure:
    """Make assets trend section."""
    columns = [
        ("total", "Total"),
        ("total_real_estate", "Real Estate"),
        ("total_no_homes", "Total w/o Real Estate"),
        ("total_retirement", "Retirement"),
        ("total_investing", "Investing"),
        ("total_liquid", "Liquid"),
    ]
    table_cols = [c for c, _ in columns]
    section = px.line(
        daily_df,
        x=daily_df.index,
        y=table_cols,
        facet_col="variable",
        facet_col_wrap=2,
        category_orders={"variable": table_cols},
    )
    update_facet_titles(section, columns)
    centered_title(section, "Assets Breakdown")
    section.update_yaxes(matches=None, title_text="")
    section.update_yaxes(col=2, showticklabels=True)
    section.update_yaxes(col=1, title_text="USD")
    section.update_xaxes(title_text="", showticklabels=True)
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
    section.update_layout(margin=margin)
    return section


def make_investing_retirement_section(
    invret_df: pd.DataFrame, margin: dict[str, int]
) -> Figure:
    """Make investing and retirement section."""
    columns = [
        ("pillar2", "Pillar 2"),
        ("ira", "IRA"),
    ]
    section = px.line(
        invret_df,
        x=invret_df.index,
        y=invret_df.columns,
        facet_col="variable",
        facet_col_wrap=2,
        labels={"value": "USD"},
    )
    update_facet_titles(section, columns)
    centered_title(section, "Retirement")
    section.update_xaxes(title_text="", showticklabels=True)
    section.update_yaxes(title_text="")
    section.update_yaxes(matches=None)
    section.update_yaxes(col=2, showticklabels=True)
    section.update_yaxes(col=1, title_text="USD")
    section.update_traces(showlegend=False)
    add_hline_current(section, invret_df, "pillar2", 0, 1)
    add_hline_current(section, invret_df, "ira", 0, 2)
    section.update_layout(margin=margin)
    return section


def make_real_estate_section(
    real_estate_df: pd.DataFrame, margin: dict[str, int]
) -> Figure:
    """Line graph of real estate."""
    cols = [x for x in real_estate_df.columns if "Percent" not in x]
    section = px.line(
        real_estate_df,
        x=real_estate_df.index,
        y=cols,
        facet_col="variable",
        facet_col_wrap=2,
        labels={"value": "USD"},
    )
    centered_title(section, "Real Estate")
    section.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    section.update_xaxes(title_text="", showticklabels=True)
    section.update_yaxes(title_text="")
    section.update_yaxes(matches=None)
    section.update_yaxes(col=2, showticklabels=True)
    section.update_yaxes(col=1, title_text="USD")
    section.update_traces(showlegend=False)
    for i, p in enumerate(reversed(homes.PROPERTIES)):
        add_hline_current(section, real_estate_df, f"{p.name} Price", i + 1, 1)
        add_hline_current(section, real_estate_df, f"{p.name} Rent", i + 1, 2)
    section.update_layout(margin=margin)
    return section


def make_real_estate_profit_bar(real_estate_df: pd.DataFrame) -> go.Bar:
    """Bar chart of real estate profit."""
    values = []
    percent = []
    cols = [f"{home.name} Price" for home in homes.PROPERTIES]
    for home in cols:
        values.append(
            real_estate_df.iloc[-1][home]
            - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
        )
        percent.append(real_estate_df.iloc[-1][f"{home} Percent Change"])
    profit_bar = go.Bar(
        x=cols,
        y=values,
        marker_color=[COLOR_GREEN if x > 0 else COLOR_RED for x in values],
        text=[f"{Float(x):.2h}<br>{y:.2f}%" for x, y in zip(values, percent)],
    )
    return profit_bar


def make_real_estate_profit_bar_yearly(real_estate_df: pd.DataFrame) -> go.Bar:
    """Bar chart of real estate profit yearly."""
    values = []
    percent = []
    cols = [f"{home.name} Price" for home in homes.PROPERTIES]
    for home in cols:
        time_diff = (
            real_estate_df[home].index[-1] - real_estate_df[home].first_valid_index()
        )  # type: ignore
        value_diff = (
            real_estate_df.iloc[-1][home]
            - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
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


def make_investing_allocation_section() -> Figure:
    """Make investing current and desired allocation pie graphs."""
    changes_section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Current", "Rebalancing Required"),
        specs=[[{"type": "pie"}, {"type": "xy"}]],
    )
    dataframe = balance_etfs.get_rebalancing_df(0)
    label_col = (
        ("US Large Cap", "US_LARGE_CAP"),
        ("US Small Cap", "US_SMALL_CAP"),
        ("US Bonds", "US_BONDS"),
        ("International Developed", "INTERNATIONAL_DEVELOPED"),
        ("International Emerging", "INTERNATIONAL_EMERGING"),
        ("Gold", "COMMODITIES_GOLD"),
        ("Silver", "COMMODITIES_SILVER"),
        ("Crypto", "COMMODITIES_CRYPTO"),
    )
    values = [dataframe.loc[col]["value"] for _, col in label_col]
    pie_total = go.Pie(labels=[name for name, _ in label_col], values=values)
    changes_section.add_trace(pie_total, row=1, col=1)
    changes_section.update_traces(row=1, col=1, textinfo="percent")

    # Rebalancing
    values = [dataframe.loc[col]["usd_to_reconcile"] for _, col in label_col]
    fig = go.Figure(
        go.Bar(
            x=[name for name, _ in label_col],
            y=values,
            text=[f"{y:,.0f}" for y in values],
        )
    )
    fig.update_traces(textangle=0)
    fig.for_each_trace(lambda t: set_bar_chart_color(t, changes_section, 1, 2))
    changes_section.update_traces(row=1, col=2, showlegend=False)
    centered_title(changes_section, "Investing Allocation")
    return changes_section


def make_allocation_profit_section(
    daily_df: pd.DataFrame, real_estate_df: pd.DataFrame, margin: dict[str, int]
) -> Figure:
    """Make asset allocation and day changes section."""
    real_estate_df = real_estate_df.copy()
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
    pie_total.for_each_trace(lambda t: changes_section.add_trace(t, row=1, col=1))

    cols = [f"{home.name} Price" for home in homes.PROPERTIES]
    for home in cols:
        real_estate_df[f"{home} Percent Change"] = (
            (
                real_estate_df[home]
                - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
            )
            / real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
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
    changes_section.update_layout(margin=margin)
    return changes_section


def make_prices_section(
    prices_df: pd.DataFrame, title: str, margin: dict[str, int]
) -> Figure:
    """Make section with prices graphs."""
    fig = px.line(
        prices_df,
        x=prices_df.index,
        y=prices_df.columns,
    )
    fig.update_yaxes(title_text="%")
    fig.update_xaxes(title_text="")
    centered_title(fig, title)
    fig.update_layout(margin=margin)
    return fig


def make_forex_section(
    forex_df: pd.DataFrame, title: str, margin: dict[str, int]
) -> Figure:
    """Make section with forex graphs."""
    fig = px.line(
        forex_df,
        x=forex_df.index,
        y=forex_df.columns,
        facet_col="variable",
        facet_col_wrap=2,
    )
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    add_hline_current(fig, forex_df, "CHFUSD", 0, 1, precision=2)
    add_hline_current(fig, forex_df, "SGDUSD", 0, 2, precision=2)
    fig.update_yaxes(matches=None, title_text="")
    fig.update_yaxes(col=2, showticklabels=True)
    fig.update_yaxes(col=1, title_text="USD")
    fig.update_yaxes(title_text="USD")
    fig.update_xaxes(title_text="")
    centered_title(fig, title)
    fig.update_layout(margin=margin)
    return fig


def make_interest_rate_section(
    interest_df: pd.DataFrame, margin: dict[str, int]
) -> Figure:
    """Make interest rate section."""
    # astype needed due to https://github.com/plotly/Kaleido/issues/236
    fig = px.line(
        interest_df,
        x=interest_df.index.astype(str),
        y=interest_df.columns,
        title="Interest Rates",
    )
    fig.update_yaxes(title_text="Percent")
    fig.update_xaxes(title_text="")
    fig.update_layout(margin=margin)
    return fig


def make_brokerage_total_section(
    brokerage_df: pd.DataFrame, margin: dict[str, int]
) -> Figure:
    section = make_subplots(
        rows=1,
        cols=len(margin_loan.LOAN_BROKERAGES),
        subplot_titles=[x.name for x in margin_loan.LOAN_BROKERAGES],
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )
    for i, broker in enumerate(margin_loan.LOAN_BROKERAGES, start=1):
        fig = px.line(
            brokerage_df,
            x=brokerage_df.index,
            y=broker.name,
        )
        fig.for_each_trace(lambda t: section.add_trace(t, row=1, col=i))
        add_hline_current(section, brokerage_df, broker.name, 1, i)
    section.update_yaxes(matches=None)
    section.update_yaxes(title_text="USD", col=1)
    section.update_traces(showlegend=False)
    section.update_xaxes(title_text="")
    centered_title(section, "Brokerage Values")
    section.update_layout(margin=margin)
    return section


def make_loan_section(margin: dict[str, int]) -> Figure:
    """Make section with margin loans."""

    section = make_subplots(
        rows=1,
        cols=len(margin_loan.LOAN_BROKERAGES),
        subplot_titles=[x.name for x in margin_loan.LOAN_BROKERAGES],
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )

    def add_loan_graph(
        df: pd.DataFrame,
        col: int,
    ):
        fig = go.Waterfall(
            measure=["relative", "relative", "total"],
            x=["Equity", "Loan", "Equity - Loan"],
            y=[
                df.iloc[-1]["Equity Balance"],
                df.iloc[-1]["Loan Balance"],
                0,
            ],
        )
        section.add_trace(fig, row=1, col=col)
        for i, percent_hline in enumerate((30, 50), start=1):
            percent_balance = (
                df.iloc[-1]["Equity Balance"]
                - df.iloc[-1][f"{percent_hline}% Equity Balance"]
            )
            section.add_hline(
                y=percent_balance,
                annotation_text=f"{percent_hline}% Equity Balance",
                line_dash="dot",
                line_color="gray",
                row=1,  # type: ignore
                col=col,  # type: ignore
            )
            remaining = df.iloc[-1][f"Distance to {percent_hline}%"]
            section.add_annotation(
                text=f"Distance to {percent_hline}%: {remaining:,.0f}",
                showarrow=False,
                x="Loan",
                y=df.iloc[-1]["Equity Balance"] * (i / 10),
                row=1,
                col=col,
            )

    for i, broker in enumerate(margin_loan.LOAN_BROKERAGES, start=1):
        df = margin_loan.get_balances_broker(broker)
        add_loan_graph(df, i)

    section.update_yaxes(matches=None)
    section.update_yaxes(title_text="USD", col=1)
    section.update_traces(showlegend=False)
    section.update_xaxes(title_text="")
    centered_title(section, "Margin/Box Loans")
    section.update_layout(margin=margin)
    return section


def make_change_section(daily_df: pd.DataFrame, column: str, title: str) -> Figure:
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

    make_total_bar_yoy(daily_df, column).for_each_trace(
        lambda t: set_bar_chart_color(t, changes_section, 1, 1)
    )
    make_total_bar_mom(daily_df, column).for_each_trace(
        lambda t: set_bar_chart_color(t, changes_section, 1, 2)
    )
    changes_section.update_yaxes(title_text="USD", col=1)
    changes_section.update_xaxes(title_text="")
    changes_section.update_xaxes(tickformat="%Y", row=1, col=1)
    centered_title(changes_section, title)
    return changes_section


def make_total_bar_mom(daily_df: pd.DataFrame, column: str) -> Figure:
    """Make month over month total profit bar graphs."""
    diff_df = daily_df.resample("ME").last().interpolate().diff().dropna().iloc[-36:]
    monthly_bar = px.bar(diff_df, x=diff_df.index, y=column)
    line_chart = px.scatter(
        diff_df,
        x=diff_df.index,
        y=column,
        trendline="lowess",
    )
    line_chart.for_each_trace(
        lambda t: monthly_bar.add_trace(t), selector={"mode": "lines"}
    )
    return monthly_bar


def make_total_bar_yoy(daily_df: pd.DataFrame, column: str) -> Figure:
    """Make year over year total profit bar graphs."""
    diff_df = daily_df.resample("YE").last().interpolate().diff().dropna().iloc[-6:]
    # Re-align at beginning of year.
    diff_df.index = pd.DatetimeIndex(diff_df.index.strftime("%Y-01-01"))  # type: ignore
    # astype needed due to https://github.com/plotly/Kaleido/issues/236
    yearly_bar = px.bar(diff_df, x=diff_df.index.astype(str), y=column, text_auto=".3s")  # type: ignore
    return yearly_bar


def get_interest_rate_df() -> pd.DataFrame:
    """Merge interest rate data."""
    fedfunds_df = common.load_sql_and_rename_col(
        "fedfunds", rename_cols={"percent": "Fed Funds"}
    )["2019":]
    sofr_df = common.load_sql_and_rename_col("sofr", rename_cols={"percent": "SOFR"})[
        "2019":
    ]
    swvxx_df = common.load_sql_and_rename_col(
        "swvxx_yield", rename_cols={"percent": "Schwab SWVXX"}
    )
    wealthfront_df = common.load_sql_and_rename_col(
        "wealthfront_cash_yield", rename_cols={"percent": "Wealthfront Cash"}
    )
    ibkr_df = common.load_sql_and_rename_col(
        "interactive_brokers_margin_rates",
        rename_cols={"USD": "USD IBKR Margin", "CHF": "CHF IBKR Margin"},
    )
    merged = reduce(
        lambda L, r: pd.merge(L, r, left_index=True, right_index=True, how="outer"),
        [
            fedfunds_df,
            sofr_df,
            swvxx_df,
            wealthfront_df,
            ibkr_df,
        ],
    )
    return merged.ffill()
