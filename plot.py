#!/usr/bin/env python3
"""Plot finance graphs."""

from functools import reduce
from typing import Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
from plotly.graph_objects import Figure
from plotly.subplots import make_subplots
from prefixed import Float

import balance_etfs
import common
import i_and_e
import margin_interest
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
        annotation_text=f"{current:,.{precision}f} {percent_annotation}",
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


def make_daily_indicator(hourly_df: pd.DataFrame) -> Figure:
    df = hourly_df[hourly_df.index[-1] + relativedelta(days=-1) :]
    fig = go.Figure()
    for col, (column, title) in enumerate(
        [
            ("total", "Total"),
            ("total_no_homes", "Total w/o Real Estate"),
        ]
    ):
        fig.add_trace(
            go.Indicator(
                mode="number+delta+gauge",
                number={"prefix": "$"},
                title={"text": title},
                value=df.iloc[-1][column],
                delta={"reference": df.iloc[0][column], "valueformat": ",.0f"},
                gauge={},
                domain={"row": 0, "column": col},
            )
        )
    centered_title(fig, "Daily Change")
    fig.update_layout(grid={"rows": 1, "columns": 2})
    return fig


def make_assets_breakdown_section(daily_df: pd.DataFrame) -> Figure:
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
    return section


def make_investing_retirement_section(invret_df: pd.DataFrame) -> Figure:
    """Make investing and retirement section."""
    columns = [
        ("pillar2", "Pillar 2"),
        ("ira", "IRA"),
        ("commodities", "Gold, Silver, Crypto"),
        ("etfs", "ETFs"),
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
    centered_title(section, "Investing & Retirement")
    section.update_xaxes(title_text="", showticklabels=True)
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


def make_real_estate_section(real_estate_df: pd.DataFrame) -> Figure:
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
    for i, p in enumerate(reversed(common.PROPERTIES)):
        add_hline_current(section, real_estate_df, f"{p.name} Price", i + 1, 1)
        add_hline_current(section, real_estate_df, f"{p.name} Rent", i + 1, 2)
    return section


def make_real_estate_profit_bar(real_estate_df: pd.DataFrame) -> go.Bar:
    """Bar chart of real estate profit."""
    values = []
    percent = []
    cols = [f"{home.name} Price" for home in common.PROPERTIES]
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
    cols = [f"{home.name} Price" for home in common.PROPERTIES]
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
        subplot_titles=(
            "Current",
            "Desired",
        ),
        specs=[[{"type": "pie"}, {"type": "pie"}]],
    )
    if (dataframe := balance_etfs.get_rebalancing_df(0, otm=False)) is None:
        return changes_section

    # Current allocation
    labels = [
        "US Large Cap",
        "US Small Cap",
        "US Bonds",
        "International Developed",
        "International Emerging",
        "Commodities Gold",
        "Commodities Silver",
        "Commodities Crypto",
    ]
    values = [
        dataframe.loc["US_LARGE_CAP"]["value"],
        dataframe.loc["US_SMALL_CAP"]["value"],
        dataframe.loc["US_BONDS"]["value"],
        dataframe.loc["INTERNATIONAL_DEVELOPED"]["value"],
        dataframe.loc["INTERNATIONAL_EMERGING"]["value"],
        dataframe.loc["COMMODITIES_GOLD"]["value"],
        dataframe.loc["COMMODITIES_SILVER"]["value"],
        dataframe.loc["COMMODITIES_CRYPTO"]["value"],
    ]
    pie_total = go.Pie(labels=labels, values=values)
    changes_section.add_trace(pie_total, row=1, col=1)

    # Desired allocation
    values = [
        dataframe.loc["US_LARGE_CAP"]["value"]
        + dataframe.loc["US_LARGE_CAP"]["usd_to_reconcile"],
        dataframe.loc["US_SMALL_CAP"]["value"]
        + dataframe.loc["US_SMALL_CAP"]["usd_to_reconcile"],
        dataframe.loc["US_BONDS"]["value"]
        + dataframe.loc["US_BONDS"]["usd_to_reconcile"],
        dataframe.loc["INTERNATIONAL_DEVELOPED"]["value"]
        + dataframe.loc["INTERNATIONAL_DEVELOPED"]["usd_to_reconcile"],
        dataframe.loc["INTERNATIONAL_EMERGING"]["value"]
        + dataframe.loc["INTERNATIONAL_EMERGING"]["usd_to_reconcile"],
        dataframe.loc["COMMODITIES_GOLD"]["value"]
        + dataframe.loc["COMMODITIES_GOLD"]["usd_to_reconcile"],
        dataframe.loc["COMMODITIES_SILVER"]["value"]
        + dataframe.loc["COMMODITIES_SILVER"]["usd_to_reconcile"],
        dataframe.loc["COMMODITIES_CRYPTO"]["value"]
        + dataframe.loc["COMMODITIES_CRYPTO"]["usd_to_reconcile"],
    ]
    pie_total = go.Pie(labels=labels, values=values)
    changes_section.add_trace(pie_total, row=1, col=2)

    changes_section.update_traces(textinfo="percent+value")
    centered_title(changes_section, "Investing Allocation")
    return changes_section


def make_allocation_profit_section(
    daily_df: pd.DataFrame, real_estate_df: pd.DataFrame
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

    cols = [f"{home.name} Price" for home in common.PROPERTIES]
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
    return changes_section


def make_prices_section(prices_df: pd.DataFrame, title: str) -> Figure:
    """Make section with prices graphs."""
    fig = px.line(
        prices_df,
        x=prices_df.index,
        y=prices_df.columns,
    )
    fig.update_yaxes(title_text="USD")
    fig.update_xaxes(title_text="")
    centered_title(fig, title)
    return fig


def make_forex_section(forex_df: pd.DataFrame, title: str) -> Figure:
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
    return fig


def make_interest_rate_section(interest_df: pd.DataFrame) -> Figure:
    """Make interest rate section."""
    section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "Interest Rates",
            "IBKR Forex Margin Interest Comparison",
        ),
    )
    px.line(
        interest_df,
        x=interest_df.index,
        y=interest_df.columns,
    ).for_each_trace(lambda t: section.add_trace(t, row=1, col=1))
    margin_df, margin_chart = make_margin_comparison_chart()
    margin_chart.for_each_trace(lambda t: section.add_trace(t, row=1, col=2))
    section.add_annotation(
        text=(
            "Cost of CHF loan as percentage of USD loan: "
            + f"{margin_interest.chf_interest_as_percentage_of_usd()*100:.2f}%"
        ),
        x=str(margin_df.index[len(margin_df.index) // 3]),
        y=margin_df.max(axis=None),
        showarrow=False,
        row=1,
        col=2,
    )
    section.update_yaxes(title_text="Percent", col=1)
    section.update_xaxes(title_text="")
    return section


def make_loan_section() -> Figure:
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

    def add_remaining_annotation(
        equity: float, loan: float, row: int, col: int, percent: int
    ):
        loan_remaining = equity * (percent / 100) + loan
        section.add_annotation(
            text=f"Distance to {percent}%: {loan_remaining:,.0f}",
            showarrow=False,
            x="Loan",
            y=equity * 0.1,
            row=row,
            col=col,
        )

    def add_loan_graph(
        get_balances: Callable[[], tuple[pd.DataFrame, pd.DataFrame]],
        col: int,
        percent: int,
    ):
        loan_balance_df, equity_balance_df = get_balances()
        fig = go.Waterfall(
            measure=["relative", "relative", "total"],
            x=["Equity", "Loan", "Equity - Loan"],
            y=[
                equity_balance_df.iloc[-1]["Equity Balance"],
                loan_balance_df.iloc[-1]["Loan Balance"],
                0,
            ],
        )
        section.add_trace(fig, row=1, col=col)
        for percent_hline in (30, 50):
            percent_balance = (
                equity_balance_df.iloc[-1]["Equity Balance"]
                - equity_balance_df.iloc[-1][f"{percent_hline}% Equity Balance"]
            )
            section.add_hline(
                y=percent_balance,
                annotation_text=f"{percent_hline}% Equity Balance",
                line_dash="dot",
                line_color="gray",
                row=1,  # type: ignore
                col=col,  # type: ignore
            )
        add_remaining_annotation(
            equity_balance_df.iloc[-1]["Equity Balance"],
            loan_balance_df.iloc[-1]["Loan Balance"],
            1,
            col,
            percent,
        )

    add_loan_graph(
        margin_loan.get_balances_ibkr,
        1,
        30,
    )
    add_loan_graph(
        margin_loan.get_balances_schwab_nonpal,
        2,
        30,
    )

    section.update_yaxes(matches=None)
    section.update_yaxes(title_text="USD", col=1)
    section.update_traces(showlegend=False)
    section.update_xaxes(title_text="")
    centered_title(section, "Margin/Box Loans")
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
    diff_df = daily_df.resample("YE").last().interpolate().diff().dropna()
    # Re-align at beginning of year.
    diff_df.index = pd.DatetimeIndex(diff_df.index.strftime("%Y-01-01"))  # type: ignore
    yearly_bar = px.bar(diff_df, x=diff_df.index, y=column, text_auto=".3s")  # type: ignore
    return yearly_bar


def make_margin_comparison_chart() -> tuple[pd.DataFrame, Figure]:
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
    return dataframe, chart


def make_short_options_section(options_df: pd.DataFrame) -> Figure:
    """Make short options moneyness/loss bar chart."""
    section = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=(
            "OTM exercise values",
            "ITM exercise values",
        ),
        vertical_spacing=0.07,
        horizontal_spacing=0.05,
    )

    def make_options_graph(df: pd.DataFrame, col: int):
        if not len(df):
            return
        df.loc[:, "name"] = df["count"].astype(str) + " " + df["name"].astype(str)
        df = df.set_index("name").sort_values("exercise_value")
        fig = go.Waterfall(
            x=df.index,
            y=df["exercise_value"],
        )
        section.add_trace(fig, row=1, col=col)

    dataframe = (
        options_df.groupby(level="name")
        .agg({"exercise_value": "sum", "count": "sum", "in_the_money": "first"})
        .reset_index()
    )
    make_options_graph(dataframe[dataframe["in_the_money"] != True], 1)  # noqa: E712
    make_options_graph(dataframe[dataframe["in_the_money"]], 2)
    section.update_yaxes(title_text="USD", col=1)
    section.update_xaxes(title_text="")
    centered_title(section, "Options")
    section.update_traces(showlegend=False)
    return section


def get_interest_rate_df() -> pd.DataFrame:
    """Merge interest rate data."""
    fedfunds_df = common.load_sqlite_and_rename_col(
        "fedfunds", rename_cols={"percent": "Fed Funds"}
    )["2019":]
    sofr_df = common.load_sqlite_and_rename_col(
        "sofr", rename_cols={"percent": "SOFR"}
    )["2019":]
    swvxx_df = common.load_sqlite_and_rename_col(
        "swvxx_yield", rename_cols={"percent": "Schwab SWVXX"}
    )
    wealthfront_df = common.load_sqlite_and_rename_col(
        "wealthfront_cash_yield", rename_cols={"percent": "Wealthfront Cash"}
    )
    ibkr_df = common.load_sqlite_and_rename_col(
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
