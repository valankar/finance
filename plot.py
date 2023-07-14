#!/usr/bin/env python3
"""Plot finance graphs."""

import math
from datetime import datetime
from functools import reduce

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from dateutil.relativedelta import relativedelta
from plotly.subplots import make_subplots
from prefixed import Float
from sqlalchemy import create_engine

import common

TODAY_TIME = datetime.now()
INDEX_HTML = "index.html"
STATIC_HTML = "static.html"
COLOR_GREEN = "DarkGreen"
COLOR_RED = "DarkRed"
HTML_STYLE = """
<style>
body {
  background-color: black;
  color: white;
}
</style>
"""

HOMES = [
    "Mt Vernon",
    "Northlake",
    "Villa Maria",
]


def add_fl_home_sale(fig, row="all", col="all"):
    """Add Florida Home Sale rectangle."""
    fig.add_vrect(
        x0="2019-10-13",
        x1="2019-10-16",
        annotation_text="FL Home Sale",
        fillcolor="green",
        opacity=0.25,
        line_width=0,
        annotation_position="top left",
        row=row,
        col=col,
    )


def add_retirement(fig, row="all", col="all"):
    """Add Retirement line."""
    ret_time = datetime(2022, 5, 31).timestamp() * 1000
    fig.add_vline(
        x=ret_time,
        annotation_text="Unemployed",
        line_color="green",
        opacity=0.25,
        annotation_position="top left",
        row=row,
        col=col,
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


def build_ranges(dataframe, columns):
    """Build graph ranges based on min/max values."""
    last_time = dataframe.index[-1].strftime("%Y-%m-%d")
    xranges = {
        "All": [dataframe.index[0].strftime("%Y-%m-%d"), last_time],
        "2y": [(TODAY_TIME + relativedelta(years=-2)).strftime("%Y-%m-%d"), last_time],
        "1y": [(TODAY_TIME + relativedelta(years=-1)).strftime("%Y-%m-%d"), last_time],
        "YTD": [TODAY_TIME.strftime("%Y-01-01"), last_time],
        "6m": [(TODAY_TIME + relativedelta(months=-6)).strftime("%Y-%m-%d"), last_time],
        "3m": [(TODAY_TIME + relativedelta(months=-3)).strftime("%Y-%m-%d"), last_time],
        "1m": [(TODAY_TIME + relativedelta(months=-1)).strftime("%Y-%m-%d"), last_time],
    }
    ranges = {}
    for column in columns:
        col_dict = {}
        for span, xrange in xranges.items():
            col_dict[span] = {
                "yrange": [
                    dataframe.loc[xrange[0] : xrange[1], column].min(),
                    dataframe.loc[xrange[0] : xrange[1], column].max(),
                ],
                "xrange": xrange,
            }
        ranges[column] = col_dict
    return ranges


def add_range_buttons_single(fig, dataframe, columns):
    """Add a range selector to a single plot that updates y axis as well as x."""
    ranges = build_ranges(dataframe, columns)
    buttons = []
    for label in ("All", "2y", "1y", "YTD", "6m", "3m", "1m"):
        button_dict = dict(
            label=label,
            method="relayout",
        )
        arg_dict = {}
        mins = maxes = []
        for col_dict in ranges.values():
            mins.append(col_dict[label]["yrange"][0])
            maxes.append(col_dict[label]["yrange"][1])
            xrange = col_dict[label]["xrange"]

        arg_dict["xaxis.range"] = xrange
        arg_dict["yaxis.range"] = [min(mins), max(maxes)]
        button_dict["args"] = [arg_dict]
        buttons.append(button_dict)
    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                active=2,  # 1y
                x=0.5,
                y=-0.05,
                buttons=buttons,
            )
        ]
    )
    # Select button 2.
    fig.plotly_relayout(buttons[2]["args"][0])


def add_range_buttons(subplot, dataframe, columns):
    """Add a range selector to a 2-col subplot that updates y axis as well as x."""
    ranges = build_ranges(dataframe, columns)
    num_col = len(columns)
    col_split = list(
        reversed(np.array_split(np.array(columns), math.ceil(num_col / 2)))
    )
    buttons = []
    for label in ("All", "2y", "1y", "YTD", "6m", "3m", "1m"):
        button_dict = dict(
            label=label,
            method="relayout",
        )
        arg_dict = {}
        col = 0
        for pair in col_split:
            for col_name in pair:
                suffix = ""
                if col == 0:
                    arg_dict["xaxis.range"] = ranges[col_name][label]["xrange"]
                else:
                    suffix = f"{col + 1}"
                arg_dict[f"yaxis{suffix}.range"] = ranges[col_name][label]["yrange"]
                col += 1
            if len(pair) < 2:
                col += 1
        button_dict["args"] = [arg_dict]
        buttons.append(button_dict)
    subplot.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                active=2,  # 1y
                x=0.5,
                y=-0.05,
                buttons=buttons,
            )
        ]
    )
    # Select button 2.
    subplot.plotly_relayout(buttons[2]["args"][0])


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

    for row, col in [(1, 2), (2, 1), (3, 2)]:
        add_fl_home_sale(section, row, col)
    for row, col in [(0, 1), (2, 1), (1, 2)]:
        add_retirement(section, row, col)
    section.add_vrect(
        x0="2019-01-01",
        x1="2019-10-13",
        annotation_text="Debt",
        fillcolor="red",
        opacity=0.25,
        line_width=0,
        annotation_position="top right",
        row=1,
        col=2,
    )
    add_range_buttons(section, daily_df, columns)
    return section


def get_investing_retirement_df(daily_df, accounts_df):
    """Get merged df with other investment accounts."""
    invret_cols = ["pillar2", "ira", "commodities", "etfs"]
    invret_df = daily_df[invret_cols]
    ibonds_df = accounts_df["USD_Treasury Direct"].rename("ibonds").fillna(0)
    pal_df = (
        accounts_df["USD_Charles Schwab_Pledged Asset Line"]
        .rename("pledged_asset_line")
        .fillna(0)
    )
    return reduce_merge_asof([invret_df, ibonds_df, pal_df])


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
    add_range_buttons(section, invret_df, invret_df.columns)
    return section


def make_real_estate_section(real_estate_df):
    """Line graph of real estate with percent change."""
    section = px.line(
        real_estate_df,
        x=real_estate_df.index,
        y=real_estate_df.columns,
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
    add_range_buttons(section, real_estate_df, real_estate_df.columns)
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
    add_range_buttons(fig, prices_df, prices_df.columns)
    return fig


def make_interest_rate_section(interest_df):
    """Make interest rate section."""
    fig = px.line(
        interest_df, x=interest_df.index, y=interest_df.columns, title="Interest Rates"
    )
    fig.update_yaxes(title_text="Percent")
    fig.update_xaxes(title_text="")
    add_range_buttons_single(fig, interest_df, interest_df.columns)
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


def make_total_bar_dod(daily_df, column):
    """Make day over day total profit bar graphs."""
    diff_df = daily_df.diff().dropna().iloc[-30:]
    daily_bar = px.bar(diff_df, x=diff_df.index, y=column)
    return daily_bar


def make_total_bar_wow(daily_df, column):
    """Make week over week total profit bar graphs."""
    diff_df = daily_df.resample("W").last().interpolate().diff().dropna().iloc[-52:]
    weekly_bar = px.bar(diff_df, x=diff_df.index, y=column)
    return weekly_bar


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


def write_dynamic_plots(section_tuples):
    """Write out dynamic plots."""
    wrote_plotlyjs = False
    with common.temporary_file_move(f"{common.PREFIX}{INDEX_HTML}") as index_file:
        index_file.write(HTML_STYLE)
        for section, height, width in section_tuples:
            if wrote_plotlyjs:
                include_plotlyjs = False
            else:
                include_plotlyjs = "cdn"
                wrote_plotlyjs = True
            height_percent = f"{height*100}%"
            width_percent = f"{width*100}%"
            index_file.write(
                section.to_html(
                    full_html=False,
                    include_plotlyjs=include_plotlyjs,
                    default_height=height_percent,
                    default_width=width_percent,
                )
            )


def write_static_plots(section_tuples):
    """Write out static plots."""
    # Static plots
    default_image_width = 1920
    default_image_height = 1080
    image_name = 1
    with common.temporary_file_move(f"{common.PREFIX}{STATIC_HTML}") as index_file:
        index_file.write(HTML_STYLE)
        for section, height, width in section_tuples:
            # Remove interaction buttons.
            section["layout"].pop("updatemenus")
            section.write_image(
                common.PREFIX + f"images/{image_name}.png",
                width=default_image_width * width,
                height=default_image_height * height,
            )
            print(f'<img src="images/{image_name}.png">', file=index_file)
            image_name += 1


def write_html_and_images(section_tuples):
    """Generate html and images."""
    write_dynamic_plots(section_tuples)
    write_static_plots(section_tuples)


def load_sqlite_and_rename_col(table, columns=None):
    """Load table from sqlite and rename columns."""
    with create_engine(common.SQLITE_URI).connect() as conn:
        dataframe = (
            pd.read_sql_table(table, conn, index_col="date")
            .resample("D")
            .last()
            .interpolate()
        )
    if columns:
        dataframe = dataframe.rename(columns=columns)
    return dataframe


def get_real_estate_df():
    """Get real estate price and rent data from sqlite."""
    table_map = {
        "mtvernon": "Mt Vernon",
        "northlake": "Northlake",
        "villamaria": "Villa Maria",
    }
    dataframes = []
    for table, home in table_map.items():
        dataframes.append(load_sqlite_and_rename_col(table, {"value": f"{home} Price"}))
        dataframes.append(
            load_sqlite_and_rename_col(f"{table}_rent", {"value": f"{home} Rent"})
        )
    return reduce_merge_asof(dataframes)


def get_interest_rate_df():
    """Merge interest rate data."""
    fedfunds_df = load_sqlite_and_rename_col("fedfunds", {"percent": "Fed Funds"})[
        "2019":
    ]
    sofr_df = load_sqlite_and_rename_col("sofr", {"percent": "SOFR"})["2019":]
    swvxx_df = load_sqlite_and_rename_col("swvxx_yield", {"percent": "Schwab SWVXX"})
    wealthfront_df = load_sqlite_and_rename_col(
        "wealthfront_cash_yield", {"percent": "Wealthfront Cash"}
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


def load_dataframes_from_sqlite():
    """Load dataframes in SQLite."""
    all_df = load_sqlite_and_rename_col("history")
    accounts_df = load_sqlite_and_rename_col("account_history")
    forex_df = load_sqlite_and_rename_col("forex")
    commodities_df = load_sqlite_and_rename_col("commodities_prices")
    prices_df = reduce_merge_asof([forex_df, commodities_df])
    return (all_df, accounts_df, prices_df)


def reduce_merge_asof(dataframes):
    """Reduce and merge date tables."""
    return reduce(
        lambda l, r: pd.merge_asof(l, r, left_index=True, right_index=True),
        dataframes,
    )


def main():
    """Main."""
    pio.templates.default = "plotly_dark"
    all_df, accounts_df, prices_df = load_dataframes_from_sqlite()
    real_estate_daily_df = get_real_estate_df()
    invret_daily_df = get_investing_retirement_df(all_df, accounts_df)

    assets_section = make_assets_breakdown_section(all_df)
    invret_section = make_investing_retirement_section(invret_daily_df)
    real_estate_section = make_real_estate_section(real_estate_daily_df)
    allocation_section = make_allocation_profit_section(
        all_df, invret_daily_df, real_estate_daily_df
    )
    net_worth_change_section = make_change_section(
        all_df, "total", "Total Net Worth Change"
    )
    total_no_homes_change_section = make_change_section(
        all_df, "total_no_homes", "Total Without Real Estate Change"
    )
    prices_section = make_prices_section(prices_df)
    yield_section = make_interest_rate_section(get_interest_rate_df())

    write_html_and_images(
        (
            (assets_section, 1, 1),
            (invret_section, 1, 1),
            (real_estate_section, 1, 1),
            (allocation_section, 0.75, 1),
            (net_worth_change_section, 0.50, 1),
            (total_no_homes_change_section, 0.50, 1),
            (prices_section, 0.75, 1),
            (yield_section, 0.5, 1),
        ),
    )
    # Reset theme to default.
    pio.templates.default = "plotly"


if __name__ == "__main__":
    main()
