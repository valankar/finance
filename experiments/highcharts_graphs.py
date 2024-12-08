#!/usr/bin/env python3
"""Plot with Highcharts"""

from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from nicegui import run, ui

import common

RANGES = ["All", "3y", "2y", "1y", "YTD", "6m", "3m", "1m", "1d"]
DEFAULT_RANGE = "1y"


def get_xrange(
    dataframe: pd.DataFrame, selected_range: str
) -> tuple[datetime, datetime] | None:
    """Determine time range for selected button."""
    latest_time = dataframe.index[-1]
    earliest_time = dataframe.index[0]
    today_time = datetime.now()
    xrange = None
    relative = None
    match selected_range:
        case "All":
            xrange = (earliest_time, latest_time)
        case "3y":
            relative = relativedelta(years=-3)
        case "2y":
            relative = relativedelta(years=-2)
        case "1y":
            relative = relativedelta(years=-1)
        case "YTD":
            xrange = (
                datetime.strptime(today_time.strftime("%Y-01-01"), "%Y-%m-%d"),
                latest_time,
            )
        case "6m":
            relative = relativedelta(months=-6)
        case "3m":
            relative = relativedelta(months=-3)
        case "1m":
            relative = relativedelta(months=-1)
        case "1d":
            relative = relativedelta(days=-1)
    if relative:
        xrange = ((latest_time + relative), latest_time)
    return xrange


def create_asset_chart(
    title: str,
    dataframe: pd.DataFrame,
    column: str,
    start: datetime | None,
) -> ui.highchart:
    xaxis: dict[str, str | float] = {"type": "datetime"}
    if start:
        xaxis["min"] = start.timestamp() * 1000
    return ui.highchart(
        {
            "title": {"text": title},
            "chart": {"styledMode": True},
            "legend": {"enabled": False},
            "series": [
                {
                    "data": list(
                        zip(
                            list(dataframe.index.map(pd.Timestamp.timestamp) * 1000),
                            list(dataframe[column].round()),
                        )
                    ),
                },
            ],
            "xAxis": xaxis,
        }
    ).style("height: 30vh")


class MainGraphs:
    def __init__(self, selected_range: str):
        self.ranged_highcharts = []
        self.selected_range = selected_range
        self.all_df = None

    def create(self):
        self.all_df = common.read_sql_query("select * from history order by date asc")
        start = None
        if r := get_xrange(self.all_df, self.selected_range):
            start, _ = r
        title_cols = (
            ("Total", "total"),
            ("Real Estate", "total_real_estate"),
            ("Total w/o Real Estate", "total_no_homes"),
            ("Retirement", "total_retirement"),
            ("Investing", "total_investing"),
            ("Liquid", "total_liquid"),
        )
        with ui.grid(columns=2).classes("w-full h-screen"):
            for title, column in title_cols:
                self.ranged_highcharts.append(
                    create_asset_chart(title, self.all_df, column, start)
                )

    async def update(self):
        start = None
        if self.all_df is None:
            return
        if r := get_xrange(self.all_df, self.selected_range):
            start, _ = r
        if not start:
            return
        for chart in self.ranged_highcharts:
            chart.options["xAxis"]["min"] = start.timestamp() * 1000
            await run.io_bound(chart.update)


def main_page():
    ui.add_css('@import url("https://code.highcharts.com/css/highcharts.css")')
    with ui.footer().classes("transparent q-py-none"):
        with ui.tabs().classes("w-full") as tabs:
            for timerange in RANGES:
                ui.tab(timerange)
    graphs = MainGraphs(DEFAULT_RANGE)
    tabs.bind_value(graphs, "selected_range")
    graphs.create()
    tabs.on_value_change(graphs.update)
