#!/usr/bin/env python3
"""Plot weight graph."""

from datetime import datetime
from functools import cached_property
from typing import List

import pandas as pd
from dateutil.relativedelta import relativedelta
from loguru import logger
from nicegui import ui

import common

SELECTED_RANGE = "1y"


class MainGraphs:
    """Collection of all main graphs."""

    def __init__(self):
        self.ui_plotly = []
        self.assets_chart = None
        self.assets_title_columns = [
            ("Total", "total"),
            ("Real Estate", "total_real_estate"),
            ("Total w/o Real Estate", "total_no_homes"),
            ("Retirement", "total_retirement"),
            ("Investing", "total_investing"),
            ("Liquid", "total_liquid"),
        ]
        self.invret_chart = None
        self.invret_title_columns = [
            ("Pillar 2", "pillar2"),
            ("IRA", "ira"),
            ("Commodities", "commodities"),
            ("ETFs", "etfs"),
        ]

    def get_yrange(self, dataframe, column, start, end):
        """Get min/max of column within window."""
        return int(dataframe[start:end][column].min()), int(
            dataframe[start:end][column].max()
        )

    def get_xrange(
        self, dataframe: pd.DataFrame, selected_range: str
    ) -> List[str] | None:
        """Determine time range for selected button."""
        today_time = datetime.now()
        last_time = dataframe.index[-1].strftime("%Y-%m-%d")
        xrange = None
        match selected_range:
            case "All":
                xrange = [dataframe.index[0].strftime("%Y-%m-%d"), last_time]
            case "2y":
                xrange = [
                    (today_time + relativedelta(years=-2)).strftime("%Y-%m-%d"),
                    last_time,
                ]
            case "1y":
                xrange = [
                    (today_time + relativedelta(years=-1)).strftime("%Y-%m-%d"),
                    last_time,
                ]
            case "YTD":
                xrange = [today_time.strftime("%Y-01-01"), last_time]
            case "6m":
                xrange = [
                    (today_time + relativedelta(months=-6)).strftime("%Y-%m-%d"),
                    last_time,
                ]
            case "3m":
                xrange = [
                    (today_time + relativedelta(months=-3)).strftime("%Y-%m-%d"),
                    last_time,
                ]
            case "1m":
                xrange = [
                    (today_time + relativedelta(months=-1)).strftime("%Y-%m-%d"),
                    last_time,
                ]
        return xrange

    @cached_property
    def all_df(self):
        """Load all dataframe."""
        return common.read_sql_table("history").sort_index()

    @property
    def invret_df(self):
        """Investing & retirement dataframe."""
        return self.all_df[["pillar2", "ira", "commodities", "etfs"]]

    def get_data(self, df, column):
        """Get x, y format for series.data."""
        return list(
            zip(
                [x.strftime("%Y-%m-%d") for x in df.index],
                df[column].tolist(),
            )
        )

    def get_markline(self, df, column):
        """Get markLine at latest value."""
        return {
            "silent": True,
            "symbol": "none",
            "label": {
                "position": "insideStartTop",
                "color": "inherit",
                "formatter": f"{df.iloc[-1][column]:,.0f}",
            },
            "data": [
                {
                    "yAxis": df.iloc[-1][column],
                }
            ],
        }

    def get_alignment_params(self, graph_alignment, title_columns):
        """Calculate title alignment, xaxis, and yaxis params."""
        title_alignment = []
        xaxis = []
        yaxis = []
        for i, (title, _) in enumerate(title_columns):
            left = int(graph_alignment[i]["left"].strip("%")) + int(
                int(graph_alignment[i]["width"].strip("%")) / 2
            )
            top = int(graph_alignment[i]["top"].strip("%")) - 2
            title_alignment.append(
                {
                    "text": title,
                    "textAlign": "center",
                    "left": f"{left}%",
                    "top": f"{top}%",
                }
            )
            xaxis.append(
                {
                    "type": "time",
                    "gridIndex": i,
                    "axisLine": {"show": False, "onZero": False},
                }
            )
            yaxis.append(
                {
                    "type": "value",
                    "gridIndex": i,
                    "axisLine": {"show": False},
                    "axisTick": {"show": False},
                    "splitLine": {"show": False},
                },
            )
        return title_alignment, xaxis, yaxis

    def get_series(self, df, title_columns):
        """Get series data for subplots."""
        series = []
        for i, (title, column) in enumerate(title_columns):
            series.append(
                {
                    "type": "line",
                    "name": title,
                    "symbol": "none",
                    "data": self.get_data(df, column),
                    "markLine": self.get_markline(df, column),
                    "xAxisIndex": i,
                    "yAxisIndex": i,
                }
            )
        return series

    async def update_min_max(self, chart, df, title_columns):
        """Update axis min/max on chart based on selected range."""
        x_min, x_max = self.get_xrange(df, SELECTED_RANGE)
        x_minmax_js_str = ", ".join(["{" + f'min: "{x_min}", max: "{x_max}"' + "}"] * 6)
        y_minmax_js_list = []
        for _, column in title_columns:
            y_min, y_max = self.get_yrange(df, column, x_min, x_max)
            y_minmax_js_list.append("{" + f"min: {y_min}, max: {y_max}" + "}")
        y_minmax_js_str = ", ".join(y_minmax_js_list)
        chart.run_chart_method(
            ":setOption",
            "{xAxis: [" + x_minmax_js_str + "], yAxis: [" + y_minmax_js_str + "]}",
        )

    async def create_assets_graph(self):
        """Create assets 2x3 graph."""

        graph_alignment = [
            {"left": "5%", "top": "6%", "width": "45%", "height": "25%"},
            {"left": "55%", "top": "6%", "width": "45%", "height": "25%"},
            {"left": "5%", "top": "36%", "width": "45%", "height": "25%"},
            {"left": "55%", "top": "36%", "width": "45%", "height": "25%"},
            {"left": "5%", "top": "66%", "width": "45%", "height": "25%"},
            {"left": "55%", "top": "66%", "width": "45%", "height": "25%"},
        ]

        title_alignment, xaxis, yaxis = self.get_alignment_params(
            graph_alignment,
            self.assets_title_columns,
        )

        return (
            ui.echart(
                {
                    "title": [
                        {"left": "center", "text": "Assets"},
                        *title_alignment,
                    ],
                    "xAxis": xaxis,
                    "yAxis": yaxis,
                    "grid": graph_alignment,
                    "tooltip": {"trigger": "axis"},
                    "series": self.get_series(self.all_df, self.assets_title_columns),
                }
            )
            .classes("w-full")
            .style("height: 100vh")
        )

    async def create_investing_retirement_graph(self):
        """Create investing & retirement 2x2 graph."""
        graph_alignment = [
            {"left": "5%", "top": "6%", "width": "45%", "height": "25%"},
            {"left": "55%", "top": "6%", "width": "45%", "height": "25%"},
            {"left": "5%", "top": "36%", "width": "45%", "height": "25%"},
            {"left": "55%", "top": "36%", "width": "45%", "height": "25%"},
        ]
        title_alignment, xaxis, yaxis = self.get_alignment_params(
            graph_alignment,
            self.invret_title_columns,
        )
        return (
            ui.echart(
                {
                    "title": [
                        {"left": "center", "text": "Investing & Retirement"},
                        *title_alignment,
                    ],
                    "xAxis": xaxis,
                    "yAxis": yaxis,
                    "grid": graph_alignment,
                    "tooltip": {"trigger": "axis"},
                    "series": self.get_series(
                        self.invret_df, self.invret_title_columns
                    ),
                }
            )
            .classes("w-full")
            .style("height: 100vh")
        )

    async def create(self):
        """Create all graphs."""
        self.assets_chart = await self.create_assets_graph()
        self.invret_chart = await self.create_investing_retirement_graph()
        await self.update()

    async def update(self):
        """Update all graphs."""
        await self.update_min_max(
            self.assets_chart, self.all_df, self.assets_title_columns
        )
        await self.update_min_max(
            self.invret_chart, self.invret_df, self.invret_title_columns
        )


@ui.page("/")
async def main_page():
    """Generate main UI."""
    headers = ui.context.client.request.headers
    logger.info(
        "User: {user}, IP: {ip}, Country: {country}",
        user=headers.get("cf-access-authenticated-user-email", "unknown"),
        ip=headers.get("cf-connecting-ip", "unknown"),
        country=headers.get("cf-ipcountry", "unknown"),
    )
    with ui.footer().classes("transparent q-py-none"):
        with ui.tabs().classes("w-full") as tabs:
            for timerange in ["All", "2y", "1y", "YTD", "6m", "3m", "1m"]:
                ui.tab(timerange)
            tabs.bind_value(globals(), "SELECTED_RANGE")

    await ui.context.client.connected()
    graphs = MainGraphs()
    await graphs.create()
    tabs.on_value_change(graphs.update)


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title="Accounts", dark=True)
