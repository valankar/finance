#!/usr/bin/env python3
"""Plot weight graph."""

import asyncio
import contextlib
import io
import os.path
import subprocess
from datetime import datetime, timedelta
from typing import Awaitable, ClassVar, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.io as pio
from loguru import logger
from nicegui import run, ui
from plotly.graph_objects import Figure

import balance_etfs
import common
import graph_generator
import i_and_e
import plot
import stock_options

SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}

RANGES = ["All", "3y", "2y", "1y", "YTD", "6m", "3m", "1m", "1d"]
DEFAULT_RANGE = "1y"


class MainGraphs:
    """Collection of all main graphs."""

    graphs: ClassVar[graph_generator.Graphs] = {}
    last_updated_time: ClassVar[datetime] = datetime.now()
    last_generation_duration: ClassVar[timedelta] = timedelta()
    latest_datapoint_time: ClassVar[pd.Timestamp] = pd.Timestamp.now()
    LAYOUT: tuple[tuple[str, str], ...] = (
        ("assets_breakdown", "96vh"),
        ("investing_retirement", "75vh"),
        ("real_estate", "96vh"),
        ("allocation_profit", "75vh"),
        ("change", "50vh"),
        ("change_no_homes", "50vh"),
        ("investing_allocation", "50vh"),
        ("prices", "45vh"),
        ("forex", "45vh"),
        ("interest_rate", "45vh"),
        ("loan", "45vh"),
        ("short_options", "50vh"),
        ("daily_indicator", "45vh"),
    )
    CACHE_CALL_ARGS = (LAYOUT, RANGES, SUBPLOT_MARGIN)

    @classmethod
    def all_graphs_populated(cls) -> bool:
        if graph_generator.generate_all_graphs.check_call_in_cache(
            *cls.CACHE_CALL_ARGS
        ):
            (
                cls.graphs,
                cls.last_updated_time,
                cls.last_generation_duration,
                cls.latest_datapoint_time,
            ) = graph_generator.generate_all_graphs(*cls.CACHE_CALL_ARGS)
            return True
        elif len(cls.graphs):
            return True
        return False

    def __init__(self, selected_range: str):
        self.ui_plotly = {}
        self.ui_stats_labels = {}
        self.selected_range = selected_range

    async def update_stats_labels(self) -> None:
        try:
            timezone = ZoneInfo(
                await ui.run_javascript(
                    "Intl.DateTimeFormat().resolvedOptions().timeZone", timeout=10
                )
            )
        except TimeoutError:
            timezone = ZoneInfo("UTC")
        self.ui_stats_labels["last_datapoint_time"].set_text(
            f"Latest datapoint: {MainGraphs.latest_datapoint_time.tz_localize('UTC').astimezone(timezone).strftime('%c')}"
        )
        self.ui_stats_labels["last_updated_time"].set_text(
            f"Graphs last updated: {MainGraphs.last_updated_time.astimezone(timezone).strftime('%c')}"
        )
        self.ui_stats_labels["last_generation_duration"].set_text(
            f"Graph generation duration: {MainGraphs.last_generation_duration.total_seconds():.2f}s"
        )

    async def create(self) -> None:
        """Create all graphs."""
        for name, height in MainGraphs.LAYOUT:
            if name in MainGraphs.graphs["ranged"]:
                graph = MainGraphs.graphs["ranged"][name][self.selected_range]
            else:
                try:
                    graph = MainGraphs.graphs["nonranged"][name]
                except KeyError:
                    continue
            self.ui_plotly[name] = (
                ui.plotly(graph).classes("w-full").style(f"height: {height}")
            )
        with ui.row().classes("flex justify-center w-full"):
            for label in [
                "last_datapoint_time",
                "last_updated_time",
                "last_generation_duration",
                "next_generation_time",
            ]:
                self.ui_stats_labels[label] = ui.label()
            ui.link("Static Images", "/image_only")
        await self.update_stats_labels()

    async def update(self) -> None:
        """Update all graphs."""
        for name in MainGraphs.graphs["ranged"]:
            await run.io_bound(
                self.ui_plotly[name].update_figure,
                MainGraphs.graphs["ranged"][name][self.selected_range],
            )
        await self.update_stats_labels()


class IncomeExpenseGraphs:
    """Collection of all income & expense graphs."""

    def get_graphs(self) -> Iterable[Awaitable[Figure]]:
        """Generate Plotly graphs. This calls subprocess."""
        ledger_df, ledger_summarized_df = i_and_e.get_ledger_dataframes()
        funcs = (
            lambda: i_and_e.get_income_expense_yearly_chart(ledger_summarized_df),
            lambda: i_and_e.get_yearly_chart(
                ledger_summarized_df, "Income", "Yearly Income"
            ),
            lambda: i_and_e.get_yearly_chart(
                ledger_summarized_df, "Expenses", "Yearly Expenses"
            ),
            lambda: i_and_e.get_yearly_chart(
                ledger_df, "Expenses", "Yearly Expenses Categorized"
            ),
            lambda: i_and_e.get_income_expense_monthly_chart(ledger_summarized_df),
            lambda: i_and_e.get_monthly_chart(
                ledger_summarized_df, "Income", "Monthly Income"
            ),
            lambda: i_and_e.get_monthly_chart(
                ledger_summarized_df, "Expenses", "Monthly Expenses"
            ),
            lambda: i_and_e.get_monthly_chart(
                ledger_df, "Income", "Monthly Income Categorized"
            ),
            lambda: i_and_e.get_monthly_chart(
                ledger_df, "Expenses", "Monthly Expenses Categorized"
            ),
            lambda: i_and_e.get_average_monthly_income_expenses_chart(ledger_df),
            lambda: i_and_e.get_average_monthly_top_expenses(ledger_df),
        )
        return (run.io_bound(f) for f in funcs)

    async def create(self):
        """Create all graphs."""
        for graph in self.get_graphs():
            ui.plotly(await graph).classes("w-full").style("height: 50vh")


def log_request():
    if request := ui.context.client.request:
        headers = request.headers
        logger.info(
            "URL: {url} User: {user}, IP: {ip}, Country: {country}, User-Agent: {agent}",
            url=request.url,
            user=headers.get("cf-access-authenticated-user-email", "unknown"),
            ip=headers.get("cf-connecting-ip", "unknown"),
            country=headers.get("cf-ipcountry", "unknown"),
            agent=headers.get("user-agent", "unknown"),
        )


@ui.page("/")
async def main_page():
    """Generate main UI."""
    log_request()
    # To avoid out of webgl context errors. See https://plotly.com/python/webgl-vs-svg/
    ui.add_body_html(
        '<script src="https://unpkg.com/virtual-webgl@1.0.6/src/virtual-webgl.js"></script>'
    )

    async def wait_for_graphs():
        skel = None
        while not MainGraphs.all_graphs_populated():
            await ui.context.client.connected()
            if not skel:
                skel = ui.skeleton("QToolbar").classes("w-full")
            await asyncio.sleep(1)
        if skel:
            skel.delete()

    await wait_for_graphs()

    with ui.footer().classes("transparent q-py-none"):
        with ui.tabs().classes("w-full") as tabs:
            for timerange in RANGES:
                ui.tab(timerange)

    await ui.context.client.connected()
    graphs = MainGraphs(DEFAULT_RANGE)
    tabs.bind_value(graphs, "selected_range")
    await graphs.create()
    tabs.on_value_change(graphs.update)


class MainGraphsImageOnly:
    def __init__(self, selected_range: str):
        self.ui_image = {}
        self.selected_range = selected_range
        self.latest_timestamp = datetime.fromtimestamp(0)

    def images(self) -> None:
        with ui.column().classes("w-full"):
            for name, _ in MainGraphs.LAYOUT:
                for path in [
                    f"{common.PREFIX}/{name}.png",
                    f"{common.PREFIX}/{name}-{self.selected_range}.png",
                ]:
                    if os.path.exists(path):
                        self.ui_image[name] = ui.image(path)
                        if (
                            ts := datetime.fromtimestamp(os.path.getmtime(path))
                        ) > self.latest_timestamp:
                            self.latest_timestamp = ts
                        break
            with ui.row().classes("flex justify-center w-full"):
                ui.label(
                    f"Latest image timestamp: {self.latest_timestamp.strftime('%c')}"
                )
                ui.link("Dynamic graphs", "/")

    def update(self) -> None:
        for name, _ in MainGraphs.LAYOUT:
            if os.path.exists(
                path := f"{common.PREFIX}/{name}-{self.selected_range}.png"
            ):
                self.ui_image[name].set_source(path)


@ui.page("/image_only")
def main_page_image_only():
    log_request()
    with ui.footer().classes("transparent q-py-none"):
        with ui.tabs().classes("w-full") as tabs:
            for timerange in RANGES:
                ui.tab(timerange)
    graphs = MainGraphsImageOnly(DEFAULT_RANGE)
    tabs.bind_value(graphs, "selected_range")
    graphs.images()
    tabs.on_value_change(graphs.update)


@ui.page("/i_and_e", title="Income & Expenses")
async def i_and_e_page():
    """Generate income & expenses page."""
    log_request()
    skel = ui.skeleton("QToolbar").classes("w-full")
    await ui.context.client.connected()
    graphs = IncomeExpenseGraphs()
    await graphs.create()
    skel.delete()


def get_stock_options_output() -> str:
    with contextlib.redirect_stdout(io.StringIO()) as output:
        with common.pandas_options():
            stock_options.main()
            return output.getvalue()


@ui.page("/stock_options", title="Stock Options")
async def stock_options_page():
    """Stock options."""
    log_request()
    skel = ui.skeleton("QToolbar").classes("w-full")
    await ui.context.client.connected()
    ui.html(f"<PRE>{await run.io_bound(get_stock_options_output)}</PRE>")
    fig = plot.make_prices_section(
        common.read_sql_table("index_prices").sort_index(), "Index Prices"
    ).update_layout(margin=SUBPLOT_MARGIN)

    options_df = stock_options.options_df_raw().loc[
        lambda df: (df["ticker"] == "SPX") & (df["count"].abs() > 1)
    ]
    for _, row in options_df.iterrows():
        fig.add_hline(
            y=row["strike"],
            annotation_text=f"{row['count']} {row['name']}",
            annotation_position="top left",
        )

    ui.plotly(fig).classes("w-full").style("height: 50vh")
    skel.delete()


@ui.page("/latest_values", title="Latest Values")
def latest_values_page():
    """Latest values."""
    log_request()
    output = subprocess.check_output(f"{common.CODE_DIR}/latest_values.sh", text=True)
    ui.html(f"<PRE>{output}</PRE>")


@ui.page("/balance_etfs", title="Balance ETFs")
@ui.page("/balance_etfs/{amount}", title="Balance ETFs")
def balance_etfs_page(amount: int = 0):
    """Balance ETFs."""
    log_request()
    with common.pandas_options():
        df = balance_etfs.get_rebalancing_df(amount=amount, otm=False)
        ui.html(f"<PRE>{df}</PRE>")


if __name__ in {"__main__", "__mp_main__"}:
    pio.templates.default = common.PLOTLY_THEME
    ui.run(
        title="Accounts",
        dark=True,
        uvicorn_reload_excludes=f".*, .py[cod], .sw.*, ~*, {common.PREFIX}",
    )
