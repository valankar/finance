#!/usr/bin/env python3
"""Plot weight graph."""

import asyncio
import contextlib
import io
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import plotly.io as pio
import portalocker
import schedule
from dateutil.relativedelta import relativedelta
from loguru import logger
from nicegui import app, background_tasks, run, ui
from zoneinfo import ZoneInfo

import balance_etfs
import common
import i_and_e
import plot
import stock_options

SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}

RANGES = ["All", "2y", "1y", "YTD", "6m", "3m", "1m"]
DEFAULT_RANGE = "1y"


@contextlib.contextmanager
def pandas_options():
    """Set pandas output options."""
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 1000
    ):
        yield


class MainGraphs:
    """Collection of all main graphs."""

    cached_graphs = {}
    last_updated_time = None
    last_generation_duration = None
    latest_datapoint_time = None
    LAYOUT = (
        ("assets_breakdown", "96vh"),
        ("investing_retirement", "75vh"),
        ("real_estate", "96vh"),
        ("allocation_profit", "75vh"),
        ("change_section", "50vh"),
        ("change_section_no_homes", "50vh"),
        ("investing_allocation_section", "50vh"),
        ("prices", "45vh"),
        ("forex", "45vh"),
        ("interest_rate", "45vh"),
        ("loan", "45vh"),
        ("short_options_section", "50vh"),
    )

    @classmethod
    def all_graphs_populated(cls):
        found_graphs = set()
        required_graphs = set([name for name, _ in cls.LAYOUT]) - set(
            ["short_options_section"]
        )
        for graph_type in ["ranged", "nonranged"]:
            found_graphs.update(cls.cached_graphs.get(graph_type, {}).keys())
        return len(required_graphs - found_graphs) == 0

    @classmethod
    def generate_all_graphs(cls):
        """Generate and save all Plotly graphs."""
        logger.info("Generating graphs")
        start_time = datetime.now()
        dataframes = {
            "all": common.read_sql_table("history").sort_index(),
            "real_estate": common.get_real_estate_df(),
            "prices": (common.read_sql_table("schwab_etfs_prices").sort_index()),
            "forex": common.read_sql_table("forex").sort_index(),
            "interest_rate": plot.get_interest_rate_df(),
            "options": stock_options.options_df(),
        }
        with ThreadPoolExecutor() as executor:
            nonranged_graphs = {
                "allocation_profit": executor.submit(
                    lambda: plot.make_allocation_profit_section(
                        dataframes["all"],
                        dataframes["real_estate"],
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json()
                ),
                "change_section": executor.submit(
                    lambda: plot.make_change_section(
                        dataframes["all"],
                        "total",
                        "Total Net Worth Change",
                    ).to_plotly_json()
                ),
                "change_section_no_homes": executor.submit(
                    lambda: plot.make_change_section(
                        dataframes["all"],
                        "total_no_homes",
                        "Total Net Worth Change w/o Real Estate",
                    ).to_plotly_json()
                ),
                "investing_allocation_section": executor.submit(
                    lambda: plot.make_investing_allocation_section().to_plotly_json()
                ),
            }
            if len(dataframes["options"]):
                nonranged_graphs["short_options_section"] = executor.submit(
                    lambda: plot.make_short_options_section(dataframes["options"])
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json()
                )
            ranged_graphs = defaultdict(dict)
            for r in RANGES:
                ranged_graphs["assets_breakdown"][r] = executor.submit(
                    lambda range: plot.make_assets_breakdown_section(
                        cls.limit_and_resample_df(dataframes["all"], range)
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json(),
                    r,
                )
                ranged_graphs["investing_retirement"][r] = executor.submit(
                    lambda range: plot.make_investing_retirement_section(
                        cls.limit_and_resample_df(
                            dataframes["all"][
                                ["pillar2", "ira", "commodities", "etfs"]
                            ],
                            range,
                        )
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json(),
                    r,
                )
                ranged_graphs["real_estate"][r] = executor.submit(
                    lambda range: plot.make_real_estate_section(
                        cls.limit_and_resample_df(
                            dataframes["real_estate"],
                            range,
                        )
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json(),
                    r,
                )
                ranged_graphs["prices"][r] = executor.submit(
                    lambda range: plot.make_prices_section(
                        cls.limit_and_resample_df(dataframes["prices"], range),
                        "Prices",
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json(),
                    r,
                )
                ranged_graphs["forex"][r] = executor.submit(
                    lambda range: plot.make_forex_section(
                        cls.limit_and_resample_df(dataframes["forex"], range),
                        "Forex",
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json(),
                    r,
                )
                ranged_graphs["interest_rate"][r] = executor.submit(
                    lambda range: plot.make_interest_rate_section(
                        cls.limit_and_resample_df(dataframes["interest_rate"], range)
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json(),
                    r,
                )
                ranged_graphs["loan"][r] = executor.submit(
                    lambda range: plot.make_loan_section(
                        lambda df: cls.get_xrange(df, range)
                    )
                    .update_layout(margin=SUBPLOT_MARGIN)
                    .to_plotly_json(),
                    r,
                )

            new_graphs = {"ranged": defaultdict(dict), "nonranged": {}}
            for name, future in nonranged_graphs.items():
                new_graphs["nonranged"][name] = future.result()
            for name, ranged in ranged_graphs.items():
                for r, future in ranged.items():
                    new_graphs["ranged"][name][r] = future.result()
        end_time = datetime.now()
        cls.cached_graphs = new_graphs
        cls.last_updated_time = end_time
        cls.last_generation_duration = end_time - start_time
        cls.latest_datapoint_time = dataframes["all"].index[-1]
        logger.info(f"Graph generation time: {cls.last_generation_duration}")

    @classmethod
    def get_xrange(cls, dataframe, selected_range):
        """Determine time range for selected button."""
        today_time = datetime.now()
        today_time_str = today_time.strftime("%Y-%m-%d")
        xrange = None
        match selected_range:
            case "All":
                xrange = [dataframe.index[0].strftime("%Y-%m-%d"), today_time_str]
            case "2y":
                xrange = [
                    (today_time + relativedelta(years=-2)).strftime("%Y-%m-%d"),
                    today_time_str,
                ]
            case "1y":
                xrange = [
                    (today_time + relativedelta(years=-1)).strftime("%Y-%m-%d"),
                    today_time_str,
                ]
            case "YTD":
                xrange = [today_time.strftime("%Y-01-01"), today_time_str]
            case "6m":
                xrange = [
                    (today_time + relativedelta(months=-6)).strftime("%Y-%m-%d"),
                    today_time_str,
                ]
            case "3m":
                xrange = [
                    (today_time + relativedelta(months=-3)).strftime("%Y-%m-%d"),
                    today_time_str,
                ]
            case "1m":
                xrange = [
                    (today_time + relativedelta(months=-1)).strftime("%Y-%m-%d"),
                    today_time_str,
                ]
        return xrange

    @classmethod
    def limit_and_resample_df(cls, df, selected_range):
        """Limit df to selected range and resample."""
        start, end = cls.get_xrange(df, selected_range)
        df = df[start:end]
        match selected_range:
            case "1m":
                window = None
            case "All" | "2y":
                window = "W"
            case _:
                window = "D"
        if window:
            return df.resample(window).mean().interpolate()
        return df

    def __init__(self, selected_range):
        self.ui_plotly = {}
        self.ui_stats_labels = {}
        self.selected_range = selected_range

    async def update_stats_labels(self):
        timezone = ZoneInfo(
            await ui.run_javascript(
                "Intl.DateTimeFormat().resolvedOptions().timeZone", timeout=10
            )
        )
        self.ui_stats_labels["last_datapoint_time"].set_text(
            f"Latest datapoint: {MainGraphs.latest_datapoint_time.tz_localize('UTC').astimezone(timezone).strftime('%c')}"
        )
        self.ui_stats_labels["last_updated_time"].set_text(
            f"Graphs last updated: {MainGraphs.last_updated_time.astimezone(timezone).strftime('%c')}"
        )
        self.ui_stats_labels["last_generation_duration"].set_text(
            f"Graph generation duration: {MainGraphs.last_generation_duration.total_seconds():.2f}s"
        )
        self.ui_stats_labels["next_generation_time"].set_text(
            "Next generation: "
            + (datetime.now() + relativedelta(seconds=schedule.idle_seconds()))
            .astimezone(timezone)
            .strftime("%c")
        )

    async def create(self):
        """Create all graphs."""
        for name, height in MainGraphs.LAYOUT:
            if name in MainGraphs.cached_graphs["ranged"]:
                graph = MainGraphs.cached_graphs["ranged"][name][self.selected_range]
            else:
                try:
                    graph = MainGraphs.cached_graphs["nonranged"][name]
                except KeyError:
                    continue
            self.ui_plotly[name] = (
                ui.plotly(graph).classes("w-full").style(f"height: {height}")
            )
        with ui.row(align_items="center").classes("w-full"):
            for label in [
                "last_datapoint_time",
                "last_updated_time",
                "last_generation_duration",
                "next_generation_time",
            ]:
                self.ui_stats_labels[label] = ui.label()
        await self.update_stats_labels()

    async def update(self):
        """Update all graphs."""
        for name in MainGraphs.cached_graphs["ranged"]:
            await run.io_bound(
                self.ui_plotly[name].update_figure,
                MainGraphs.cached_graphs["ranged"][name][self.selected_range],
            )
        await self.update_stats_labels()


class IncomeExpenseGraphs:
    """Collection of all income & expense graphs."""

    def get_graphs(self):
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


@ui.page("/i_and_e", title="Income & Expenses")
async def i_and_e_page():
    """Generate income & expenses page."""
    skel = ui.skeleton("QToolbar").classes("w-full")
    await ui.context.client.connected()
    graphs = IncomeExpenseGraphs()
    await graphs.create()
    skel.delete()


@ui.page("/stock_options", title="Stock Options")
def stock_options_page():
    """Stock options."""

    with contextlib.redirect_stdout(io.StringIO()) as output:
        with pandas_options():
            stock_options.main()
            ui.html(f"<PRE>{output.getvalue()}</PRE>")


@ui.page("/latest_values", title="Latest Values")
def latest_values_page():
    """Latest values."""
    output = subprocess.check_output("./latest_values.sh", text=True)
    ui.html(f"<PRE>{output}</PRE>")


@ui.page("/balance_etfs", title="Balance ETFs")
@ui.page("/balance_etfs/{amount}", title="Balance ETFs")
def balance_etfs_page(amount: int = 0):
    """Balance ETFs."""
    with pandas_options():
        df = balance_etfs.get_rebalancing_df(amount)
        ui.html(f"<PRE>{df}</PRE>")


@ui.page("/healthcheck", title="Healthcheck")
def healthcheck_page():
    """Docker healthcheck."""
    ui.html("<PRE>ok</PRE>")


@app.get("/generate_graphs")
def generate_graphs_page():
    background_tasks.create(run.io_bound(schedule.run_all))
    return {"message": "ok"}


async def update_graphs_loop():
    # Kick off run of everything before loop.
    await run.io_bound(schedule.run_all)
    while True:
        await run.io_bound(schedule.run_pending)
        await asyncio.sleep(10)


def lock_and_generate_graphs():
    try:
        with portalocker.Lock(common.LOCKFILE, timeout=common.LOCKFILE_TIMEOUT):
            MainGraphs.generate_all_graphs()
    except portalocker.LockException as e:
        logger.error(f"Failed to acquire portalocker lock: {e}")


if __name__ in {"__main__", "__mp_main__"}:
    pio.templates.default = "plotly_dark"
    schedule.every().hour.at(":05").do(lock_and_generate_graphs)
    app.on_startup(update_graphs_loop)
    ui.run(title="Accounts", dark=True)
