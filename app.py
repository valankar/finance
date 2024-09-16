#!/usr/bin/env python3
"""Plot weight graph."""

import asyncio
import contextlib
import io
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Lock

import pandas as pd
import plotly.io as pio
import portalocker
import schedule
from dateutil.relativedelta import relativedelta
from loguru import logger
from nicegui import app, run, ui

import balance_etfs
import common
import i_and_e
import plot
import stock_options

SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}

RANGES = ["All", "2y", "1y", "YTD", "6m", "3m", "1m"]
SELECTED_RANGE = "1y"


@contextlib.contextmanager
def pandas_options():
    """Set pandas output options."""
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 1000
    ):
        yield


class MainGraphs:
    """Collection of all main graphs."""

    cached_graphs_lock = Lock()
    cached_graphs = {}
    last_updated_time = None
    last_generation_duration = None
    latest_datapoint_time = None

    @classmethod
    def all_graphs_populated(cls):
        with cls.cached_graphs_lock:
            for r in RANGES:
                if r not in cls.cached_graphs:
                    return False
        return True

    @classmethod
    def generate_all_graphs(cls):
        """Generate and save all Plotly graphs."""
        logger.info("Generating graphs")
        start_time = datetime.now()
        dataframes = {
            "all": common.read_sql_table("history").sort_index(),
            "real_estate": common.get_real_estate_df(),
            "prices": (
                common.read_sql_table("schwab_etfs_prices")
                .drop(columns="IBKR", errors="ignore")
                .sort_index()
            ),
            "forex": (
                common.read_sql_table("forex")
                .drop(columns="SGDUSD", errors="ignore")
                .sort_index()
            ),
            "interest_rate": plot.get_interest_rate_df(),
        }
        with ThreadPoolExecutor() as executor:
            norange_graphs = {
                "allocation_profit": executor.submit(
                    plot.make_allocation_profit_section,
                    dataframes["all"],
                    dataframes["real_estate"],
                ),
                "change_section": executor.submit(
                    plot.make_change_section,
                    dataframes["all"],
                    "total",
                    "Total Net Worth Change",
                ),
                "change_section_no_homes": executor.submit(
                    plot.make_change_section,
                    dataframes["all"],
                    "total_no_homes",
                    "Total Net Worth Change w/o Real Estate",
                ),
                "investing_allocation_section": executor.submit(
                    plot.make_investing_allocation_section
                ),
                "short_options_section": executor.submit(
                    plot.make_short_options_section
                ),
            }
            ranged_graphs = {}
            for r in RANGES:
                ranged_graphs[r] = {
                    "assets_breakdown": executor.submit(
                        lambda range: plot.make_assets_breakdown_section(
                            cls.limit_and_resample_df(dataframes["all"], range)
                        ).update_layout(margin=SUBPLOT_MARGIN),
                        r,
                    ),
                    "investing_retirement": executor.submit(
                        lambda range: plot.make_investing_retirement_section(
                            cls.limit_and_resample_df(
                                dataframes["all"][
                                    ["pillar2", "ira", "commodities", "etfs"]
                                ],
                                range,
                            )
                        ).update_layout(margin=SUBPLOT_MARGIN),
                        r,
                    ),
                    "real_estate": executor.submit(
                        lambda range: plot.make_real_estate_section(
                            cls.limit_and_resample_df(
                                dataframes["real_estate"],
                                range,
                            )
                        ).update_layout(margin=SUBPLOT_MARGIN),
                        r,
                    ),
                    "prices": executor.submit(
                        lambda range: plot.make_prices_section(
                            cls.limit_and_resample_df(dataframes["prices"], range),
                            "Prices",
                        ),
                        r,
                    ),
                    "forex": executor.submit(
                        lambda range: plot.make_prices_section(
                            cls.limit_and_resample_df(dataframes["forex"], range),
                            "Forex",
                        ),
                        r,
                    ),
                    "interest_rate": executor.submit(
                        lambda range: plot.make_interest_rate_section(
                            cls.limit_and_resample_df(
                                dataframes["interest_rate"], range
                            )
                        ).update_layout(margin=SUBPLOT_MARGIN),
                        r,
                    ),
                    "loan": executor.submit(
                        lambda range: plot.make_loan_section(
                            lambda df: cls.get_xrange(df, range)
                        ).update_layout(margin=SUBPLOT_MARGIN),
                        r,
                    ),
                }
            new_graphs = {}
            for r in RANGES:
                new_graphs[r] = (
                    (ranged_graphs[r]["assets_breakdown"].result(), "96vh"),
                    (ranged_graphs[r]["investing_retirement"].result(), "75vh"),
                    (ranged_graphs[r]["real_estate"].result(), "96vh"),
                    (norange_graphs["allocation_profit"].result(), "75vh"),
                    (norange_graphs["change_section"].result(), "50vh"),
                    (norange_graphs["change_section_no_homes"].result(), "50vh"),
                    (norange_graphs["investing_allocation_section"].result(), "50vh"),
                    (ranged_graphs[r]["prices"].result(), "50vh"),
                    (ranged_graphs[r]["interest_rate"].result(), "40vh"),
                    (ranged_graphs[r]["loan"].result(), "40vh"),
                    (norange_graphs["short_options_section"].result(), "50vh"),
                )
        end_time = datetime.now()
        with cls.cached_graphs_lock:
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
    def smooth_df(cls, df, selected_range):
        """Resample df according to range."""
        match selected_range:
            case "1m":
                window = None
            case "All" | "2y":
                window = "7D"
            case _:
                window = "D"
        if window:
            return df.resample(window).mean().interpolate()
        return df

    @classmethod
    def limit_and_resample_df(cls, df, selected_range):
        """Limit df to selected range and resample."""
        start, end = cls.get_xrange(df, selected_range)
        return cls.smooth_df(df[start:end], selected_range)

    def __init__(self):
        self.ui_plotly = []
        self.ui_stats_labels = {}

    def update_stats_labels(self):
        self.ui_stats_labels["last_datapoint_time"].set_text(
            f"Latest datapoint: {MainGraphs.latest_datapoint_time.strftime('%c')}"
        )
        self.ui_stats_labels["last_updated_time"].set_text(
            f"Graphs last updated: {MainGraphs.last_updated_time.strftime('%c')}"
        )
        self.ui_stats_labels["last_generation_duration"].set_text(
            f"Graph generation duration: {MainGraphs.last_generation_duration.total_seconds():.2f}s"
        )
        self.ui_stats_labels["next_generation_time"].set_text(
            "Next generation: "
            + (
                datetime.now() + relativedelta(seconds=schedule.idle_seconds())
            ).strftime("%c")
        )

    async def create(self):
        """Create all graphs."""
        with MainGraphs.cached_graphs_lock:
            for graph, height in MainGraphs.cached_graphs[SELECTED_RANGE]:
                self.ui_plotly.append(
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
            self.update_stats_labels()

    async def update(self):
        """Update all graphs."""
        with MainGraphs.cached_graphs_lock:
            for i, (graph, _) in enumerate(MainGraphs.cached_graphs[SELECTED_RANGE]):
                self.ui_plotly[i].update_figure(graph)
            self.update_stats_labels()


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
            tabs.bind_value(globals(), "SELECTED_RANGE")

    await ui.context.client.connected()
    graphs = MainGraphs()
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
    schedule.every(30).minutes.do(lock_and_generate_graphs)
    app.on_startup(update_graphs_loop)
    ui.run(title="Accounts", dark=True)
