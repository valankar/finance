#!/usr/bin/env python3
"""Plot weight graph."""

import contextlib
import io
import subprocess
from datetime import datetime

import pandas as pd
import plotly.io as pio
from dateutil.relativedelta import relativedelta
from loguru import logger
from nicegui import run, ui

import balance_etfs
import common
import i_and_e
import plot
import stock_options

SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}

SELECTED_RANGE = "1y"


def get_frequency(selected_range):
    """Determine frequency from selected range button."""
    match selected_range:
        case "All" | "2y":
            frequency = "weekly"
        case "1m":
            frequency = "hourly"
        case _:
            frequency = "daily"
    return frequency


def get_xrange(dataframe, selected_range):
    """Determine time range for selected button."""
    today_time = datetime.now()
    last_time = dataframe.index[-1].strftime("%Y-%m-%d")
    match selected_range:
        case "All":
            return [dataframe.index[0].strftime("%Y-%m-%d"), last_time]
        case "2y":
            return [
                (today_time + relativedelta(years=-2)).strftime("%Y-%m-%d"),
                last_time,
            ]
        case "1y":
            return [
                (today_time + relativedelta(years=-1)).strftime("%Y-%m-%d"),
                last_time,
            ]
        case "YTD":
            return [today_time.strftime("%Y-01-01"), last_time]
        case "6m":
            return [
                (today_time + relativedelta(months=-6)).strftime("%Y-%m-%d"),
                last_time,
            ]
        case "3m":
            return [
                (today_time + relativedelta(months=-3)).strftime("%Y-%m-%d"),
                last_time,
            ]
        case "1m":
            return [
                (today_time + relativedelta(months=-1)).strftime("%Y-%m-%d"),
                last_time,
            ]


def load_all_df(frequency):
    """Load all dataframe."""
    return common.read_sql_table_resampled_last(
        "history", extra_cols=["total", "total_no_homes"], frequency=frequency
    )


@contextlib.contextmanager
def pandas_options():
    """Set pandas output options."""
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 1000
    ):
        yield


class MainGraphs:
    """Collection of all main graphs."""

    def __init__(self):
        self.ui_plotly = []

    def load_real_estate_df(self, frequency):
        """Load real estate dataframe."""
        # Fix issue with missing datapoints.
        match frequency:
            case "weekly":
                resample = "W"
            case "daily":
                resample = "D"
            case "hourly":
                resample = "h"
        return (
            common.get_real_estate_df(frequency=frequency)
            .resample(resample)
            .last()
            .interpolate()
        )

    def get_graphs(self):
        """Generate Plotly graphs. This calls subprocess."""
        frequency = get_frequency(SELECTED_RANGE)
        all_df = load_all_df(frequency)
        all_start, all_end = get_xrange(all_df, SELECTED_RANGE)
        invret_df = plot.get_investing_retirement_df(load_all_df(frequency))
        invret_start, invret_end = get_xrange(invret_df, SELECTED_RANGE)
        real_estate_df = self.load_real_estate_df(frequency)
        real_estate_start, real_estate_end = get_xrange(real_estate_df, SELECTED_RANGE)
        prices_df = common.reduce_merge_asof(
            [common.read_sql_table_resampled_last("forex", frequency=frequency)]
        )
        prices_start, prices_end = get_xrange(prices_df, SELECTED_RANGE)
        intrate_df = plot.get_interest_rate_df(get_frequency(SELECTED_RANGE))
        intrate_start, intrate_end = get_xrange(intrate_df, SELECTED_RANGE)
        funcs = (
            (
                lambda: plot.make_assets_breakdown_section(
                    all_df[all_start:all_end]
                ).update_layout(margin=SUBPLOT_MARGIN),
                "96vh",
            ),
            (
                lambda: plot.make_investing_retirement_section(
                    invret_df[invret_start:invret_end]
                ).update_layout(margin=SUBPLOT_MARGIN),
                "75vh",
            ),
            (
                lambda: plot.make_real_estate_section(
                    real_estate_df[real_estate_start:real_estate_end]
                ).update_layout(margin=SUBPLOT_MARGIN),
                "96vh",
            ),
            (
                lambda: plot.make_allocation_profit_section(all_df, real_estate_df),
                "75vh",
            ),
            (
                lambda: plot.make_change_section(
                    all_df, "total", "Total Net Worth Change"
                ),
                "50vh",
            ),
            (
                lambda: plot.make_change_section(
                    all_df, "total_no_homes", "Total Net Worth Change w/o Real Estate"
                ),
                "50vh",
            ),
            (
                plot.make_investing_allocation_section,
                "50vh",
            ),
            (
                lambda: plot.make_prices_section(
                    prices_df[prices_start:prices_end]
                ).update_layout(margin=SUBPLOT_MARGIN),
                "50vh",
            ),
            (
                lambda: plot.make_interest_rate_section(
                    intrate_df[intrate_start:intrate_end]
                ).update_layout(margin=SUBPLOT_MARGIN),
                "40vh",
            ),
            (
                lambda: plot.make_loan_section(
                    lambda df: get_xrange(df, SELECTED_RANGE)
                ).update_layout(margin=SUBPLOT_MARGIN),
                "40vh",
            ),
            (
                plot.make_short_options_section,
                "50vh",
            ),
        )
        return ((run.io_bound(f), h) for f, h in funcs)

    async def create(self):
        """Create all graphs."""
        for graph, height in self.get_graphs():
            self.ui_plotly.append(
                ui.plotly(await graph).classes("w-full").style(f"height: {height}")
            )

    async def update(self):
        """Update all graphs."""
        for i, (graph, _) in enumerate(self.get_graphs()):
            self.ui_plotly[i].update_figure(await graph)


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
    with ui.footer().classes("transparent q-py-none"):
        with ui.tabs().classes("w-full") as tabs:
            for timerange in ["All", "2y", "1y", "YTD", "6m", "3m", "1m"]:
                ui.tab(timerange).disable()
            tabs.bind_value(globals(), "SELECTED_RANGE")

    async def tabs_disable_and_run(func):
        for tab in tabs.descendants():
            tab.disable()
        await func()
        for tab in tabs.descendants():
            tab.enable()

    await ui.context.client.connected()
    graphs = MainGraphs()
    await tabs_disable_and_run(graphs.create)
    tabs.on_value_change(lambda: tabs_disable_and_run(graphs.update))


@ui.page("/i_and_e", title="Income & Expenses")
async def i_and_e_page():
    """Generate income & expenses page."""
    await ui.context.client.connected()
    graphs = IncomeExpenseGraphs()
    await graphs.create()


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


pio.templates.default = "plotly_dark"
ui.run(title="Accounts", dark=True)
