#!/usr/bin/env python3
"""Plot weight graph."""

import io
import re
import subprocess
from typing import Awaitable, Iterable

import pandas as pd
import plotly.io as pio
from loguru import logger
from nicegui import run, ui
from nicegui.elements.table import Table
from plotly.graph_objects import Figure

import balance_etfs
import common
import i_and_e
import ledger_ui
import main_graphs
import stock_options
import stock_options_ui
from main_echarts import ECharts
from main_matplot import Matplots


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
    main_graphs.MainGraphs(common.WalrusDb().db).create()


@ui.page("/image_only")
async def main_page_image_only():
    log_request()
    main_graphs.MainGraphsImageOnly(common.WalrusDb().db).create()


@ui.page("/echarts")
async def echarts_page():
    log_request()
    ECharts().create()


@ui.page("/matplot")
async def matplot_page():
    log_request()
    Matplots(common.WalrusDb().db).create()


@ui.page("/i_and_e", title="Income & Expenses")
async def i_and_e_page():
    """Generate income & expenses page."""
    log_request()
    skel = ui.skeleton("QToolbar").classes("w-full")
    await ui.context.client.connected()
    graphs = IncomeExpenseGraphs()
    await graphs.create()
    skel.delete()


@ui.page("/ledger", title="Ledger")
async def ledger_page():
    """Generate income & expenses page."""
    log_request()
    await ui.context.client.connected()
    columns = 2
    if await ui.run_javascript("window.innerWidth;", timeout=10) < 1000:
        columns = 1
    ledger_ui.LedgerUI().main_page(columns)


@ui.page("/stock_options", title="Stock Options")
async def stock_options_page():
    """Stock options."""
    log_request()
    await stock_options_ui.StockOptionsPage().main_page()


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
        df = balance_etfs.get_rebalancing_df(amount=amount)
        ui.html(f"<PRE>{df}</PRE>")


def floatify(string: str) -> float:
    return float(re.sub(r"[^\d\.-]", "", string))


def body_cell_slot(
    table: Table, column: str, color: str, condition: str, else_color: str = ""
):
    if else_color:
        else_color = f"text-{else_color}"
    table.add_slot(
        f"body-cell-{column}",
        (
            rf"""<q-td key="{column}" :props="props">"""
            rf"""<q-label :class="{condition} ? 'text-{color}' : '{else_color}'">"""
            "{{ props.value }}"
            "</q-label>"
            "</q-td>"
        ),
    )


@ui.page("/transactions", title="Transactions")
async def transactions_page():
    log_request()
    if (data := stock_options.get_options_data()) is None:
        raise ValueError("No options data available")
    bev = data.bev
    columns = 2
    if await ui.run_javascript("window.innerWidth;", timeout=10) < 1000:
        columns = 1
    with ui.grid(columns=columns):
        for account, currency in (
            ("Charles Schwab Brokerage", r"\\$"),
            ("Interactive Brokers", r"\\$"),
            ("Interactive Brokers", "CHF"),
            ("UBS Personal", "CHF"),
        ):
            with ui.column(align_items="center"):
                ui.label(account if currency != "CHF" else f"{account} (CHF)")
                ledger_cmd = rf"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/^{currency}$/' --tail 10 -d 'd<[60 days hence]' --csv-format '%D,%P,%t,%T\n' -n csv"
                df = pd.read_csv(
                    io.StringIO(
                        subprocess.check_output(
                            f"{ledger_cmd} '{account}'", text=True, shell=True
                        )
                    ),
                    header=0,
                    names=["Date", "Payee", "Amount", "Balance"],
                    parse_dates=["Date"],
                    converters={"Amount": floatify, "Balance": floatify},
                )
                for ev in bev:
                    if ev.broker != account or currency == "CHF":
                        continue
                    vals = []
                    for val in ev.values:
                        vals.append(
                            (
                                pd.Timestamp(val.expiration),
                                "Options Expiration",
                                val.value,
                            )
                        )
                    exp_df = pd.DataFrame(
                        data=vals, columns=["Date", "Payee", "Amount"]
                    )
                    df = (
                        pd.concat([df, exp_df])
                        .sort_values("Date")
                        .reset_index(drop=True)
                    )
                    for i in range(1, len(df)):
                        df.loc[i, "Balance"] = (
                            df.loc[i - 1, "Balance"] + df.loc[i, "Amount"]
                        )  # type: ignore
                df["Days"] = -(pd.Timestamp.now() - df["Date"]).dt.days  # type: ignore
                table = ui.table.from_pandas(df.round(2))
                body_cell_slot(table, "Days", "green", "Number(props.value) > 0")
                body_cell_slot(
                    table, "Date", "green", "new Date(props.value) > new Date()"
                )
                body_cell_slot(table, "Balance", "red", "Number(props.value) < 0")


if __name__ in {"__main__", "__mp_main__"}:
    pio.templates.default = common.PLOTLY_THEME
    ui.run(
        title="Accounts",
        dark=True,
        uvicorn_reload_excludes=f".*, .py[cod], .sw.*, ~*, {common.PREFIX}",
    )
