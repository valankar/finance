#!/usr/bin/env python3
"""Plot weight graph."""

import asyncio
import contextlib
import io
import os
import re
import subprocess
from datetime import date, datetime
from typing import Awaitable, Iterable

import humanize
import pandas as pd
import plotly.io as pio
from fastapi import Request
from loguru import logger
from nicegui import app, background_tasks, html, run, ui
from plotly.graph_objects import Figure
from starlette.responses import RedirectResponse

import balance_etfs
import common
import futures
import i_and_e
import latest_values
import ledger_ui
import main_graphs
import plot
import stock_options_ui
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
    main_graphs.MainGraphs().create()


@ui.page("/image_only")
async def main_page_image_only():
    log_request()
    main_graphs.MainGraphsImageOnly().create()


@ui.page("/matplot")
async def matplot_page():
    log_request()
    Matplots().create()


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
    await ledger_ui.LedgerUI().main_page()


@ui.page("/stock_options", title="Stock Options")
async def stock_options_page():
    """Stock options."""
    log_request()
    stock_options_ui.StockOptionsPage().main_page()


@ui.page("/latest_values", title="Latest Values")
def latest_values_page():
    """Latest values."""
    log_request()
    with contextlib.redirect_stdout(io.StringIO()) as output:
        with common.pandas_options():
            latest_values.main()
    html.pre(output.getvalue())


@ui.page("/balance_etfs", title="Balance ETFs")
async def balance_etfs_page():
    """Balance ETFs."""
    log_request()
    await ui.context.client.connected()
    adjustments = {}
    ticker_vals = []
    df = balance_etfs.get_rebalancing_df(0)
    with ui.grid(columns=2).classes("w-full"):
        table = ui.table.from_pandas(df.reset_index(names="category"))
        graph = ui.plotly(plot.make_investing_allocation_section(df))

    def validate(x: str):
        if not x:
            return None
        try:
            int(x)
        except ValueError:
            return "Only int allowed"
        return None

    async def update():
        adjustment = {k: int(v.value) for k, v in adjustments.items() if v.value}
        amt = 0
        if amount.value:
            amt = int(amount.value)
        for t, v in ticker_vals:
            if t.value and v.value:
                adjustment[t.value] = int(v.value)
        if adjustment:
            ui.notify(f"Adjustments: {adjustment}")
        df = balance_etfs.get_rebalancing_df(amt, adjustment)
        table.update_from_pandas(df.reset_index(names="category"))
        graph.update_figure(plot.make_investing_allocation_section(df))

    amount = ui.input(label="Amount", validation=validate).on("keydown.enter", update)

    with ui.grid(rows=1, columns=2).classes("w-full"):
        with ui.card(align_items="center").classes("w-full"):
            ui.label("Category Adjustments")
            with ui.grid(columns=2).classes("w-full"):
                for category in df.index:
                    adjustments[category] = ui.input(
                        label=category, validation=validate
                    ).on("keydown.enter", update)
        with ui.card(align_items="center").classes("w-full"):
            ui.label("Ticker Adjustments")
            with ui.grid(columns=2).classes("w-full"):
                for _ in range(4):
                    ticker = ui.input(label="TICKER").on("keydown.enter", update)
                    val = ui.input(label="VALUE", validation=validate).on(
                        "keydown.enter", update
                    )
                    ticker_vals.append((ticker, val))


@ui.page("/futures", title="Futures")
async def futures_page():
    log_request()
    data = futures.Futures().redis_data
    with ui.row().classes("items-center"):
        ui.label(f"Staleness: {humanize.naturaldelta(datetime.now() - data.updated)}")
    html.pre(data.main_output)


def floatify(string: str) -> float:
    return float(re.sub(r"[^\d\.-]", "", string))


@ui.page("/regenerate")
def regenerate_page():
    with ui.row():
        button = ui.button("Run")
        flush_cache = ui.checkbox("Flush ticker cache", value=True)
    log = ui.log().classes("w-full h-[90vh]")

    async def run_command():
        button.disable()
        cmd = [
            "./code/accounts/finance_hourly.py",
            "--no-daily",
        ]
        if not flush_cache.value:
            cmd.append("--no-flush-cache")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        while process.stdout is not None:
            line = await process.stdout.readline()
            if not line:
                break
            log.push(line.decode().strip())

        await process.wait()
        if process.returncode == 0:
            with log:
                ui.navigate.to("/")
        else:
            log.push(f"Process finished with return code {process.returncode}")
            button.enable()

    button.on_click(lambda: background_tasks.create(run_command()))


@ui.page("/transactions", title="Transactions")
async def transactions_page():
    log_request()
    await ui.context.client.connected()
    futures_by_account = (
        futures.Futures().futures_df.groupby(level="account")["value"].sum()
    )
    with ui.grid().classes("md:grid-cols-3"):
        for account, currency in (
            ("Charles Schwab Brokerage", r"\\$"),
            ("Interactive Brokers", r"\\$"),
            ("Interactive Brokers", "CHF"),
            ("UBS Personal Account", "CHF"),
            ("Assets:Cash", "CHF"),
            ("Apple Card", r"\\$"),
            ("Bank of America Travel Rewards Credit Card", r"\\$"),
            ("American Express Investor Card", r"\\$"),
            ("UBS Visa", "CHF"),
            ("Wise", "CHF"),
            ("Revolut", "CHF"),
        ):
            with ui.column(align_items="center"):
                ui.label(account if currency != "CHF" else f"{account} (CHF)")
                ledger_cmd = rf"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/^{currency}$/' --tail 10 -d 'd<[60 days hence]' --csv-format '%D,%P,%t,%T\n' -n csv"
                df = pd.read_csv(
                    io.StringIO(
                        subprocess.check_output(
                            f"{ledger_cmd} '{account}'$", text=True, shell=True
                        )
                    ),
                    header=0,
                    names=["Date", "Payee", "Amount", "Balance"],
                    parse_dates=["Date"],
                    converters={"Amount": floatify, "Balance": floatify},
                )
                vals = []
                if currency.endswith("$") and account in futures_by_account:
                    vals.append(
                        (
                            pd.Timestamp(date.today()),
                            "Futures value",
                            futures_by_account[account],
                        )
                    )
                if vals:
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
                stock_options_ui.body_cell_slot(
                    table, "Days", "green", "Number(props.value) > 0"
                )
                stock_options_ui.body_cell_slot(
                    table, "Date", "green", "new Date(props.value) > new Date()"
                )
                stock_options_ui.body_cell_slot(
                    table, "Balance", "red", "Number(props.value) < 0"
                )


@ui.page("/schwab_login")
async def schwab_login(request: Request) -> RedirectResponse:
    if not (uri := os.environ.get("SCHWAB_CALLBACK_URI")):
        raise ValueError("SCHWAB_CALLBACK_URI not defined")
    return await common.schwab_conn.oauth.authorize_redirect(request, uri)


@app.get("/callback")
async def schwab_callback(request: Request) -> RedirectResponse:
    token = await common.schwab_conn.oauth.authorize_access_token(request)
    common.schwab_conn.write_token(token)
    return RedirectResponse("/")


@ui.page("/screenshot", title="Screenshot")
def screenshot_page():
    ui.image(f"{common.PUBLIC_HTML}/screenshot.png")


if __name__ in {"__main__", "__mp_main__"}:
    pio.templates.default = common.PLOTLY_THEME
    ui.run(
        title="Finance",
        dark=True,
        uvicorn_reload_excludes=f".*, .py[cod], .sw.*, ~*, {common.PREFIX}",
        storage_secret="finance",
        reconnect_timeout=30,
    )
