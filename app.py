#!/usr/bin/env python3
"""Plot weight graph."""

import asyncio
import contextlib
import io
import os
import pickle
import re
import subprocess
from datetime import date
from pathlib import Path
from typing import Awaitable, Iterable

import pandas as pd
import plotly.express as px
import plotly.io as pio
from fastapi import Request
from loguru import logger
from nicegui import app, background_tasks, html, run, ui
from plotly.graph_objects import Figure
from starlette.responses import RedirectResponse

import balance_etfs
import brokerages
import common
import futures
import i_and_e
import latest_values
import ledger_amounts
import ledger_ui
import main_graphs
import margin_loan
import plot
import stock_options
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
            "URL: {url} User: {user}, IP: {ip}, User-Agent: {agent}",
            url=request.url,
            user=headers.get("remote-email", "unknown"),
            ip=headers.get("x-forwarded-for", "unknown"),
            agent=headers.get("user-agent", "unknown"),
        )


@ui.page("/")
async def main_page():
    """Generate main UI."""
    log_request()
    mg = main_graphs.MainGraphs()
    mg.create()

    # Check for graph updates and reload if changed
    if updated := mg.plotly_graphs.get("updated"):
        last_update = pickle.loads(updated)
    else:
        last_update = None

    async def check_for_update():
        nonlocal last_update
        if current := mg.plotly_graphs.get("updated"):
            stored = pickle.loads(current)
            if last_update and stored > last_update:
                ui.navigate.to("/")

    ui.timer(30, check_for_update)


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
async def ledger_page(request: Request):
    """Generate income & expenses page."""
    log_request()
    if not authorized(request):
        return
    await ledger_ui.LedgerUI().main_page()


def authorized(request: Request) -> bool:
    if request.headers.get("remote-email", None) != "valankar@gmail.com":
        ui.label("Unauthorized")
        return False
    return True


def show_error():
    with ui.context.client.content.clear():
        # Capture loguru's formatted exception output
        log_capture = io.StringIO()
        handler_id = logger.add(log_capture, enqueue=True, backtrace=False)
        logger.exception("Error occurred")
        logger.remove(handler_id)
        ui.code(log_capture.getvalue())


@ui.page("/hourly_logs", title="Hourly Logs")
def hourly_logs_page():
    log_request()
    ui.code(Path(common.HOURLY_LOGFILE).read_text())


@ui.page("/stock_options", title="Stock Options")
async def stock_options_page():
    """Stock options."""
    log_request()
    await ui.context.client.connected()
    ui.on_exception(show_error)
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
    ui.on_exception(show_error)

    log_area = ui.log().classes("w-full")
    log_area.set_visibility(False)

    show_log = False

    def on_log_checkbox_changed(e):
        nonlocal show_log
        show_log = e.value
        log_area.set_visibility(e.value)

    async def run_with_log(func, *args):
        handler_id = logger.add(lambda message: log_area.push(message), enqueue=True)
        try:
            log_area.set_visibility(True)
            result = await run.io_bound(func, *args)
            return result
        finally:
            log_area.set_visibility(show_log)
            logger.remove(handler_id)

    def get_main_out(
        rebalancing_df: pd.DataFrame,
    ):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            with common.pandas_options():
                balance_etfs.options_rebalancing(rebalancing_df, None)
                balance_etfs.futures_rebalancing(rebalancing_df, None)
            return output.getvalue()

    adjustments = {}
    ticker_vals = []
    df = await run_with_log(balance_etfs.get_rebalancing_df, 0)
    main_out_text = await run_with_log(get_main_out, df)

    with ui.grid(columns=2).classes("w-full"):
        table = ui.table.from_pandas(df.reset_index(names="category"))
        graph = ui.plotly(plot.make_investing_rebalancing_bar(df))

    def validate(x: str):
        if not x:
            return None
        try:
            int(x)
        except ValueError:
            return "Only int allowed"
        return None

    main_out = ui.label(main_out_text).style("white-space: pre; font-family: monospace")

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
            ui.notify(f"Total: {sum(adjustment.values())}")
        df = await run_with_log(balance_etfs.get_rebalancing_df, amt, adjustment)
        table.update_from_pandas(df.reset_index(names="category"))
        graph.update_figure(plot.make_investing_rebalancing_bar(df))
        main_out.text = await run_with_log(get_main_out, df)

    with ui.row().classes("items-center"):
        amount = ui.input(label="Amount", validation=validate).on(
            "keydown.enter", update
        )
        ui.checkbox("Show log", value=False).on_value_change(on_log_checkbox_changed)

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
    stock_options_ui.StockOptionsPage().main_page()


@ui.page("/futures", title="Futures")
async def futures_page():
    log_request()
    await ui.context.client.connected()
    ui.on_exception(show_error)
    html.pre(await run.io_bound(futures.Futures().get_summary))


def floatify(string: str) -> float:
    return float(re.sub(r"[^\d\.-]", "", string))


@ui.page("/regenerate")
def regenerate_page():
    with ui.row():
        button = ui.button("Run")
    log = ui.log().classes("w-full h-[90vh]")

    async def run_command():
        button.disable()
        cmd = [
            "./code/accounts/finance_hourly.py",
            "--no-daily",
        ]

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
async def transactions_page(request: Request):
    log_request()
    await ui.context.client.connected()
    ui.on_exception(show_error)
    if not authorized(request):
        return
    futures_by_account = (
        futures.Futures()
        .futures_df.groupby(level="account")[["value", "margin_requirement"]]
        .sum()
    )
    o: stock_options.OptionsAndSpreads = stock_options.get_options_and_spreads()
    options_df = pd.concat([x.df for x in o.short_options])
    with ui.grid().classes("md:grid-cols-3"):
        for account, currency in (
            (common.Brokerage.SCHWAB, r"\\$"),
            ("Charles Schwab Checking", r"\\$"),
            (common.Brokerage.IBKR, r"\\$"),
            (common.Brokerage.IBKR, "CHF"),
            ("UBS Personal Account", "CHF"),
            ("Assets:Cash", "CHF"),
            ("Apple Card", r"\\$"),
            ("Bank of America Cash Rewards Credit Card", r"\\$"),
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
                if currency.endswith("$") and account in futures_by_account.index:
                    vals.append(
                        (
                            pd.Timestamp(date.today()),
                            "Futures value - margin",
                            futures_by_account.loc[account]["value"]
                            - futures_by_account.loc[account]["margin_requirement"],
                        )
                    )
                if account == common.Brokerage.SCHWAB:
                    csp = stock_options.short_put_exposure(options_df, account)
                    vals.append(
                        (
                            pd.Timestamp(date.today()),
                            "CSP Requirement",
                            csp,
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
                        balance = df.at[i - 1, "Balance"]
                        amount = df.at[i, "Amount"]
                        df.at[i, "Balance"] = balance + amount  # type: ignore
                df["Days"] = -(pd.Timestamp.now() - pd.to_datetime(df["Date"])).dt.days
                table = ui.table.from_pandas(df.round({"Amount": 2, "Balance": 2}))
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
    return await common.Schwab().oauth.authorize_redirect(request, uri)


@app.get("/callback")
async def schwab_callback(request: Request) -> RedirectResponse:
    s = common.Schwab()
    token = await s.oauth.authorize_access_token(request)
    s.write_token(token)
    return RedirectResponse("/")


@ui.page("/brokerages", title="Current Brokerage Values")
async def brokerages_page():
    log_request()
    await ui.context.client.connected()
    ui.on_exception(show_error)
    df = brokerages.load_df()
    cutoff = df.index.max() - pd.Timedelta(hours=12)
    df = df[df.index >= cutoff]
    new_entry = {}
    for b, b_df in (await run.io_bound(margin_loan.get_balances_broker)).items():
        new_entry[b] = b_df["Total"].iloc[-1]
    df.loc[pd.Timestamp.now()] = new_entry
    p = plot.make_brokerage_total_section(df, common.SUBPLOT_MARGIN)
    ui.plotly(p).classes("w-full")


@ui.page("/swygx_holdings", title="SWYGX Holdings History")
async def swygx_holdings_page():
    """Display SWYGX holdings history as a stacked area plot and pie chart."""
    log_request()
    await ui.context.client.connected()

    def get_holdings_graphs():
        df = common.read_sql_table("swygx_holdings")
        holdings_cols = [c for c in df.columns if c != "date"]

        # Stacked area plot
        area_fig = px.area(
            df.reset_index(),
            x="date",
            y=holdings_cols,
            title="SWYGX Holdings History",
            labels={"value": "Allocation %", "variable": "Holding"},
        )
        area_fig.update_layout(
            yaxis_title="Allocation %",
            xaxis_title="",
        )

        # Pie chart of latest values
        latest = df.iloc[-1][holdings_cols]
        pie_fig = px.pie(
            names=latest.index,
            values=latest.values,
            title=f"Latest Allocations ({df.index[-1].strftime('%Y-%m-%d')})",
        )
        pie_fig.update_traces(textinfo="percent+label")

        return area_fig, pie_fig

    area_graph, pie_graph = await run.io_bound(get_holdings_graphs)
    with ui.grid().classes("w-full gap-4 grid-cols-2"):
        ui.plotly(area_graph).classes("w-full").style("height: 80vh")
        ui.plotly(pie_graph).classes("w-full").style("height: 80vh")


@ui.page("/stocks_by_brokerage", title="Stocks by Brokerage")
async def stocks_by_brokerage_page():
    """Display pie charts of stock/ETF holdings as percentage of portfolio for each brokerage."""
    log_request()
    await ui.context.client.connected()

    def get_brokerage_stock_pies():
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        # Get stock holdings for each brokerage
        brokerage_data = {}
        for brokerage in common.Brokerage:
            amounts = ledger_amounts.get_etfs_amounts(brokerage.value)
            if amounts:
                prices = common.get_tickers(set(amounts.keys()))
                holdings = []
                for ticker, shares in amounts.items():
                    if ticker in prices:
                        value = shares * prices[ticker]
                        holdings.append({"ticker": ticker, "value": value})
                if holdings:
                    brokerage_data[brokerage.value] = pd.DataFrame(holdings)

        if not brokerage_data:
            return None

        # Create subplots with one pie chart per brokerage
        n_brokerages = len(brokerage_data)
        fig = make_subplots(
            rows=1,
            cols=n_brokerages,
            subplot_titles=list(brokerage_data.keys()),
            specs=[[{"type": "pie"}] * n_brokerages],
        )

        for i, (brokerage_name, df) in enumerate(brokerage_data.items(), start=1):
            pie = go.Pie(
                labels=df["ticker"],
                values=df["value"],
                textinfo="percent+label",
                name=brokerage_name,
            )
            fig.add_trace(pie, row=1, col=i)

        fig.update_layout(
            title={
                "text": "Stock/ETF Holdings by Brokerage",
                "x": 0.5,
                "xanchor": "center",
            },
            showlegend=False,
        )

        return fig

    fig = await run.io_bound(get_brokerage_stock_pies)
    if fig:
        ui.plotly(fig).classes("w-full").style("height: 80vh")
    else:
        ui.label("No stock data available for brokerages.")


@ui.page("/real_estate", title="Real Estate")
async def real_estate_page():
    """Display raw real estate values from Redfin, Zillow, and Taxes."""
    log_request()
    await ui.context.client.connected()

    # Time period options: days mapping (None = all)
    periods = {"1 Week": 7, "1 Month": 30, "1 Year": 365, "All": None}
    selected_period = (
        ui.radio(options=list(reversed(periods.keys())), value="All")
        .props("inline")
        .classes("w-full justify-center")
    )

    def get_fig(days):
        return plot.make_real_estate_raw_section(common.SUBPLOT_MARGIN, days)

    fig = await run.io_bound(get_fig, None)
    plotly_graph = ui.plotly(fig).classes("w-full").style("height: 85vh")

    def on_change(e):
        days = periods[e.value]
        background_tasks.create(update_graph(days))

    async def update_graph(days):
        new_fig = await run.io_bound(get_fig, days)
        plotly_graph.update_figure(new_fig)

    selected_period.on_value_change(on_change)


if __name__ in {"__main__", "__mp_main__"}:
    pio.templates.default = common.PLOTLY_THEME
    ui.run(
        title="Finance",
        dark=True,
        uvicorn_reload_excludes=f".*, .py[cod], .sw.*, ~*, {common.PREFIX}",
        storage_secret="finance",
        reconnect_timeout=30,
    )
