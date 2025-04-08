import asyncio
import contextlib
import io
import typing
from datetime import date, datetime

import pandas as pd
import plotly.express as px
from loguru import logger
from nicegui import background_tasks, run, ui
from plotly.graph_objs import Figure

import common
import plot
import stock_options
from app import body_cell_slot


class OptionsData(typing.NamedTuple):
    opts: stock_options.OptionsAndSpreads
    bev: list[stock_options.BrokerExpirationValues]
    main_output: str
    updated: datetime


class RowGraphPair(typing.NamedTuple):
    row: dict[str, typing.Any]
    graph: typing.Optional[Figure]


class StockOptionsPage:
    options_data: typing.ClassVar[typing.Optional[OptionsData]] = None
    PL_GRID_COLUMNS: typing.ClassVar[int] = 3

    def __init__(self):
        self.refresh_button: typing.Optional[ui.button] = None

    def make_short_call_pl_graph(
        self,
        d: stock_options.ShortCallDetails,
    ) -> Figure:
        cd = d.details
        x = [d.strike, d.strike + cd.contract_price_per_share]
        y = [-cd.contract_price, 0]
        line_color = "green"
        if cd.ticker_price < d.strike:
            x.insert(0, cd.ticker_price)
            y.insert(0, -cd.contract_price)
        elif cd.ticker_price > (d.strike + cd.contract_price_per_share):
            x.append(cd.ticker_price)
            y.append(
                self.find_y_intercept(
                    d.strike,
                    -cd.contract_price,
                    d.strike + cd.contract_price_per_share,
                    0,
                    cd.ticker_price,
                )
            )
            line_color = "red"
        else:
            # Price is between strike and strike + contract_price_per_share.
            x.insert(1, cd.ticker_price)
            y.insert(
                1,
                self.find_y_intercept(
                    d.strike,
                    -cd.contract_price,
                    d.strike + cd.contract_price_per_share,
                    0,
                    cd.ticker_price,
                ),
            )
        fig = px.line(x=x, y=y, markers=True)
        plot.centered_title(fig, f"{cd.account} {cd.ticker} {cd.expiration} Short Call")
        self.add_ticker_vline(fig, cd.ticker_price, line_color)
        fig.update_xaxes(title_text=f"{cd.ticker} Price")
        fig.update_yaxes(title_text="P/L")
        return fig

    def make_short_calls_table(self, options_data: OptionsData):
        rows: list[RowGraphPair] = []
        d: stock_options.ShortCallDetails
        for d in options_data.opts.short_calls:
            cd: stock_options.CommonDetails = d.details
            expiration = f"{cd.expiration} ({(cd.expiration - date.today()).days}d)"
            graph = self.make_short_call_pl_graph(d)
            rows.append(
                RowGraphPair(
                    row={
                        "account": cd.account,
                        "name": f"{cd.ticker} {d.strike:.0f}",
                        "expiration": expiration,
                        "type": "Short Call",
                        "strike": d.strike,
                        "count": cd.count,
                        "intrinsic value": f"{cd.intrinsic_value:.0f}",
                        "contract price": f"{cd.contract_price:.0f}",
                        "half mark": f"{cd.half_mark:.2f}",
                        "double mark": f"{cd.double_mark:.2f}",
                        "quote": f"{cd.quote:.0f}",
                        "profit option": f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})",
                        "profit stock": f"{d.profit_stock:.2f}",
                        "ticker price": cd.ticker_price,
                    },
                    graph=graph,
                )
            )
        if rows:
            self.make_spread_section(
                "Short Calls", rows, profit_color_col="profit option"
            )

    def make_complex_options_table(
        self,
        options_data: OptionsData,
    ):
        rows: list[RowGraphPair] = []
        for spreads, spread_type in (
            (options_data.opts.bull_put_spreads_no_ic, "Bull Put"),
            (options_data.opts.bear_call_spreads_no_ic, "Bear Call"),
        ):
            for d in spreads:
                sd: stock_options.SpreadDetails = d.details
                cd: stock_options.CommonDetails = sd.details
                name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
                risk = f"{sd.risk:.0f}"
                half_mark = f"{cd.half_mark:.2f}"
                double_mark = f"{cd.double_mark:.2f}"
                profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
                if spread_type == "Bull Put":
                    graph = self.make_bull_put_pl_graph(sd)
                elif spread_type == "Bear Call":
                    graph = self.make_bear_call_pl_graph(sd)
                rows.append(
                    RowGraphPair(
                        row={
                            "account": cd.account,
                            "name": name,
                            "expiration": f"{cd.expiration} ({(cd.expiration - date.today()).days}d)",
                            "type": spread_type,
                            "count": cd.count,
                            "intrinsic value": f"{cd.intrinsic_value:.0f}",
                            "maximum loss": risk,
                            "contract price": f"{cd.contract_price:.0f}",
                            "half mark": half_mark,
                            "double mark": double_mark,
                            "quote": f"{cd.quote:.0f}",
                            "profit": profit,
                            "ticker price": cd.ticker_price,
                        },
                        graph=graph,
                    )
                )
        if rows:
            self.make_spread_section("Vertical Spreads", rows)
            rows.clear()

        for d in options_data.opts.iron_condors:
            icd: stock_options.IronCondorDetails = d.details
            cd: stock_options.CommonDetails = icd.details
            name = f"{cd.ticker} {icd.low_put_strike:.0f}/{icd.high_put_strike:.0f}/{icd.low_call_strike:.0f}/{icd.high_call_strike:.0f}"
            risk = f"{icd.risk:.0f}"
            half_mark = f"{cd.half_mark:.2f}"
            double_mark = f"{cd.double_mark:.2f}"
            profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
            rows.append(
                RowGraphPair(
                    row={
                        "account": cd.account,
                        "name": name,
                        "expiration": f"{cd.expiration} ({(cd.expiration - date.today()).days}d)",
                        "type": "Iron Condor",
                        "count": cd.count,
                        "intrinsic value": f"{cd.intrinsic_value:.0f}",
                        "maximum loss": risk,
                        "contract price": f"{cd.contract_price:.0f}",
                        "half mark": half_mark,
                        "double mark": double_mark,
                        "quote": f"{cd.quote:.0f}",
                        "profit": profit,
                        "ticker price": cd.ticker_price,
                    },
                    graph=self.make_iron_condor_pl_graph(icd),
                )
            )
        if rows:
            self.make_spread_section("Iron Condors", rows)
            rows.clear()

        self.make_box_spread_sections("Box Spreads", options_data.opts.box_spreads)
        self.make_box_spread_sections(
            "Old Box Spreads", options_data.opts.old_box_spreads
        )

    def make_box_spread_sections(
        self, title: str, box_spreads: list[stock_options.BoxSpread]
    ):
        rows = []
        for d in box_spreads:
            bd: stock_options.BoxSpreadDetails = d.details
            sd: stock_options.SpreadDetails = d.details.details
            cd: stock_options.CommonDetails = sd.details
            name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
            rows.append(
                RowGraphPair(
                    row={
                        "account": cd.account,
                        "name": name,
                        "expiration": f"{cd.expiration} ({(cd.expiration - date.today()).days}d)",
                        "type": "Box Spread",
                        "count": cd.count,
                        "intrinsic value": f"{cd.intrinsic_value:.0f}",
                        "contract price": f"{cd.contract_price:.0f}",
                        "cost": f"{cd.intrinsic_value - cd.contract_price:.0f}",
                        "loan term": f"{bd.loan_term_days}d",
                        "apy": f"{bd.apy:.2%}",
                    },
                    graph=None,
                )
            )
        if rows:
            self.make_spread_section(title, rows, colored=False)

    def make_spread_section(
        self,
        title: str,
        rows: list[RowGraphPair],
        colored: bool = True,
        profit_color_col: str = "profit",
    ):
        ui.label(title)
        sorted_rows = sorted(
            rows,
            key=lambda x: (
                x.row["account"],
                x.row["expiration"],
                x.row["name"],
                x.row["type"],
            ),
        )
        table = ui.table(rows=[row.row for row in sorted_rows])
        if colored:
            # Current price is more than double the contract price.
            body_cell_slot(
                table,
                "quote",
                "red",
                "Number(props.value) < (Number(props.row['contract price']) * 2)",
            )
            body_cell_slot(
                table,
                profit_color_col,
                "red",
                "Number(props.value.split(' ')[0]) < 0",
                "green",
            )
        graphs = [row.graph for row in sorted_rows if row.graph]
        if graphs:
            with ui.grid(columns=StockOptionsPage.PL_GRID_COLUMNS):
                for graph in graphs:
                    ui.plotly(graph)

    def find_x_intercept(self, x1, y1, x2, y2, y):
        return x1 + (x2 - x1) * (y - y1) / (y2 - y1)

    def find_y_intercept(self, x1, y1, x2, y2, x):
        return y1 + (y2 - y1) * (x - x1) / (x2 - x1)

    def add_ticker_vline(self, fig: Figure, price: float, color: str):
        fig.add_vline(
            x=price,
            line_dash="dot",
            line_color=color,
            annotation_text=f"{price:.2f}",
        )

    def make_iron_condor_pl_graph(self, d: stock_options.IronCondorDetails) -> Figure:
        cd = d.details
        left_breakeven = self.find_x_intercept(
            d.low_put_strike, d.risk, d.high_put_strike, -cd.contract_price, 0
        )
        right_breakeven = self.find_x_intercept(
            d.low_call_strike, -cd.contract_price, d.high_call_strike, d.risk, 0
        )
        x = [
            d.low_put_strike,
            left_breakeven,
            d.high_put_strike,
            d.low_call_strike,
            right_breakeven,
            d.high_call_strike,
        ]
        y = [d.risk, 0, -cd.contract_price, -cd.contract_price, 0, d.risk]
        line_color = "red"
        if cd.ticker_price > left_breakeven and cd.ticker_price < right_breakeven:
            line_color = "green"
        if cd.ticker_price < d.low_put_strike:
            x.insert(0, cd.ticker_price)
            y.insert(0, d.risk)
        elif cd.ticker_price > d.high_call_strike:
            x.append(cd.ticker_price)
            y.append(d.risk)
        fig = px.line(x=x, y=y, markers=True)
        plot.centered_title(fig, f"{cd.account} {cd.ticker} {cd.expiration} IC")
        self.add_ticker_vline(fig, cd.ticker_price, line_color)
        fig.update_xaxes(title_text=f"{cd.ticker} Price")
        fig.update_yaxes(title_text="P/L")
        return fig

    def make_bull_put_pl_graph(self, d: stock_options.SpreadDetails) -> Figure:
        cd = d.details
        breakeven = self.find_x_intercept(
            d.low_strike, d.risk, d.high_strike, -cd.contract_price, 0
        )
        x = [d.low_strike, breakeven, d.high_strike]
        y = [d.risk, 0, -cd.contract_price]
        line_color = "red"
        if cd.ticker_price > breakeven:
            line_color = "green"
        if cd.ticker_price > d.high_strike:
            x.append(cd.ticker_price)
            y.append(-cd.contract_price)
        else:
            x.insert(0, cd.ticker_price)
            y.insert(0, d.risk)
        fig = px.line(x=x, y=y, markers=True)
        plot.centered_title(fig, f"{cd.account} {cd.ticker} {cd.expiration} Bull Put")
        self.add_ticker_vline(fig, cd.ticker_price, line_color)
        fig.update_xaxes(title_text=f"{cd.ticker} Price")
        fig.update_yaxes(title_text="P/L")
        return fig

    def make_bear_call_pl_graph(self, d: stock_options.SpreadDetails) -> Figure:
        cd = d.details
        breakeven = self.find_x_intercept(
            d.low_strike, -cd.contract_price, d.high_strike, d.risk, 0
        )
        x = [d.low_strike, breakeven, d.high_strike]
        y = [-cd.contract_price, 0, d.risk]
        line_color = "red"
        if cd.ticker_price < breakeven:
            line_color = "green"
        if cd.ticker_price > d.high_strike:
            x.append(cd.ticker_price)
            y.append(d.risk)
        else:
            x.insert(0, cd.ticker_price)
            y.insert(0, -cd.contract_price)
        fig = px.line(x=x, y=y, markers=True)
        plot.centered_title(fig, f"{cd.account} {cd.ticker} {cd.expiration} Bear Call")
        self.add_ticker_vline(fig, cd.ticker_price, line_color)
        fig.update_xaxes(title_text=f"{cd.ticker} Price")
        fig.update_yaxes(title_text="P/L")
        return fig

    @classmethod
    async def wait_for_data(cls) -> typing.Optional[OptionsData]:
        skel = None
        await ui.context.client.connected()
        while not cls.all_data_loaded():
            if not skel:
                skel = ui.skeleton("QToolbar").classes("w-full")
            await asyncio.sleep(1)
        if skel:
            skel.delete()
        return cls.options_data

    @classmethod
    def all_data_loaded(cls) -> bool:
        if generate_options_data.check_call_in_cache():
            cls.options_data = generate_options_data()
            return True
        elif cls.options_data:
            return True
        return False

    async def refresh_data(self):
        if self.refresh_button:
            self.refresh_button.disable()
        StockOptionsPage.options_data = None
        await run.io_bound(generate_options_data.clear)
        await run.io_bound(stock_options.options_df.clear)
        background_tasks.create(run.io_bound(generate_options_data))
        ui.navigate.reload()

    async def main_page(self):
        """Stock options."""
        if (data := await StockOptionsPage.wait_for_data()) is None:
            return
        with ui.row().classes("items-center"):
            ui.label(
                f"Staleness: {(datetime.now() - data.updated).total_seconds() / 60:.0f}m"
            )
            self.refresh_button = ui.button("Clear Cache", on_click=self.refresh_data)
        ui.html(f"<PRE>{data.main_output}</PRE>")
        opts = data.opts
        self.make_short_calls_table(data)
        self.make_complex_options_table(data)
        spread_df = pd.concat(
            [s.df for s in opts.bull_put_spreads + opts.bear_call_spreads]
        )
        tickers = spread_df["ticker"].unique()
        for ticker in sorted(tickers):
            ticker_df = spread_df.query("ticker == @ticker")
            price_df = (
                common.read_sql_table("schwab_etfs_prices")[[ticker]]
                .resample("D")
                .last()
                .dropna()
            )
            for broker in sorted(ticker_df.index.get_level_values("account").unique()):
                df = ticker_df.xs(broker, level="account")
                fig = (
                    plot.make_prices_section(price_df, f"{ticker} @ {broker}")
                    .update_layout(margin=common.SUBPLOT_MARGIN)
                    .update_traces(showlegend=False)
                )
                for index, row in df.iterrows():
                    name, expiration = typing.cast(tuple, index)
                    color = "green" if row["count"] > 0 else "red"
                    text = (
                        f"{row['count']} {name} ({(expiration - datetime.now()).days}d)"
                    )
                    fig.add_hline(
                        y=row["strike"],
                        annotation_text=text,
                        annotation_position="top left",
                        annotation_font_color=color,
                        line_dash="dot",
                        line_color=color,
                    )
                ui.plotly(fig).classes("w-full").style("height: 50vh")

        # Indexes
        options_df = stock_options.remove_spreads(
            opts.all_options, [s.df for s in opts.box_spreads]
        ).query('ticker == "SPX"')
        fig = plot.make_prices_section(
            common.read_sql_table("index_prices")[["^SPX"]]
            .resample("D")
            .last()
            .dropna(),
            "Index Prices",
        ).update_layout(margin=common.SUBPLOT_MARGIN)
        for index, row in options_df.iterrows():
            _, name, _ = typing.cast(tuple, index)
            fig.add_hline(
                y=row["strike"],
                annotation_text=f"{row['count']} {name}",
                annotation_position="top left",
                line_dash="dot",
                line_color="gray",
            )
        ui.plotly(fig).classes("w-full").style("height: 50vh")


@common.cache_forever_decorator
def generate_options_data() -> OptionsData:
    logger.info("Generating options data")
    opts = stock_options.get_options_and_spreads()
    itm_df = stock_options.get_itm_df(opts)
    expiration_values = stock_options.get_expiration_values(itm_df)
    with contextlib.redirect_stdout(io.StringIO()) as output:
        with common.pandas_options():
            stock_options.main(show_spreads=False, opts=opts)
            main_output = output.getvalue()
    data = OptionsData(
        opts=opts,
        bev=expiration_values,
        main_output=main_output,
        updated=datetime.now(),
    )
    logger.info("Finished generating options data")
    return data


def clear_and_generate():
    generate_options_data.clear()
    generate_options_data()
