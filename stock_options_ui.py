import io
import pickle
import typing
from datetime import date, datetime

import humanize
import pandas as pd
from loguru import logger
from matplotlib.figure import Figure
from nicegui import html, ui
from nicegui.elements.table import Table

import common
import etfs
import stock_options
from main_graphs import GraphCommon


class RowGraphPair(typing.NamedTuple):
    row: dict[str, typing.Any]
    graph: typing.Optional[bytes]


class UIData(typing.NamedTuple):
    short_calls: list[RowGraphPair]
    vertical_spreads: list[RowGraphPair]
    iron_condors: list[RowGraphPair]
    synthetics: list[RowGraphPair]
    ticker_spread_images: list[bytes]
    index_images: list[bytes]


class StockOptionsPage(GraphCommon):
    PL_GRID_COLUMNS: typing.ClassVar[int] = 3
    REDIS_SUBKEY: typing.ClassVar[str] = "StockOptionsPage UIData"

    def __init__(self):
        self.image_graphs = common.walrus_db.db.Hash(self.REDIS_KEY)

    @property
    def ui_data(self) -> UIData:
        return pickle.loads(self.image_graphs[self.REDIS_SUBKEY])

    @ui_data.setter
    def ui_data(self, value: UIData):
        self.image_graphs[self.REDIS_SUBKEY] = pickle.dumps(value)

    @property
    def options_data(self) -> stock_options.OptionsData:
        if (data := stock_options.get_options_data()) is None:
            raise ValueError("Options data is not available.")
        return data

    def make_image_graph(self, fig: Figure) -> bytes:
        data = io.BytesIO()
        fig.savefig(data, format="png")
        return data.getvalue()

    def make_ui_data(self, options_data: stock_options.OptionsData) -> UIData:
        return UIData(
            short_calls=self.make_short_calls_data(options_data),
            vertical_spreads=self.make_vertical_spreads_data(options_data),
            iron_condors=self.make_iron_condors_data(options_data),
            synthetics=self.make_synthetics_data(options_data),
            ticker_spread_images=self.make_ticker_spread_images(
                options_data.opts.bull_put_spreads, options_data.opts.bear_call_spreads
            ),
            index_images=self.make_index_images(),
        )

    def make_ticker_spread_images(
        self,
        bull_put_spreads: list[stock_options.Spread],
        bear_call_spreads: list[stock_options.Spread],
    ) -> list[bytes]:
        images: list[bytes] = []
        dfs = [s.df for s in bull_put_spreads + bear_call_spreads]
        if not dfs:
            return images
        spread_df = pd.concat(dfs)
        tickers = spread_df["ticker"].unique()
        for ticker in sorted(tickers):
            ticker_df = spread_df.query("ticker == @ticker")
            price_df = etfs.get_prices_wide_df()[[ticker]].resample("D").last().dropna()
            for broker in sorted(ticker_df.index.get_level_values("account").unique()):
                df = ticker_df.xs(broker, level="account")
                fig = Figure(figsize=(15, 5), layout="tight")
                ax = fig.subplots()
                ax.plot(price_df.index, price_df, color="tab:blue")
                ax.set_ylabel("Price")
                ax.set_title(f"{ticker} @ {broker}")
                for _, row in df.iterrows():
                    color = "green" if row["count"] > 0 else "red"
                    ax.axhline(row["strike"], color=color, linestyle="--")
                images.append(self.make_image_graph(fig))
        return images

    def make_index_images(self) -> list[bytes]:
        images: list[bytes] = []
        spx_df = (
            common.read_sql_table("index_prices")[["^SPX"]]
            .resample("D")
            .last()
            .dropna()
        )
        fig = Figure(figsize=(15, 5), layout="tight")
        ax = fig.subplots()
        ax.plot(spx_df.index, spx_df, color="tab:blue")
        ax.set_ylabel("Price")
        ax.set_title("SPX")
        images.append(self.make_image_graph(fig))
        return images

    def make_short_calls_data(
        self, options_data: stock_options.OptionsData
    ) -> list[RowGraphPair]:
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
                        "count": cd.amount,
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
        return rows

    def make_short_calls_table(self, rows: list[RowGraphPair]):
        self.make_spread_section("Short Calls", rows, profit_color_col="profit option")

    def make_synthetics_data(
        self,
        options_data: stock_options.OptionsData,
    ) -> list[RowGraphPair]:
        rows: list[RowGraphPair] = []
        for d in options_data.opts.synthetics:
            sd: stock_options.SpreadDetails = d.details
            cd: stock_options.CommonDetails = sd.details
            name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
            risk = f"{sd.risk:.0f}"
            half_mark = f"{cd.half_mark:.2f}"
            double_mark = f"{cd.double_mark:.2f}"
            profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
            rows.append(
                RowGraphPair(
                    row={
                        "account": cd.account,
                        "name": name,
                        "expiration": f"{cd.expiration} ({(cd.expiration - date.today()).days}d)",
                        "type": "Synthetic",
                        "count": cd.amount,
                        "intrinsic value": f"{cd.intrinsic_value:.0f}",
                        "maximum loss": risk,
                        "contract price": f"{cd.contract_price:.0f}",
                        "half mark": half_mark,
                        "double mark": double_mark,
                        "quote": f"{cd.quote:.0f}",
                        "profit": profit,
                        "ticker price": cd.ticker_price,
                    },
                    graph=None,
                )
            )
        return rows

    def make_vertical_spreads_data(
        self,
        options_data: stock_options.OptionsData,
    ) -> list[RowGraphPair]:
        rows: list[RowGraphPair] = []
        for spreads, spread_type in (
            (options_data.opts.bull_put_spreads_no_ic, "Bull Put"),
            (options_data.opts.bear_call_spreads_no_ic, "Bear Call"),
            (options_data.opts.bull_call_spreads, "Bull Call"),
        ):
            for d in spreads:
                sd: stock_options.SpreadDetails = d.details
                cd: stock_options.CommonDetails = sd.details
                name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
                risk = f"{sd.risk:.0f}"
                half_mark = f"{cd.half_mark:.2f}"
                double_mark = f"{cd.double_mark:.2f}"
                profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
                match spread_type:
                    case "Bull Put":
                        graph = self.make_bull_put_pl_graph(sd)
                    case "Bear Call":
                        graph = self.make_bear_call_pl_graph(sd)
                    case "Bull Call":
                        graph = self.make_bull_call_pl_graph(sd)
                    case _:
                        continue
                rows.append(
                    RowGraphPair(
                        row={
                            "account": cd.account,
                            "name": name,
                            "expiration": f"{cd.expiration} ({(cd.expiration - date.today()).days}d)",
                            "type": spread_type,
                            "count": cd.amount,
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
        return rows

    def make_iron_condors_data(
        self,
        options_data: stock_options.OptionsData,
    ) -> list[RowGraphPair]:
        rows: list[RowGraphPair] = []
        for d in options_data.opts.iron_condors:
            icd: stock_options.IronCondorDetails = d.details
            cd: stock_options.CommonDetails = icd.details
            name = f"{cd.ticker} {icd.low_put_strike:.0f}/{icd.high_put_strike:.0f}/{icd.low_call_strike:.0f}/{icd.high_call_strike:.0f}"
            risk = f"{icd.risk:.0f}"
            half_mark = f"{cd.half_mark:.2f}"
            double_mark = f"{cd.double_mark:.2f}"
            profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
            graph = self.make_iron_condor_pl_graph(icd)
            rows.append(
                RowGraphPair(
                    row={
                        "account": cd.account,
                        "name": name,
                        "expiration": f"{cd.expiration} ({(cd.expiration - date.today()).days}d)",
                        "type": "Iron Condor",
                        "count": cd.amount,
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
        return rows

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
                        "count": cd.amount,
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
        if not rows:
            return
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
            with ui.grid().classes("w-full gap-0 md:grid-cols-3"):
                for graph in graphs:
                    ui.image(self.encode_png(graph))

    def find_x_intercept(self, x1, y1, x2, y2, y):
        return x1 + (x2 - x1) * (y - y1) / (y2 - y1)

    def find_y_intercept(self, x1, y1, x2, y2, x):
        return y1 + (y2 - y1) * (x - x1) / (x2 - x1)

    def make_short_call_pl_graph(
        self,
        d: stock_options.ShortCallDetails,
    ) -> bytes:
        cd = d.details
        x = [d.strike, d.strike + cd.contract_price_per_share]
        y = [-cd.contract_price, 0]
        line_color = "tab:green"
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
            line_color = "tab:red"
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
        fig = Figure()
        ax = fig.subplots()
        ax.plot(x, y, marker=".", color="tab:blue")
        ax.set_xlabel("Price")
        ax.set_ylabel("P/L")
        ax.set_title(f"{cd.account} {cd.ticker} {cd.expiration} Short Call")
        ax.axvline(cd.ticker_price, color=line_color, linestyle="--")
        return self.make_image_graph(fig)

    def make_iron_condor_pl_graph(self, d: stock_options.IronCondorDetails) -> bytes:
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
        fig = Figure()
        ax = fig.subplots()
        ax.plot(x, y, marker=".", color="tab:blue")
        ax.set_xlabel("Price")
        ax.set_ylabel("P/L")
        ax.set_title(f"{cd.account} {cd.ticker} {cd.expiration} IC")
        ax.axvline(cd.ticker_price, color=line_color, linestyle="--")
        return self.make_image_graph(fig)

    def make_bull_put_pl_graph(self, d: stock_options.SpreadDetails) -> bytes:
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
        elif cd.ticker_price < d.low_strike:
            x.insert(0, cd.ticker_price)
            y.insert(0, d.risk)
        else:
            x.insert(1, cd.ticker_price)
            y.insert(
                1,
                self.find_y_intercept(
                    d.low_strike,
                    d.risk,
                    d.high_strike,
                    -cd.contract_price,
                    cd.ticker_price,
                ),
            )

        fig = Figure()
        ax = fig.subplots()
        ax.plot(x, y, marker=".", color="tab:blue")
        ax.set_xlabel("Price")
        ax.set_ylabel("P/L")
        ax.set_title(
            f"{cd.account} {cd.ticker} {d.low_strike}/{d.high_strike} {cd.expiration} Bull Put"
        )
        ax.axvline(cd.ticker_price, color=line_color, linestyle="--")
        return self.make_image_graph(fig)

    def make_bear_call_pl_graph(self, d: stock_options.SpreadDetails) -> bytes:
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
        fig = Figure()
        ax = fig.subplots()
        ax.plot(x, y, marker=".", color="tab:blue")
        ax.set_xlabel("Price")
        ax.set_ylabel("P/L")
        ax.set_title(
            f"{cd.account} {cd.ticker} {d.low_strike}/{d.high_strike} {cd.expiration} Bear Call"
        )
        ax.axvline(cd.ticker_price, color=line_color, linestyle="--")
        return self.make_image_graph(fig)

    def make_bull_call_pl_graph(self, d: stock_options.SpreadDetails) -> bytes:
        cd = d.details
        breakeven = self.find_x_intercept(
            d.low_strike, -cd.contract_price, d.high_strike, d.risk, 0
        )
        x = [d.low_strike, breakeven, d.high_strike]
        y = [-cd.contract_price, 0, d.risk]
        line_color = "red"
        if cd.ticker_price > breakeven or cd.contract_price < 0:
            line_color = "green"
        if cd.ticker_price > d.high_strike:
            x.append(cd.ticker_price)
            y.append(d.risk)
        else:
            x.insert(0, cd.ticker_price)
            y.insert(0, -cd.contract_price)
        fig = Figure()
        ax = fig.subplots()
        ax.plot(x, y, marker=".", color="tab:blue")
        ax.set_xlabel("Price")
        ax.set_ylabel("P/L")
        ax.set_title(
            f"{cd.account} {cd.ticker} {d.low_strike}/{d.high_strike} {cd.expiration} Bull Call"
        )
        ax.axvline(cd.ticker_price, color=line_color, linestyle="--")
        return self.make_image_graph(fig)

    def generate(self):
        logger.info(f"Generating {self.REDIS_SUBKEY}")
        self.ui_data = self.make_ui_data(self.options_data)
        logger.info(f"Finished generating {self.REDIS_SUBKEY}")

    def main_page(self):
        """Stock options."""
        data = self.options_data
        with ui.row().classes("items-center"):
            ui.label(
                f"Staleness: {humanize.naturaldelta(datetime.now() - data.updated)}"
            )
        html.pre(data.main_output)
        self.make_spread_section(
            "Short Calls", self.ui_data.short_calls, profit_color_col="profit option"
        )
        self.make_spread_section("Synthetics", self.ui_data.synthetics)
        self.make_spread_section("Vertical Spreads", self.ui_data.vertical_spreads)
        self.make_spread_section("Iron Condors", self.ui_data.iron_condors)
        self.make_box_spread_sections("Box Spreads", data.opts.box_spreads)
        for image in self.ui_data.ticker_spread_images + self.ui_data.index_images:
            ui.image(self.encode_png(image)).classes("w-full")


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


def main():
    StockOptionsPage().generate()


if __name__ == "__main__":
    main()
