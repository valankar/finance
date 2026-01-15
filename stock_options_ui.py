import io
import pickle
import typing
from datetime import date, datetime

import humanize
import pandas as pd
from loguru import logger
from matplotlib.figure import Figure
from nicegui import ui
from nicegui.elements.table import Table

import common
import stock_options
from main_graphs import GraphCommon

RowType = dict[str, typing.Any]


class UIData(typing.NamedTuple):
    short_calls: list[RowType]
    vertical_spreads: list[RowType]
    iron_condors: list[RowType]
    synthetics: list[RowType]
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
            index_images=self.make_index_images(),
        )

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
    ) -> list[RowType]:
        rows: list[RowType] = []
        d: stock_options.ShortCallDetails
        for d in options_data.opts.short_calls:
            cd: stock_options.CommonDetails = d.details
            expiration = f"{cd.expiration} ({(cd.expiration - date.today()).days}d)"
            rows.append(
                {
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
                }
            )
        return rows

    def make_short_calls_table(self, rows: list[RowType]):
        self.make_spread_section("Short Calls", rows, profit_color_col="profit option")

    def make_synthetics_data(
        self,
        options_data: stock_options.OptionsData,
    ) -> list[RowType]:
        rows: list[RowType] = []
        for d in options_data.opts.synthetics:
            sd: stock_options.SpreadDetails = d.details
            cd: stock_options.CommonDetails = sd.details
            name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
            risk = f"{sd.risk:.0f}"
            half_mark = f"{cd.half_mark:.2f}"
            double_mark = f"{cd.double_mark:.2f}"
            profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
            rows.append(
                RowType(
                    {
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
                    }
                )
            )
        return rows

    def make_vertical_spreads_data(
        self,
        options_data: stock_options.OptionsData,
    ) -> list[RowType]:
        rows: list[RowType] = []
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
                rows.append(
                    RowType(
                        {
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
                        }
                    )
                )
        return rows

    def make_iron_condors_data(
        self,
        options_data: stock_options.OptionsData,
    ) -> list[RowType]:
        rows: list[RowType] = []
        for d in options_data.opts.iron_condors:
            icd: stock_options.IronCondorDetails = d.details
            cd: stock_options.CommonDetails = icd.details
            name = f"{cd.ticker} {icd.low_put_strike:.0f}/{icd.high_put_strike:.0f}/{icd.low_call_strike:.0f}/{icd.high_call_strike:.0f}"
            risk = f"{icd.risk:.0f}"
            half_mark = f"{cd.half_mark:.2f}"
            double_mark = f"{cd.double_mark:.2f}"
            profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
            rows.append(
                {
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
                }
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
                RowType(
                    {
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
                    }
                )
            )
        if rows:
            self.make_spread_section(title, rows, colored=False)

    def make_spread_section(
        self,
        title: str,
        rows: list[RowType],
        colored: bool = True,
        profit_color_col: str = "profit",
    ):
        if not rows:
            return
        ui.label(title)
        sorted_rows = sorted(
            rows,
            key=lambda x: (
                x["account"],
                x["expiration"],
                x["name"],
                x["type"],
            ),
        )
        table = ui.table(rows=[row for row in sorted_rows])
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
        options_df = data.opts.all_options
        for broker in options_df.index.get_level_values("account").unique():
            ui.label(broker)
            df = (
                options_df.xs(broker, level="account")
                .reset_index()
                .sort_values(by="name")
            ).drop(
                columns=[
                    "exercise_value",
                    "min_contract_price",
                    "profit_stock_price",
                    "type",
                    "ticker",
                ]
            )
            i = int(df.columns.get_loc("expiration")) + 1  # type: ignore
            df.insert(i, "days", (df["expiration"] - pd.Timestamp.now()).dt.days)
            i = int(df.columns.get_loc("notional_value")) + 1  # type: ignore
            df.insert(i, "nv_per_contract", df["notional_value"] / abs(df["count"]))
            ui.table.from_pandas(df.round(2))
        self.make_spread_section(
            "Short Calls", self.ui_data.short_calls, profit_color_col="profit option"
        )
        self.make_spread_section("Synthetics", self.ui_data.synthetics)
        self.make_spread_section("Vertical Spreads", self.ui_data.vertical_spreads)
        self.make_spread_section("Iron Condors", self.ui_data.iron_condors)
        self.make_box_spread_sections("Box Spreads", data.opts.box_spreads)
        for image in self.ui_data.index_images:
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
