import typing
from datetime import date

import pandas as pd
from nicegui import ui
from nicegui.elements.table import Table

import stock_options
from main_graphs import GraphCommon

RowType = dict[str, typing.Any]


class StockOptionsPage(GraphCommon):
    PL_GRID_COLUMNS: typing.ClassVar[int] = 3

    def __init__(self):
        options_data = stock_options.get_options_data()
        self.short_calls = self.make_short_calls_data(options_data)
        self.vertical_spreads = self.make_vertical_spreads_data(options_data)
        self.iron_condors = self.make_iron_condors_data(options_data)
        self.synthetics = self.make_synthetics_data(options_data)

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
            if len(d.df.query("type == 'CALL' & count > 0")):
                t = "Long Synthetic"
            else:
                t = "Short Synthetic"
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
                        "type": t,
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

    def make_all_options_section(self, broker: str, options_df: pd.DataFrame):
        ui.label(broker)
        df = (
            options_df.xs(broker, level="account").reset_index().sort_values(by="name")
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
        table = ui.table.from_pandas(df.assign(**df.select_dtypes("number").round(2)))
        body_cell_slot(
            table,
            "profit_option_value",
            "red",
            "Number(props.value) < 0",
            "green",
        )

    def main_page(self):
        """Stock options."""
        opts = stock_options.get_options_data().opts
        for broker in opts.all_options.index.get_level_values("account").unique():
            df = stock_options.remove_spreads(
                opts.all_options, [s.df for s in opts.box_spreads]
            )
            self.make_all_options_section(broker, df)
        self.make_spread_section(
            "Short Calls", self.short_calls, profit_color_col="profit option"
        )
        self.make_spread_section("Synthetics", self.synthetics)
        self.make_spread_section("Vertical Spreads", self.vertical_spreads)
        self.make_spread_section("Iron Condors", self.iron_condors)
        self.make_box_spread_sections("Box Spreads", opts.box_spreads)


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
