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
        opts = stock_options.get_options_and_spreads()
        self.short_options = self.make_options_data(opts, "short")
        self.long_options = self.make_options_data(opts, "long")
        self.vertical_spreads = self.make_vertical_spreads_data(opts)
        self.iron_condors = self.make_iron_condors_data(opts)
        self.synthetics = self.make_synthetics_data(opts)
        self.strangles = self.make_strangles_data(opts)

    def make_options_data(
        self, opts: stock_options.OptionsAndSpreads, t: typing.Literal["long", "short"]
    ) -> list[RowType]:
        rows: list[RowType] = []
        match t:
            case "long":
                o = opts.long_options
            case "short":
                o = opts.short_options
        for d in sorted(o, key=lambda x: x.details.profit, reverse=True):
            cd: stock_options.CommonDetails = d.details
            expiration = f"{cd.expiration} ({(cd.expiration - date.today()).days}d)"
            rows.append(
                {
                    "account": cd.account,
                    "name": f"{cd.ticker} {d.strike:.0f}",
                    "expiration": expiration,
                    "type": d.option_type,
                    "count": cd.amount,
                    "intrinsic value": f"{cd.intrinsic_value:.0f}",
                    "notional value": f"{cd.notional_value:.0f}",
                    "delta": f"{d.df['delta'].sum():.2f}",
                    "contract price": f"{cd.contract_price:.0f}",
                    "quote": f"{(cd.quote / cd.amount / 100):.2f}",
                    "profit": f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})",
                    "ticker price": cd.ticker_price,
                }
            )
        return rows

    def make_strangles_data(
        self,
        opts: stock_options.OptionsAndSpreads,
    ) -> list[RowType]:
        rows: list[RowType] = []
        for d in sorted(
            opts.strangles, key=lambda x: x.details.details.profit, reverse=True
        ):
            sd: stock_options.SpreadDetails = d.details
            cd: stock_options.CommonDetails = sd.details
            t = "Short Strangle"
            name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
            delta = f"{sd.delta:.2f}"
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
                        "notional value": f"{cd.notional_value:.0f}",
                        "delta": delta,
                        "contract price": f"{cd.contract_price:.0f}",
                        "quote": f"{(cd.quote / cd.amount / 100):.2f}",
                        "profit": profit,
                        "ticker price": cd.ticker_price,
                    }
                )
            )
        return rows

    def make_synthetics_data(
        self,
        opts: stock_options.OptionsAndSpreads,
    ) -> list[RowType]:
        rows: list[RowType] = []
        for d in sorted(
            opts.synthetics, key=lambda x: x.details.details.profit, reverse=True
        ):
            sd: stock_options.SpreadDetails = d.details
            cd: stock_options.CommonDetails = sd.details
            if len(d.df.query("type == 'CALL' & count > 0")):
                t = "Long Synthetic"
            else:
                t = "Short Synthetic"
            name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
            delta = f"{sd.delta:.2f}"
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
                        "notional value": f"{cd.notional_value:.0f}",
                        "delta": delta,
                        "contract price": f"{cd.contract_price:.0f}",
                        "quote": f"{(cd.quote / cd.amount / 100):.2f}",
                        "profit": profit,
                        "ticker price": cd.ticker_price,
                    }
                )
            )
        return rows

    def make_vertical_spreads_data(
        self,
        opts: stock_options.OptionsAndSpreads,
    ) -> list[RowType]:
        rows: list[RowType] = []
        for spreads, spread_type in (
            (opts.bull_put_spreads, "Bull Put"),
            (opts.bear_call_spreads, "Bear Call"),
            (opts.bull_call_spreads, "Bull Call"),
        ):
            for d in sorted(
                spreads, key=lambda x: x.details.details.profit, reverse=True
            ):
                sd: stock_options.SpreadDetails = d.details
                cd: stock_options.CommonDetails = sd.details
                name = f"{cd.ticker} {sd.low_strike:.0f}/{sd.high_strike:.0f}"
                delta = f"{sd.delta:.2f}"
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
                            "notional value": f"{cd.notional_value:.0f}",
                            "delta": delta,
                            "contract price": f"{cd.contract_price:.0f}",
                            "quote": f"{(cd.quote / cd.amount / 100):.2f}",
                            "profit": profit,
                            "ticker price": cd.ticker_price,
                        }
                    )
                )
        return rows

    def make_iron_condors_data(
        self,
        opts: stock_options.OptionsAndSpreads,
    ) -> list[RowType]:
        rows: list[RowType] = []
        for d in opts.iron_condors:
            icd: stock_options.IronCondorDetails = d.details
            cd: stock_options.CommonDetails = icd.details
            name = f"{cd.ticker} {icd.low_put_strike:.0f}/{icd.high_put_strike:.0f}/{icd.low_call_strike:.0f}/{icd.high_call_strike:.0f}"
            risk = f"{icd.risk:.0f}"
            profit = f"{cd.profit:.0f} ({abs(cd.profit / cd.contract_price):.0%})"
            rows.append(
                {
                    "account": cd.account,
                    "name": name,
                    "expiration": f"{cd.expiration} ({(cd.expiration - date.today()).days}d)",
                    "type": "Iron Condor",
                    "count": cd.amount,
                    "intrinsic value": f"{cd.intrinsic_value:.0f}",
                    "notional value": f"{cd.notional_value:.0f}",
                    "maximum loss": risk,
                    "contract price": f"{cd.contract_price:.0f}",
                    "quote": f"{(cd.quote / cd.amount / 100):.2f}",
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
        table = ui.table(rows=rows)
        if colored:
            body_cell_slot(
                table,
                profit_color_col,
                "red",
                "Number(props.value.split(' ')[0]) < 0",
                "green",
            )

    def make_all_options_section(self, options_df: pd.DataFrame):
        if not len(options_df):
            return
        ui.label("Ungrouped Options")
        df = (options_df.reset_index().sort_values(by="name")).drop(
            columns=[
                "exercise_value",
                "profit_stock_price",
                "type",
                "ticker",
            ]
        )
        i = int(df.columns.get_loc("expiration")) + 1  # type: ignore
        df.insert(i, "days", (df["expiration"] - pd.Timestamp.now()).dt.days)
        i = int(df.columns.get_loc("notional_value")) + 1  # type: ignore
        df.insert(i, "nv_per_contract", df["notional_value"] / abs(df["count"]))
        df["contract_price"] = df["contract_price"] * df["multiplier"] * df["count"]
        df["profit_percent"] = round(
            df["profit_option_value"] / abs(df["contract_price"]) * 100
        )
        df["profit_display"] = df.apply(
            lambda row: (
                f"{row['profit_option_value']:.0f} ({row['profit_percent']:.0f}%)"
            ),
            axis=1,
        )
        df = (
            df.drop(columns=["multiplier", "intrinsic_value"])
            .sort_values("profit_option_value", ascending=False)
            .drop(columns=["profit_option_value", "profit_percent"])
        )
        table = ui.table.from_pandas(df.assign(**df.select_dtypes("number").round(2)))
        body_cell_slot(
            table,
            "profit_display",
            "red",
            "Number(props.value.split(' ')[0]) <= 0",
            "green",
        )

    def main_page(self):
        """Stock options."""
        opts = stock_options.get_options_and_spreads()
        self.make_spread_section("Synthetics", self.synthetics)
        self.make_spread_section("Vertical Spreads", self.vertical_spreads)
        self.make_spread_section("Strangles", self.strangles)
        self.make_spread_section("Iron Condors", self.iron_condors)
        self.make_spread_section("Short Options", self.short_options)
        self.make_spread_section("Long Options", self.long_options)
        self.make_box_spread_sections("Box Spreads", opts.box_spreads)
        self.make_all_options_section(opts.pruned_options)


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
