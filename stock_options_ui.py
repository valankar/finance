import asyncio
import contextlib
import io
import typing
from datetime import date, datetime

import pandas as pd
from loguru import logger
from nicegui import ui

import common
import plot
import stock_options


class OptionsData(typing.NamedTuple):
    opts: stock_options.OptionsAndSpreads
    bev: list[stock_options.BrokerExpirationValues]
    main_output: str
    updated: datetime


class StockOptionsPage:
    options_data: typing.ClassVar[typing.Optional[OptionsData]] = None

    def make_complex_options_table(
        self,
        bull_put_spreads: list[pd.DataFrame],
        bear_call_spreads: list[pd.DataFrame],
        box_spreads: list[pd.DataFrame],
    ):
        rows = []
        for spreads, spread_type in (
            (bull_put_spreads, "Bull Put"),
            (bear_call_spreads, "Bear Call"),
            (box_spreads, "Box"),
        ):
            for spread_df in spreads:
                d = stock_options.get_spread_details(spread_df)
                name = f"{d.ticker} {d.low_strike:.0f}/{d.high_strike:.0f}"
                risk = half_mark = double_mark = ""
                if spread_type != "Box":
                    risk = f"{d.risk:.0f}"
                    half_mark = f"{d.half_mark:.2f}"
                    double_mark = f"{d.double_mark:.2f}"
                rows.append(
                    {
                        "account": d.account,
                        "name": name,
                        "expiration": f"{d.expiration} ({(d.expiration - date.today()).days}d)",
                        "type": spread_type,
                        "count": d.count,
                        "intrinsic value": f"{d.intrinsic_value:.0f}",
                        "maximum loss": risk,
                        "contract price": f"{d.contract_price:.0f}",
                        "half mark": half_mark,
                        "double mark": double_mark,
                        "ticker price": d.ticker_price,
                    }
                )
        if rows:
            ui.label("Spreads")
            ui.table(rows=sorted(rows, key=lambda x: x["expiration"]))

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

    async def main_page(self):
        """Stock options."""
        if (data := await StockOptionsPage.wait_for_data()) is None:
            return
        ui.label(
            f"Staleness: {(datetime.now() - data.updated).total_seconds() / 60:.0f}m"
        )
        ui.html(f"<PRE>{data.main_output}</PRE>")
        opts = data.opts
        self.make_complex_options_table(
            opts.bull_put_spreads, opts.bear_call_spreads, opts.box_spreads
        )
        spread_df = pd.concat(opts.bull_put_spreads + opts.bear_call_spreads)
        tickers = spread_df["ticker"].unique()
        for ticker in sorted(tickers):
            ticker_df = spread_df.query("ticker == @ticker")
            price_df = common.read_sql_table("schwab_etfs_prices")[[ticker]].dropna()
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
            opts.all_options, opts.box_spreads
        ).query('ticker == "SPX"')
        fig = plot.make_prices_section(
            common.read_sql_table("index_prices")[["^SPX"]], "Index Prices"
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
    return OptionsData(
        opts=opts,
        bev=expiration_values,
        main_output=main_output,
        updated=datetime.now(),
    )


def clear_and_generate():
    generate_options_data.clear()
    generate_options_data()
