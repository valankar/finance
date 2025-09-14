import asyncio
import base64
import glob
import os
import pickle
import tempfile
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, ClassVar, Generator, Optional, cast

import humanize
import kaleido
import pandas as pd
import plotly.io as pio
from dateutil.relativedelta import relativedelta
from loguru import logger
from nicegui import run, ui
from nicegui.tailwind_types.text_color import TextColor
from plotly.graph_objects import Figure

import brokerages
import common
import etfs
import homes
import latest_values
import plot

RANGES = ["All", "3y", "2y", "1y", "YTD", "6m", "3m", "1m", "1d"]
DEFAULT_RANGE = "1y"

pio.templates.default = common.PLOTLY_THEME


@dataclass
class FigureData:
    name: str
    fig: Future[Figure]
    range: Optional[str] = None
    fig_json: Optional[Future[dict]] = None


class GraphCommon:
    REDIS_KEY: ClassVar[str] = "GraphData"
    REDIS_SUBKEY: ClassVar[str]

    def encode_png(self, graph: bytes) -> str:
        encoded = base64.b64encode(graph).decode("utf-8")
        return f"data:image/png;base64,{encoded}"

    def make_ui_key(self, redis_key: str) -> str:
        # Remove range from suffix
        return ":".join(redis_key.split(":")[:-1])

    def make_redis_key(
        self,
        title: str,
        subgroup: Optional[str] = None,
        subsubgroup: Optional[str] = None,
    ) -> str:
        joined = [x for x in [title, subgroup, subsubgroup] if x]
        redis_key = f"{self.REDIS_SUBKEY}:" + ":".join(joined)
        return redis_key

    def prices_df(self, selected_range: str) -> pd.DataFrame:
        df = etfs.get_prices_percent_diff_df()
        if (retval := self.get_xrange(df, selected_range)) is None:
            return df
        start, end = retval
        return etfs.get_prices_percent_diff_df((start, end))

    def limit_and_resample_df(
        self, df: pd.DataFrame, selected_range: str
    ) -> pd.DataFrame:
        """Limit df to selected range and resample."""
        if (retval := self.get_xrange(df, selected_range)) is None:
            return df
        start, end = retval
        try:
            df = df[start:end]
        except KeyError:
            return df
        match selected_range:
            case "1m" | "1d":
                window = None
            case "All" | "3y" | "2y":
                window = "W"
            case _:
                window = "D"
        if window:
            return df.resample(window).last().interpolate()
        return df

    def get_xrange(
        self, dataframe: pd.DataFrame, selected_range: str
    ) -> tuple[str | datetime, str | datetime] | None:
        """Determine time range for selected button."""
        latest_time = dataframe.index[-1]
        earliest_time = dataframe.index[0]
        today_time = datetime.now()
        xrange = None
        relative = None
        match selected_range:
            case "All":
                xrange = (earliest_time, latest_time)
            case "3y":
                relative = relativedelta(years=-3)
            case "2y":
                relative = relativedelta(years=-2)
            case "1y":
                relative = relativedelta(years=-1)
            case "YTD":
                xrange = (today_time.strftime("%Y-01-01"), latest_time)
            case "6m":
                relative = relativedelta(months=-6)
            case "3m":
                relative = relativedelta(months=-3)
            case "1m":
                relative = relativedelta(months=-1)
            case "1d":
                relative = relativedelta(days=-1)
        if relative:
            xrange = ((latest_time + relative), latest_time)
        return xrange

    def section_title(self, title: str, color: TextColor = "white"):
        with ui.column(align_items="center").classes("w-full"):
            ui.label(title).tailwind.text_color(color)

    def daily_change(self):
        diff = latest_values.difference_df(common.read_sql_table("history"))[1].iloc[-1]
        age = datetime.now() - cast(pd.Timestamp, diff.name)
        age_color = "red-500" if age > timedelta(hours=2) else "white"
        self.section_title(
            f"Daily Change (Staleness: {humanize.naturaldelta(age)})", color=age_color
        )
        total = diff["total"]
        total_color = "green-500" if total >= 0 else "red-500"
        total_no_homes = diff["total_no_homes"]
        total_no_homes_color = "green-500" if total_no_homes >= 0 else "red-500"
        with ui.grid(rows=2, columns=2).classes("w-full place-items-center"):
            ui.label("Total").tailwind.font_size("lg")
            ui.label("Total w/o Real Estate").tailwind.font_size("lg")
            ui.label(f"{total:+,.0f}").tailwind.font_size("lg").text_color(total_color)
            ui.label(f"{total_no_homes:+,.0f}").tailwind.font_size("lg").text_color(
                total_no_homes_color
            )

    def common_links(self):
        with ui.row().classes("flex justify-center w-full"):
            ui.link("Dynamic Graphs", "/")
            ui.link("Static Images", "/image_only")
            ui.link("Matplot", "/matplot")
            ui.link("Stock Options", "/stock_options")
            ui.link("Futures", "/futures")
            ui.link("Transactions", "/transactions")
            ui.link("Latest Values", "/latest_values")


class MainGraphs(GraphCommon):
    """Collection of all main graphs."""

    REDIS_SUBKEY: ClassVar[str] = "PlotlyGraphs"
    LAYOUT: ClassVar[tuple[tuple[str, str], ...]] = (
        ("assets_breakdown", "96vh"),
        ("investing_retirement", "45vh"),
        ("real_estate", "96vh"),
        ("allocation_profit", "75vh"),
        ("change", "50vh"),
        ("change_no_homes", "50vh"),
        ("investing_allocation", "50vh"),
        ("prices", "45vh"),
        ("forex", "45vh"),
        ("interest_rate", "45vh"),
        ("loan", "45vh"),
        ("brokerage_total", "45vh"),
    )

    def __init__(self):
        self.ui_plotly_ranged: dict[str, ui.plotly] = {}
        self.selected_range = DEFAULT_RANGE
        self.plotly_graphs = common.walrus_db.db.Hash(self.REDIS_KEY)

    def get_plotly_json(self, key: str) -> Optional[dict]:
        if graph := self.plotly_graphs.get(key):
            return pickle.loads(graph)
        return None

    def create(self) -> None:
        """Create all graphs."""
        # To avoid out of webgl context errors. See https://plotly.com/python/webgl-vs-svg/
        ui.add_body_html(
            '<script src="https://unpkg.com/virtual-webgl@1.0.6/src/virtual-webgl.js"></script>'
        )
        with ui.footer().classes("transparent q-py-none"):
            with ui.tabs().classes("w-full") as tabs:
                for timerange in RANGES:
                    ui.tab(timerange)
        tabs.bind_value(self, "selected_range")
        tabs.on_value_change(self.update)
        for layout_name, height in MainGraphs.LAYOUT:
            name = self.make_redis_key(layout_name, self.selected_range)
            if graph := self.get_plotly_json(name):
                self.ui_plotly_ranged[self.make_ui_key(name)] = (
                    ui.plotly(graph).classes("w-full").style(f"height: {height}")
                )
                continue
            # Non-ranged
            name = self.make_redis_key(layout_name)
            if graph := self.get_plotly_json(name):
                ui.plotly(graph).classes("w-full").style(f"height: {height}")
        self.daily_change()
        self.common_links()

    async def update(self) -> None:
        """Update all graphs."""
        for ui_key, ui_plotly in self.ui_plotly_ranged.items():
            name = f"{ui_key}:{self.selected_range}"
            if graph := self.get_plotly_json(name):
                await run.io_bound(ui_plotly.update_figure, graph)

    def get_plot_height_percent(self, name: str) -> float:
        for n, height in self.LAYOUT:
            if n == name:
                return float(int(height[:-2]) / 100)
        return 1.0

    async def generate(self, executor: ProcessPoolExecutor):
        """Generate and save all Plotly graphs."""
        logger.info("Generating Plotly graphs")
        ranges = RANGES
        subplot_margin = common.SUBPLOT_MARGIN
        start_time = datetime.now()
        dataframes = {
            "all": common.read_sql_table("history"),
            "brokerages": brokerages.load_df(),
            "real_estate": homes.get_real_estate_df(),
            "forex": common.read_sql_table("forex"),
            "interest_rate": plot.get_interest_rate_df(),
        }
        fs: list[FigureData] = []
        fs.append(
            FigureData(
                name="allocation_profit",
                fig=executor.submit(
                    plot.make_allocation_profit_section,
                    dataframes["all"],
                    dataframes["real_estate"],
                    subplot_margin,
                ),
            )
        )
        fs.append(
            FigureData(
                name="change",
                fig=executor.submit(
                    plot.make_change_section,
                    dataframes["all"],
                    "total",
                    "Total Net Worth Change",
                ),
            )
        )
        fs.append(
            FigureData(
                name="change_no_homes",
                fig=executor.submit(
                    plot.make_change_section,
                    dataframes["all"],
                    "total_no_homes",
                    "Total Net Worth Change w/o Real Estate",
                ),
            )
        )
        fs.append(
            FigureData(
                name="investing_allocation",
                fig=executor.submit(plot.make_investing_allocation_section),
            )
        )
        fs.append(
            FigureData(
                name="loan",
                fig=executor.submit(
                    plot.make_loan_section,
                    subplot_margin,
                ),
            )
        )
        for r in ranges:
            fs.append(
                FigureData(
                    name="assets_breakdown",
                    range=r,
                    fig=executor.submit(
                        plot.make_assets_breakdown_section,
                        self.limit_and_resample_df(dataframes["all"], r),
                        subplot_margin,
                    ),
                )
            )
            fs.append(
                FigureData(
                    name="investing_retirement",
                    range=r,
                    fig=executor.submit(
                        plot.make_investing_retirement_section,
                        self.limit_and_resample_df(
                            dataframes["all"][["pillar2", "ira"]],
                            r,
                        ),
                        subplot_margin,
                    ),
                )
            )
            fs.append(
                FigureData(
                    name="real_estate",
                    range=r,
                    fig=executor.submit(
                        plot.make_real_estate_section,
                        self.limit_and_resample_df(
                            dataframes["real_estate"],
                            r,
                        ),
                        subplot_margin,
                    ),
                )
            )
            fs.append(
                FigureData(
                    name="prices",
                    range=r,
                    fig=executor.submit(
                        plot.make_prices_section,
                        self.limit_and_resample_df(self.prices_df(r), r).sort_index(
                            axis=1
                        ),
                        "Prices",
                        subplot_margin,
                    ),
                )
            )
            fs.append(
                FigureData(
                    name="forex",
                    range=r,
                    fig=executor.submit(
                        plot.make_forex_section,
                        self.limit_and_resample_df(dataframes["forex"], r),
                        "Forex",
                        subplot_margin,
                    ),
                )
            )
            fs.append(
                FigureData(
                    name="interest_rate",
                    range=r,
                    fig=executor.submit(
                        plot.make_interest_rate_section,
                        self.limit_and_resample_df(dataframes["interest_rate"], r),
                        subplot_margin,
                    ),
                )
            )
            fs.append(
                FigureData(
                    name="brokerage_total",
                    range=r,
                    fig=executor.submit(
                        plot.make_brokerage_total_section,
                        self.limit_and_resample_df(dataframes["brokerages"], r),
                        subplot_margin,
                    ),
                )
            )
        for f in fs:
            f.fig_json = executor.submit(f.fig.result().to_plotly_json)
        # Generate images with kaleido
        with tempfile.TemporaryDirectory() as dir:
            mgio = MainGraphsImageOnly()
            async with kaleido.Kaleido(n=os.cpu_count() or 1) as k:
                await k.write_fig_from_object(
                    self.make_kaleido_generator(fs, dir, mgio.make_redis_key)
                )
            files = glob.glob(f"{dir}/*")
            for f in files:
                p = Path(f)
                # Filename stem is redis key
                mgio.image_graphs[p.stem] = p.read_bytes()
        # Store Plotly json
        for f in fs:
            if f.fig_json:
                self.plotly_graphs[self.make_redis_key(f.name, f.range)] = pickle.dumps(
                    f.fig_json.result()
                )
        logger.info(
            f"Graph generation time for Plotly: {humanize.precisedelta(datetime.now() - start_time)}"
        )

    def make_kaleido_generator(
        self,
        figs: list[FigureData],
        dest_dir: str,
        filename_maker: Callable[[str, Optional[str]], str],
    ) -> Generator[dict, None, None]:
        for f in figs:
            filename = filename_maker(f.name, f.range)
            yield dict(
                fig=f.fig.result(),
                path=f"{dest_dir}/{filename}.png",
                opts=dict(
                    width=1024,
                    height=int(768 * self.get_plot_height_percent(f.name)),
                ),
            )


class MainGraphsImageOnly(GraphCommon):
    REDIS_SUBKEY: ClassVar[str] = "PlotlyImageGraphs"

    def __init__(self):
        self.ui_image_ranged: dict[str, ui.image] = {}
        self.selected_range = DEFAULT_RANGE
        self.image_graphs = common.walrus_db.db.Hash(self.REDIS_KEY)

    def create(self):
        with ui.footer().classes("transparent q-py-none"):
            with ui.tabs().classes("w-full") as tabs:
                for timerange in RANGES:
                    ui.tab(timerange)
        tabs.bind_value(self, "selected_range")
        tabs.on_value_change(self.update)
        with ui.column().classes("w-full"):
            for layout_name, _ in MainGraphs.LAYOUT:
                name = self.make_redis_key(layout_name, self.selected_range)
                if graph := self.image_graphs.get(name):
                    self.ui_image_ranged[self.make_ui_key(name)] = ui.image(
                        self.encode_png(graph)
                    )
                    continue
                # Non-ranged
                name = self.make_redis_key(layout_name)
                if graph := self.image_graphs.get(name):
                    ui.image(self.encode_png(graph))
        self.daily_change()
        self.common_links()

    async def update(self) -> None:
        for ui_key, image in self.ui_image_ranged.items():
            name = f"{ui_key}:{self.selected_range}"
            if graph := self.image_graphs.get(name):
                await run.io_bound(
                    image.set_source,
                    self.encode_png(graph),
                )


def main():
    with ProcessPoolExecutor() as executor:
        asyncio.run(MainGraphs().generate(executor))


if __name__ == "__main__":
    main()
