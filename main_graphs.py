import asyncio
import base64
import io
import pickle
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Callable, ClassVar, NamedTuple, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.io as pio
import walrus
from dateutil.relativedelta import relativedelta
from loguru import logger
from nicegui import run, ui
from plotly.graph_objects import Figure

import brokerages
import common
import homes
import plot


class PlotlyGraph(NamedTuple):
    plotly_json: dict


class ImageGraph(NamedTuple):
    png_data: bytes


class PlotlyGraphs(NamedTuple):
    # Key is graph name.
    nonranged: dict[str, PlotlyGraph]
    # First key is range, next is graph name.
    ranged: dict[str, dict[str, PlotlyGraph]]
    last_updated_time: datetime
    last_generation_duration: timedelta
    latest_datapoint_time: pd.Timestamp


class ImageGraphs(NamedTuple):
    nonranged: dict[str, ImageGraph]
    ranged: dict[str, dict[str, ImageGraph]]


RANGES = ["All", "3y", "2y", "1y", "YTD", "6m", "3m", "1m", "1d"]
DEFAULT_RANGE = "1y"


class MainGraphs:
    """Collection of all main graphs."""

    REDIS_KEY: ClassVar[str] = "GraphData"
    REDIS_SUBKEY: ClassVar[str] = "PlotlyGraphs"
    LAYOUT: ClassVar[tuple[tuple[str, str], ...]] = (
        ("assets_breakdown", "96vh"),
        ("investing_retirement", "75vh"),
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
        ("daily_indicator", "45vh"),
    )

    def __init__(self, db: walrus.Database):
        self.ui_plotly = {}
        self.ui_stats_labels = {}
        self.selected_range = DEFAULT_RANGE
        self.db = db

    def graph_data_available(self) -> bool:
        return (
            MainGraphs.REDIS_KEY in self.db
            and MainGraphs.REDIS_SUBKEY in self.db.Hash(MainGraphs.REDIS_KEY)
        )

    async def wait_for_graphs(self):
        skel = None
        await ui.context.client.connected()
        while not self.graph_data_available():
            if not skel:
                skel = ui.skeleton("QToolbar").classes("w-full")
            await asyncio.sleep(1)
        if skel:
            skel.delete()

    @property
    def plotly_graphs(self) -> PlotlyGraphs:
        if not self.graph_data_available():
            raise ValueError("Data is not initialized.")
        return pickle.loads(self.db.Hash(MainGraphs.REDIS_KEY)[MainGraphs.REDIS_SUBKEY])

    @plotly_graphs.setter
    def plotly_graphs(self, graphs: PlotlyGraphs):
        self.db.Hash(MainGraphs.REDIS_KEY)[MainGraphs.REDIS_SUBKEY] = pickle.dumps(
            graphs
        )

    async def update_stats_labels(self) -> None:
        try:
            timezone = ZoneInfo(
                await ui.run_javascript(
                    "Intl.DateTimeFormat().resolvedOptions().timeZone", timeout=10
                )
            )
        except TimeoutError:
            timezone = ZoneInfo("UTC")
        self.ui_stats_labels["last_datapoint_time"].set_text(
            f"Latest datapoint: {self.plotly_graphs.latest_datapoint_time.tz_localize('UTC').astimezone(timezone).strftime('%c')}"
        )
        self.ui_stats_labels["last_updated_time"].set_text(
            f"Graphs last updated: {self.plotly_graphs.last_updated_time.astimezone(timezone).strftime('%c')}"
        )
        self.ui_stats_labels["last_generation_duration"].set_text(
            f"Graph generation duration: {self.plotly_graphs.last_generation_duration.total_seconds():.2f}s"
        )

    async def create(self) -> None:
        """Create all graphs."""
        for name, height in MainGraphs.LAYOUT:
            if name in self.plotly_graphs.ranged:
                graph = self.plotly_graphs.ranged[name][self.selected_range]
            else:
                try:
                    graph = self.plotly_graphs.nonranged[name]
                except KeyError:
                    continue
            self.ui_plotly[name] = (
                ui.plotly(graph.plotly_json)
                .classes("w-full")
                .style(f"height: {height}")
            )
        with ui.row().classes("flex justify-center w-full"):
            for label in [
                "last_datapoint_time",
                "last_updated_time",
                "last_generation_duration",
                "next_generation_time",
            ]:
                self.ui_stats_labels[label] = ui.label()
        with ui.row().classes("flex justify-center w-full"):
            ui.link("Static Images", "/image_only")
            common_links()
        await self.update_stats_labels()

    async def update(self) -> None:
        """Update all graphs."""
        for name in self.plotly_graphs.ranged:
            await run.io_bound(
                self.ui_plotly[name].update_figure,
                self.plotly_graphs.ranged[name][self.selected_range].plotly_json,
            )
        await self.update_stats_labels()


class MainGraphsImageOnly:
    REDIS_KEY: ClassVar[str] = "GraphData"
    REDIS_SUBKEY: ClassVar[str] = "ImageGraphs"

    def __init__(self, db: walrus.Database):
        self.ui_image: dict[str, ui.image] = {}
        self.selected_range = DEFAULT_RANGE
        self.db = db

    def graph_data_available(self) -> bool:
        return (
            MainGraphsImageOnly.REDIS_KEY in self.db
            and MainGraphsImageOnly.REDIS_SUBKEY
            in self.db.Hash(MainGraphsImageOnly.REDIS_KEY)
        )

    async def wait_for_graphs(self):
        skel = None
        await ui.context.client.connected()
        while not self.graph_data_available():
            if not skel:
                skel = ui.skeleton("QToolbar").classes("w-full")
            await asyncio.sleep(1)
        if skel:
            skel.delete()

    @property
    def image_graphs(self) -> ImageGraphs:
        if not self.graph_data_available():
            raise ValueError("Data is not initialized.")
        return pickle.loads(
            self.db.Hash(MainGraphsImageOnly.REDIS_KEY)[
                MainGraphsImageOnly.REDIS_SUBKEY
            ]
        )

    @image_graphs.setter
    def image_graphs(self, graphs: ImageGraphs):
        self.db.Hash(MainGraphsImageOnly.REDIS_KEY)[
            MainGraphsImageOnly.REDIS_SUBKEY
        ] = pickle.dumps(graphs)

    @staticmethod
    def encode_png(graph: ImageGraph) -> str:
        encoded = base64.b64encode(graph.png_data).decode("utf-8")
        return f"data:image/png;base64,{encoded}"

    async def create(self) -> None:
        with ui.column().classes("w-full"):
            for name, _ in MainGraphs.LAYOUT:
                if name in self.image_graphs.ranged:
                    graph = self.image_graphs.ranged[name][self.selected_range]
                else:
                    try:
                        graph = self.image_graphs.nonranged[name]
                    except KeyError:
                        continue
                self.ui_image[name] = ui.image(self.encode_png(graph))
            with ui.row().classes("flex justify-center w-full"):
                ui.link("Dynamic graphs", "/")
                common_links()

    async def update(self) -> None:
        for name in self.image_graphs.ranged:
            graph = self.image_graphs.ranged[name][self.selected_range]
            await run.io_bound(
                self.ui_image[name].set_source,
                self.encode_png(graph),
            )


def common_links():
    ui.link("Stock Options", "/stock_options")
    ui.link("Transactions", "/transactions")


def get_xrange(
    dataframe: pd.DataFrame, selected_range: str
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


def limit_and_resample_df(df: pd.DataFrame, selected_range: str) -> pd.DataFrame:
    """Limit df to selected range and resample."""
    if (retval := get_xrange(df, selected_range)) is None:
        return df
    start, end = retval
    df = df[start:end]
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


def get_plot_height_percent(name: str, layout: tuple[tuple[str, str], ...]) -> float:
    for n, height in layout:
        if n == name:
            return float(int(height[:-2]) / 100)
    return 1.0


def plot_generate(
    name: str,
    plot_func: Callable[..., Figure],
    layout: tuple[tuple[str, str], ...],
    r: Optional[str] = None,
) -> tuple[PlotlyGraph, ImageGraph]:
    pio.templates.default = common.PLOTLY_THEME
    if r:
        fig = plot_func(r)
    else:
        fig = plot_func()
    data = io.BytesIO()
    fig.write_image(
        data,
        format="png",
        width=1024,
        height=768 * get_plot_height_percent(name, layout),
    )
    return (
        PlotlyGraph(plotly_json=fig.to_plotly_json()),
        ImageGraph(png_data=data.getvalue()),
    )


def generate_all_graphs() -> tuple[PlotlyGraphs, ImageGraphs]:
    """Generate and save all Plotly graphs."""
    logger.info("Generating graphs")
    layout = MainGraphs.LAYOUT
    ranges = RANGES
    subplot_margin = common.SUBPLOT_MARGIN
    start_time = datetime.now()
    dataframes = {
        "all": common.read_sql_table("history"),
        "brokerages": brokerages.load_df(),
        "real_estate": homes.get_real_estate_df(),
        "prices": common.read_sql_table("schwab_etfs_prices"),
        "forex": common.read_sql_table("forex"),
        "interest_rate": plot.get_interest_rate_df(),
    }
    nonranged_graphs_generate = [
        (
            "allocation_profit",
            lambda: plot.make_allocation_profit_section(
                dataframes["all"],
                dataframes["real_estate"],
            ).update_layout(margin=subplot_margin),
        ),
        (
            "change",
            lambda: plot.make_change_section(
                dataframes["all"],
                "total",
                "Total Net Worth Change",
            ),
        ),
        (
            "change_no_homes",
            lambda: plot.make_change_section(
                dataframes["all"],
                "total_no_homes",
                "Total Net Worth Change w/o Real Estate",
            ),
        ),
        (
            "investing_allocation",
            lambda: plot.make_investing_allocation_section(),
        ),
        (
            "loan",
            lambda: plot.make_loan_section().update_layout(margin=subplot_margin),
        ),
        (
            "daily_indicator",
            lambda: plot.make_daily_indicator(dataframes["all"]),
        ),
    ]
    ranged_graphs_generate = [
        (
            "assets_breakdown",
            lambda range: plot.make_assets_breakdown_section(
                limit_and_resample_df(dataframes["all"], range)
            ).update_layout(margin=subplot_margin),
        ),
        (
            "investing_retirement",
            lambda range: plot.make_investing_retirement_section(
                limit_and_resample_df(
                    dataframes["all"][["pillar2", "ira", "commodities", "etfs"]],
                    range,
                )
            ).update_layout(margin=subplot_margin),
        ),
        (
            "real_estate",
            lambda range: plot.make_real_estate_section(
                limit_and_resample_df(
                    dataframes["real_estate"],
                    range,
                )
            ).update_layout(margin=subplot_margin),
        ),
        (
            "prices",
            lambda range: plot.make_prices_section(
                limit_and_resample_df(dataframes["prices"], range).sort_index(axis=1),
                "Prices",
            ).update_layout(margin=subplot_margin),
        ),
        (
            "forex",
            lambda range: plot.make_forex_section(
                limit_and_resample_df(dataframes["forex"], range),
                "Forex",
            ).update_layout(margin=subplot_margin),
        ),
        (
            "interest_rate",
            lambda range: plot.make_interest_rate_section(
                limit_and_resample_df(dataframes["interest_rate"], range)
            ).update_layout(margin=subplot_margin),
        ),
        (
            "brokerage_total",
            lambda range: plot.make_brokerage_total_section(
                limit_and_resample_df(dataframes["brokerages"], range)
            ).update_layout(margin=subplot_margin),
        ),
    ]
    nonranged_graphs: dict[str, PlotlyGraph] = {}
    ranged_graphs: dict[str, dict[str, PlotlyGraph]] = defaultdict(dict)
    nonranged_images: dict[str, ImageGraph] = {}
    ranged_images: dict[str, dict[str, ImageGraph]] = defaultdict(dict)
    with ThreadPoolExecutor() as e:
        fs: list[tuple[Future, str]] = []
        for args in nonranged_graphs_generate:
            fs.append((e.submit(plot_generate, *args, layout), args[0]))
        fs_ranged: list[tuple[Future, str, str]] = []
        for r in ranges:
            for args in ranged_graphs_generate:
                fs_ranged.append(
                    (e.submit(plot_generate, *args, layout, r), args[0], r)
                )
        for f, name in fs:
            g, i = f.result()
            nonranged_graphs[name] = g
            nonranged_images[name] = i
        for f, name, r in fs_ranged:
            g, i = f.result()
            ranged_graphs[name][r] = g
            ranged_images[name][r] = i

    end_time = datetime.now()
    last_updated_time = end_time
    last_generation_duration = end_time - start_time
    latest_datapoint_time = dataframes["all"].index[-1]
    logger.info(f"Graph generation time: {last_generation_duration}")
    return (
        PlotlyGraphs(
            nonranged=nonranged_graphs,
            ranged=ranged_graphs,
            last_updated_time=last_updated_time,
            last_generation_duration=last_generation_duration,
            latest_datapoint_time=latest_datapoint_time,
        ),
        ImageGraphs(
            nonranged=nonranged_images,
            ranged=ranged_images,
        ),
    )
