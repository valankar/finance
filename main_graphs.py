import base64
import io
import pickle
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Callable, ClassVar, Optional

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
import stock_options

RANGES = ["All", "3y", "2y", "1y", "YTD", "6m", "3m", "1m", "1d"]
DEFAULT_RANGE = "1y"


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

    def limit_and_resample_df(
        self, df: pd.DataFrame, selected_range: str
    ) -> pd.DataFrame:
        """Limit df to selected range and resample."""
        if (retval := self.get_xrange(df, selected_range)) is None:
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

    def common_links(self):
        with ui.row().classes("flex justify-center w-full"):
            ui.link("Dynamic Graphs", "/")
            ui.link("Static Images", "/image_only")
            ui.link("Matplot", "/matplot")
            ui.link("Stock Options", "/stock_options")
            ui.link("Transactions", "/transactions")


class MainGraphs(GraphCommon):
    """Collection of all main graphs."""

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
        self.ui_plotly_ranged: dict[str, ui.plotly] = {}
        self.selected_range = DEFAULT_RANGE
        self.db = db
        self.plotly_graphs = db.Hash(self.REDIS_KEY)

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
        self.common_links()

    async def update(self) -> None:
        """Update all graphs."""
        for ui_key, ui_plotly in self.ui_plotly_ranged.items():
            name = f"{ui_key}:{self.selected_range}"
            if graph := self.get_plotly_json(name):
                await run.io_bound(ui_plotly.update_figure, graph)

    def get_plot_height_percent(
        self, name: str, layout: tuple[tuple[str, str], ...]
    ) -> float:
        for n, height in layout:
            if n == name:
                return float(int(height[:-2]) / 100)
        return 1.0

    def plot_generate(
        self,
        name: str,
        plot_func: Callable[..., Figure],
        layout: tuple[tuple[str, str], ...],
        r: Optional[str] = None,
    ) -> tuple[dict, bytes]:
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
            height=768 * self.get_plot_height_percent(name, layout),
        )
        return (
            fig.to_plotly_json(),
            data.getvalue(),
        )

    def generate(self):
        """Generate and save all Plotly graphs."""
        logger.info("Generating graphs")
        layout = self.LAYOUT
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
        if (options_data := stock_options.get_options_data()) is None:
            raise ValueError("No options data available")
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
                lambda: plot.make_loan_section(
                    options_data.opts.options_value_by_brokerage
                ).update_layout(margin=subplot_margin),
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
                    self.limit_and_resample_df(dataframes["all"], range)
                ).update_layout(margin=subplot_margin),
            ),
            (
                "investing_retirement",
                lambda range: plot.make_investing_retirement_section(
                    self.limit_and_resample_df(
                        dataframes["all"][["pillar2", "ira", "commodities", "etfs"]],
                        range,
                    )
                ).update_layout(margin=subplot_margin),
            ),
            (
                "real_estate",
                lambda range: plot.make_real_estate_section(
                    self.limit_and_resample_df(
                        dataframes["real_estate"],
                        range,
                    )
                ).update_layout(margin=subplot_margin),
            ),
            (
                "prices",
                lambda range: plot.make_prices_section(
                    self.limit_and_resample_df(dataframes["prices"], range).sort_index(
                        axis=1
                    ),
                    "Prices",
                ).update_layout(margin=subplot_margin),
            ),
            (
                "forex",
                lambda range: plot.make_forex_section(
                    self.limit_and_resample_df(dataframes["forex"], range),
                    "Forex",
                ).update_layout(margin=subplot_margin),
            ),
            (
                "interest_rate",
                lambda range: plot.make_interest_rate_section(
                    self.limit_and_resample_df(dataframes["interest_rate"], range)
                ).update_layout(margin=subplot_margin),
            ),
            (
                "brokerage_total",
                lambda range: plot.make_brokerage_total_section(
                    self.limit_and_resample_df(dataframes["brokerages"], range)
                ).update_layout(margin=subplot_margin),
            ),
        ]
        mgio = MainGraphsImageOnly(self.db)
        with ThreadPoolExecutor() as e:
            fs: list[tuple[Future, str]] = []
            for args in nonranged_graphs_generate:
                fs.append((e.submit(self.plot_generate, *args, layout), args[0]))
            fs_ranged: list[tuple[Future, str, str]] = []
            for r in ranges:
                for args in ranged_graphs_generate:
                    fs_ranged.append(
                        (e.submit(self.plot_generate, *args, layout, r), args[0], r)
                    )
            for f, name in fs:
                g, i = f.result()
                self.plotly_graphs[self.make_redis_key(name)] = pickle.dumps(g)
                mgio.image_graphs[mgio.make_redis_key(name)] = i
            for f, name, r in fs_ranged:
                g, i = f.result()
                self.plotly_graphs[self.make_redis_key(name, r)] = pickle.dumps(g)
                mgio.image_graphs[mgio.make_redis_key(name, r)] = i
        end_time = datetime.now()
        last_generation_duration = end_time - start_time
        logger.info(f"Graph generation time: {last_generation_duration}")


class MainGraphsImageOnly(GraphCommon):
    REDIS_SUBKEY: ClassVar[str] = "PlotlyImageGraphs"

    def __init__(self, db: walrus.Database):
        self.ui_image_ranged: dict[str, ui.image] = {}
        self.selected_range = DEFAULT_RANGE
        self.image_graphs = db.Hash(self.REDIS_KEY)

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
        self.common_links()

    async def update(self) -> None:
        for ui_key, image in self.ui_image_ranged.items():
            name = f"{ui_key}:{self.selected_range}"
            if graph := self.image_graphs.get(name):
                await run.io_bound(
                    image.set_source,
                    self.encode_png(graph),
                )
