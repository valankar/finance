from copy import deepcopy
from datetime import datetime
from typing import Optional

from nicegui import binding, run, ui

import ledger_ops


@binding.bindable_dataclass
class Input:
    payee: str = ""
    price: str = ""
    commodity: str = ""
    date: str = ""
    search: str = ""


class LedgerUI:
    def __init__(self):
        self.reset()

    def reset(self):
        self.input = Input()
        self.input.date = datetime.today().strftime("%Y/%m/%d")
        self.log: Optional[ui.log] = None
        self.aggrid: Optional[ui.aggrid] = None
        self.aggrid_data: list[dict] = []
        self.results: list[ledger_ops.LedgerEntry] = []
        self.selected_index: Optional[int] = None
        self.editor_data: str = ""

    async def process_search(
        self, input: Input
    ) -> tuple[Input, list[ledger_ops.LedgerEntry]]:
        input = deepcopy(input)
        results = await run.io_bound(
            lambda: ledger_ops.get_ledger_entries(
                input.payee, input.commodity, input.search
            )
        )
        return (input, results)

    async def input_changed(self):
        if not self.aggrid:
            return
        if not any(
            map(len, [self.input.commodity, self.input.payee, self.input.search])
        ):
            self.aggrid_data.clear()
            self.aggrid.update()
            return
        input, results = await self.process_search(self.input)
        if input != self.input:
            # Ignore results that no longer match query
            return
        self.aggrid_data.clear()
        self.results = list(reversed(results))
        for entry in self.results:
            self.aggrid_data.append(
                {
                    "date": entry.date,
                    "payee": entry.payee,
                    "amount": entry.first_amount(),
                }
            )
        self.aggrid.update()
        self.aggrid.run_row_method("0", "setSelected", True)

    def modify_ledger(self):
        if self.selected_index is not None:
            output_lines = self.results[self.selected_index].full_list()
        else:
            output_lines = self.editor_data.split("\n")
        if not output_lines:
            return
        if not (ledger := ledger_ops.parse_ledger_entry(output_lines)):
            return
        if new_entry := ledger_ops.modify_ledger(
            ledger, self.input.date, self.input.price
        ):
            self.editor_data = new_entry.full_str()

    async def write_ledger(self):
        if not self.log:
            return
        if not self.editor_data:
            self.log.push("No ledger output to write")
            return
        if (
            new_entry := ledger_ops.parse_ledger_entry(self.editor_data.split("\n"))
        ) is None:
            self.log.push("Failed to parse ledger entry")
            return
        success, logs = new_entry.write()
        for log in logs:
            self.log.push(log)
        if success:
            await self.input_changed()

    def row_selected(self, data: dict):
        if not data["selected"]:
            return
        self.selected_index = int(data["rowIndex"])
        self.editor_data = self.results[self.selected_index].full_str()
        self.modify_ledger()

    @ui.refreshable_method
    async def main_page(self):
        self.reset()
        with ui.grid().classes("w-full gap-0 md:grid-cols-2"):
            with ui.card():
                with ui.grid(columns=2):
                    with ui.column():
                        ui.input(
                            label="Payee", on_change=self.input_changed
                        ).bind_value(self.input, "payee")
                        ui.input(
                            label="Price", on_change=self.modify_ledger
                        ).bind_value(self.input, "price")
                        ui.input(
                            label="Commodity", on_change=self.input_changed
                        ).bind_value(self.input, "commodity")
                        ui.input(
                            label="Other search term",
                            on_change=self.input_changed,
                        ).bind_value(self.input, "search")
                    ui.date(mask="YYYY/MM/DD", on_change=self.modify_ledger).bind_value(
                        self.input, "date"
                    )
            with ui.card(align_items="center"):
                ui.button(
                    text="Write Ledger Entry", on_click=self.write_ledger
                ).bind_enabled_from(self, "editor_data")
                ui.button(text="Reset", on_click=self.main_page.refresh)
                ui.codemirror(theme="basicDark").bind_value(self, "editor_data")
            with ui.card():
                self.aggrid = (
                    ui.aggrid(
                        {
                            "columnDefs": [
                                {
                                    "headerName": "Date",
                                    "field": "date",
                                    "sortable": False,
                                },
                                {
                                    "headerName": "Payee",
                                    "field": "payee",
                                    "sortable": False,
                                },
                                {
                                    "headerName": "Amount",
                                    "field": "amount",
                                    "sortable": False,
                                },
                            ],
                            "rowData": self.aggrid_data,
                            "rowSelection": "single",
                        }
                    )
                    .classes("ag-theme-balham-dark")
                    .on("rowSelected", lambda msg: self.row_selected(msg.args))
                )
            with ui.card(align_items="stretch"):
                self.log = ui.log()
