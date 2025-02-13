#!/usr/bin/env python3

import asyncio
import subprocess
from datetime import datetime
from typing import Optional

from textual import on, work
from textual.app import App, ComposeResult
from textual.containers import Center, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import (
    Button,
    Input,
    Label,
    ListItem,
    ListView,
    Log,
    MaskedInput,
    TextArea,
)

import ledger_ops


class LedgerEntries(Widget):
    ledger_entries: reactive[list[ledger_ops.LedgerEntry]] = reactive(
        [], recompose=True
    )

    class Selected(Message):
        def __init__(self, entry: ledger_ops.LedgerEntry):
            self.entry = entry
            super().__init__()

    def compose(self) -> ComposeResult:
        items = [ListItem(Label(entry.header())) for entry in self.ledger_entries]
        yield ListView(*items)

    def clear(self):
        self.ledger_entries.clear()
        self.mutate_reactive(LedgerEntries.ledger_entries)

    @work(exclusive=True)
    async def parse_from_ledger_cmd(self, ledger_cmd):
        self.log(ledger_cmd)
        process = await asyncio.create_subprocess_shell(
            ledger_cmd, stdout=subprocess.PIPE
        )
        output, _ = await process.communicate()
        self.ledger_entries = list(
            reversed(ledger_ops.parse_ledger_output(output.decode()))
        )
        self.mutate_reactive(LedgerEntries.ledger_entries)

    @on(ListView.Highlighted)
    def listview_selected(self, event: ListView.Highlighted):
        if (lv_index := event.list_view.index) is None or not len(self.ledger_entries):
            return
        self.post_message(self.Selected(self.ledger_entries[lv_index]))


class ModifiedLedgerEntry(Widget):
    original_entry: reactive[ledger_ops.LedgerEntry | None] = reactive(
        None, recompose=True
    )
    modified_entry: reactive[ledger_ops.LedgerEntry | None] = reactive(
        None, recompose=True
    )

    def compose(self) -> ComposeResult:
        content = ""
        if self.modified_entry:
            content = self.modified_entry.full_str()
        elif self.original_entry:
            content = self.original_entry.full_str()
        yield TextArea(content, show_line_numbers=True)

    def get_textarea_content(self) -> str:
        return self.query_one(TextArea).text

    def set_original_modified(
        self,
        orig: Optional[ledger_ops.LedgerEntry],
        modified: Optional[ledger_ops.LedgerEntry],
    ):
        self.original_entry = orig
        self.modified_entry = modified

    def modify_ledger(self, date_str: str, price: str):
        if not self.original_entry:
            return
        if new_entry := ledger_ops.modify_ledger(self.original_entry, date_str, price):
            self.modified_entry = new_entry


class LedgerAdd(App):
    CSS_PATH = "ledger_add.tcss"

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Input(placeholder="Payee", id="payee", classes="run_ledger")
            yield Input(placeholder="Price", id="price")
            yield Input(placeholder="Commodity", id="commodity", classes="run_ledger")
            yield MaskedInput(
                value=datetime.today().strftime("%Y/%m/%d"),
                placeholder="Date",
                template="0000/00/00",
                id="date",
            )
            yield Input(
                placeholder="Other search term", id="search", classes="run_ledger"
            )
        with Vertical():
            with Center():
                yield Button("Write Ledger Entry", id="write_ledger", disabled=True)
            with Center():
                yield Button("Reset", id="reset")
            yield Log()
        yield LedgerEntries(id="ledger_entries")
        yield ModifiedLedgerEntry()

    @on(Button.Pressed, "#reset")
    async def reset(self):
        await self.recompose()

    def reset_output(self):
        self.query_one(LedgerEntries).clear()
        self.query_one(ModifiedLedgerEntry).set_original_modified(None, None)
        self.query_one("#write_ledger", Button).disabled = True

    def log_message(self, message):
        self.log(message)
        self.query_one(Log).write_line(message)

    @on(Button.Pressed, "#write_ledger")
    def write_ledger(self):
        new_entry = ledger_ops.parse_ledger_entry(
            self.query_one(ModifiedLedgerEntry).get_textarea_content().split("\n")
        )
        if not new_entry:
            self.log_message("Failed to parse ledger entry")
            return
        for log in new_entry.write():
            self.log_message(log)
        self.run_ledger_cmd()

    @on(Input.Changed, ".run_ledger")
    def run_ledger_cmd(self):
        payee = self.query_one("#payee", Input).value
        commodity = self.query_one("#commodity", Input).value
        search = self.query_one("#search", Input).value
        if not any(map(len, [commodity, payee, search])):
            self.reset_output()
            return
        ledger_cmd = ledger_ops.make_ledger_command(payee, commodity, search)
        self.query_one(LedgerEntries).parse_from_ledger_cmd(ledger_cmd)

    @on(Input.Changed, "#price")
    @on(MaskedInput.Changed, "#date")
    def modify_ledger(self):
        date_str = self.query_one("#date", MaskedInput).value
        price = self.query_one("#price", Input).value.upper()
        self.query_one(ModifiedLedgerEntry).modify_ledger(date_str, price)

    @on(LedgerEntries.Selected)
    def process_selection(self, event: LedgerEntries.Selected):
        self.query_one(ModifiedLedgerEntry).set_original_modified(event.entry, None)
        self.query_one("#write_ledger", Button).disabled = False
        self.modify_ledger()


if __name__ == "__main__":
    app = LedgerAdd()
    app.run()
