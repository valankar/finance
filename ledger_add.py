#!/usr/bin/env python3


import asyncio
import re
import subprocess
from datetime import datetime
from typing import NamedTuple, Optional

from dateutil import parser
from textual import on, work
from textual.app import App, ComposeResult
from textual.containers import Center
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import (
    Button,
    Footer,
    Input,
    Label,
    ListItem,
    ListView,
    MaskedInput,
    TextArea,
)

import common


class LedgerEntry(NamedTuple):
    header: str
    full: list[str]


class LedgerEntries(Widget):
    ledger_entries: reactive[list[LedgerEntry]] = reactive([], recompose=True)

    class Selected(Message):
        def __init__(self, entry: LedgerEntry):
            self.entry = entry
            super().__init__()

    def compose(self) -> ComposeResult:
        items = [ListItem(Label(entry.header)) for entry in self.ledger_entries]
        yield ListView(*items)

    def clear(self):
        self.ledger_entries.clear()
        self.mutate_reactive(LedgerEntries.ledger_entries)

    def parse_ledger_output(self, output: str) -> list[LedgerEntry]:
        entries = []
        for entry in output.split("\n\n"):
            lines = entry.split("\n")
            entries.append(LedgerEntry(header=lines[0], full=lines))
        return entries

    @work(exclusive=True)
    async def parse_from_ledger_cmd(self, ledger_cmd):
        self.log(ledger_cmd)
        process = await asyncio.create_subprocess_shell(
            ledger_cmd, stdout=subprocess.PIPE
        )
        output, _ = await process.communicate()
        self.ledger_entries = list(reversed(self.parse_ledger_output(output.decode())))
        self.mutate_reactive(LedgerEntries.ledger_entries)

    @on(ListView.Highlighted)
    def listview_selected(self, event: ListView.Highlighted):
        if (lv_index := event.list_view.index) is None or not len(self.ledger_entries):
            return
        self.post_message(self.Selected(self.ledger_entries[lv_index]))


class ModifiedLedgerEntry(Widget):
    original_entry: reactive[LedgerEntry | None] = reactive(None, recompose=True)
    modified_entry: reactive[LedgerEntry | None] = reactive(None, recompose=True)

    def compose(self) -> ComposeResult:
        content = ""
        if self.modified_entry:
            content = "\n".join(self.modified_entry.full)
        elif self.original_entry:
            content = "\n".join(self.original_entry.full)
        yield TextArea(content, show_line_numbers=True)

    def get_textarea_content(self) -> str:
        return self.query_one(TextArea).text

    def set_original_modified(
        self, orig: Optional[LedgerEntry], modified: Optional[LedgerEntry]
    ):
        self.original_entry = orig
        self.modified_entry = modified

    def modify_ledger(self, date_str: str, price: str):
        if not self.original_entry:
            return
        output_lines = self.original_entry.full.copy()
        if date_str:
            line = f"{date_str} {' '.join(output_lines[0].split()[1:])}"
            output_lines[0] = line
        if len(price):
            try:
                float(price)
            except ValueError:
                pass
            else:
                price = f"${price}"
            new_output = []
            found = False
            for line in output_lines:
                if not found and any(x in line for x in ["Expenses:", "Liabilities:"]):
                    old_price = re.split(r"\s{2,}", line)[-1]
                    new_output.append(line.replace(old_price, price))
                    found = True
                else:
                    new_output.append(line)
            output_lines = new_output
        if not len(output_lines[-1]):
            output_lines.pop()
        self.modified_entry = LedgerEntry(header=output_lines[0], full=output_lines)


class LedgerAdd(App):
    CSS_PATH = "ledger_add.tcss"

    def compose(self) -> ComposeResult:
        with Center():
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
        with Center():
            yield Button("Write Ledger Entry", id="write_ledger", disabled=True)
            yield Button("Reset", id="reset")
        yield LedgerEntries(id="ledger_entries")
        yield ModifiedLedgerEntry()
        yield Footer()

    @on(Button.Pressed, "#reset")
    async def reset(self):
        await self.recompose()

    def reset_output(self):
        self.query_one(LedgerEntries).clear()
        self.query_one(ModifiedLedgerEntry).set_original_modified(None, None)
        self.query_one("#write_ledger", Button).disabled = True

    @on(Button.Pressed, "#write_ledger")
    def write_ledger(self):
        new_entry = self.query_one(ModifiedLedgerEntry).get_textarea_content()
        if not new_entry:
            self.log("No ledger output to write")
            return
        new_entry += "\n"
        new_entry_date = parser.parse(new_entry.split("\n")[0].split()[0])
        self.log(f"New entry date: {new_entry_date}")
        with open(f"{common.LEDGER_DAT}", "r") as f:
            contents = f.readlines()
        needs_insert = False
        for i, text in enumerate(contents):
            if text and text[0].isdigit():
                entry_date = parser.parse(text.split()[0])
                if entry_date > new_entry_date:
                    needs_insert = True
                    break
        if needs_insert:
            self.log(f"Insert at line {i}")
            contents.insert(i, new_entry)
            with open(f"{common.LEDGER_DAT}", "w") as f:
                f.write("".join(contents))
        else:
            self.log("Appending to end of ledger")
            with open(f"{common.LEDGER_DAT}", "a") as f:
                f.write(new_entry)

        self.run_ledger_cmd()

    @on(Input.Changed, ".run_ledger")
    def run_ledger_cmd(self):
        payee = self.query_one("#payee", Input).value
        commodity = self.query_one("#commodity", Input).value
        search = self.query_one("#search", Input).value
        if not any(map(len, [commodity, payee, search])):
            self.reset_output()
            return
        args = []
        if payee:
            args.append(f"payee {payee}")
        if search:
            if not payee:
                args.append(search)
            else:
                args.append(f"and {search}")
        if commodity:
            args.append(f"--limit 'commodity=~/{commodity}/'")
        ledger_cmd = f"{common.LEDGER_PREFIX} --tail 10 print {' '.join(args)}"
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
