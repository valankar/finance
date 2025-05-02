import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dateutil import parser

import common


@dataclass
class LedgerEntry:
    date: str
    payee: str
    body: list[str]

    def header(self) -> str:
        return f"{self.date} {self.payee}"

    def parse_date(self) -> datetime:
        return parser.parse(self.date)

    def full_list(self) -> list[str]:
        lines = [self.header()]
        lines.extend(self.body)
        return lines

    def full_str(self) -> str:
        return "\n".join(self.full_list())

    def validate(self) -> tuple[bool, str]:
        ledger_cmd = f"{common.LEDGER_BIN} -f -"
        try:
            output = subprocess.check_output(
                ledger_cmd,
                input=self.full_str(),
                shell=True,
                text=True,
                stderr=subprocess.PIPE,
            )
            return True, output
        except subprocess.CalledProcessError as e:
            return False, e.stderr

    def write(self) -> tuple[bool, list[str]]:
        logs = []
        validated, output = self.validate()
        if not validated:
            logs.append(output)
            return False, logs
        new_entry_date = self.parse_date()
        logs.append(f"New entry date: {new_entry_date}")
        with open(f"{common.LEDGER_DAT}", "r") as f:
            contents = f.readlines()
        needs_insert = False
        i = 0
        for i, text in enumerate(contents):
            if text and text[0].isdigit():
                entry_date = parser.parse(text.split()[0])
                if entry_date > new_entry_date:
                    needs_insert = True
                    break
        if needs_insert:
            logs.append(f"Insert at line {i}")
            contents.insert(i, self.full_str() + "\n")
            with open(f"{common.LEDGER_DAT}", "w") as f:
                f.write("".join(contents))
        else:
            logs.append("Appending to end of ledger")
            with open(f"{common.LEDGER_DAT}", "a") as f:
                f.write(self.full_str() + "\n")
        return True, logs


def modify_ledger(
    entry: LedgerEntry, date_str: str, price: str
) -> Optional[LedgerEntry]:
    output_lines = entry.full_list()
    price = price.upper()
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
    output_lines = [x for x in output_lines if x]
    if (new_entry := parse_ledger_entry(output_lines)) is not None:
        if date_str:
            new_entry.date = date_str
        return new_entry
    return None


def parse_ledger_entry(lines: list[str]) -> Optional[LedgerEntry]:
    try:
        date = lines[0].split()[0]
        payee = lines[0].split(maxsplit=1)[1]
        return LedgerEntry(date=date, payee=payee, body=lines[1:])
    except IndexError:
        pass
    return None


def parse_ledger_output(output: str) -> list[LedgerEntry]:
    entries = []
    for entry in output.split("\n\n"):
        lines = [x for x in entry.split("\n") if x]
        if ledger := parse_ledger_entry(lines):
            entries.append(ledger)
    return entries


def make_ledger_command(
    payee: Optional[str] = None,
    commodity: Optional[str] = None,
    search: Optional[str] = None,
) -> str:
    args = []
    if payee:
        args.append(f"payee '{payee}'")
    if search:
        if not payee:
            args.append(f"'{search}'")
        else:
            args.append(f"and '{search}'")
    if commodity:
        args.append(f"--limit 'commodity=~/{commodity}/'")
    ledger_prefix = common.LEDGER_PREFIX.replace("-c ", "")
    return f"{ledger_prefix} --tail 10 print {' '.join(args)}"


def get_ledger_entries(
    payee: Optional[str] = None,
    commodity: Optional[str] = None,
    search: Optional[str] = None,
) -> list[LedgerEntry]:
    ledger_cmd = make_ledger_command(payee, commodity, search)
    return get_ledger_entries_from_command(ledger_cmd)


def get_ledger_entries_from_command(command: str) -> list[LedgerEntry]:
    return parse_ledger_output(subprocess.check_output(command, shell=True, text=True))


def get_ledger_balance(command):
    """Get account balance from ledger."""
    try:
        return float(
            subprocess.check_output(
                f"{command} | tail -1", shell=True, text=True
            ).split()[1]
        )
    except IndexError:
        return 0
