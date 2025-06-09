#!/usr/bin/env python3

import os
import subprocess

from cyclopts import App

import common

HOST = "valankar@cachyos-server"
UNISON_CMD = "unison -batch -terse"


def run(command: str):
    subprocess.run(command, shell=True, check=True)


app = App()


@app.default
def sync_prod(
    restart_dev: bool = True,
    copy_ledger: bool = True,
    restart_prod: bool = False,
):
    os.chdir(common.CODE_DIR)
    if restart_dev:
        run("docker compose --profile development down")

    run(
        f"{UNISON_CMD} "
        "-force . "
        f"-forcepartial 'BelowPath web -> ssh://{HOST}/code/accounts' "
        "-forcepartial 'Path .schwab_token.json -> newer' "
        "-ignore 'Path .venv' -ignore 'Path __pycache__' "
        f". ssh://{HOST}/code/accounts"
    )
    run(
        f"{UNISON_CMD} -force {common.LEDGER_DIR} -ignore 'Path ledger.ledger' -ignore 'Path prices.db' -ignore 'Path __pycache__' {common.LEDGER_DIR} ssh://{HOST}/code/ledger"
    )
    if copy_ledger:
        run(
            f"{UNISON_CMD} -force ssh://{HOST}/code/ledger -path 'ledger.ledger' -path 'prices.db' {common.LEDGER_DIR} ssh://{HOST}/code/ledger"
        )
    if restart_prod:
        run(f"ssh {HOST} 'cd code/accounts && docker compose restart'")
    if restart_dev:
        run("docker compose --profile development up -d accounts-dev")


if __name__ == "__main__":
    app()
