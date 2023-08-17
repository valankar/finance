#!/usr/bin/env python3
"""Write finance history."""

import subprocess

import pandas as pd

import common

LEDGER_LIQUID_CMD = (
    f"{common.LEDGER_PREFIX} --limit 'commodity=~/(SWVXX|\\\\$|^CHF|^GBP|^SGD)/' "
    "--limit 'not(account=~/(Retirement|Precious Metals|Treasury|Zurcher)/)' -J "
    "-n bal \\(^assets or ^liabilities\\)"
)
LEDGER_COMMODITIES_CMD = (
    f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Investments:Precious Metals"'
)
LEDGER_ETFS_CMD = (
    f'{common.LEDGER_PREFIX} --limit "commodity=~/^SCH/" -J -n bal '
    '^"Assets:Investments:Charles Schwab .*Brokerage"'
)
LEDGER_IRA_CMD = (
    f'{common.LEDGER_PREFIX} --limit "commodity=~/^SWYGX/" -J -n bal '
    '^"Assets:Investments:Retirement:Charles Schwab IRA"'
)
LEDGER_TREASURY_DIRECT_CMD = (
    f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Investments:Treasury Direct"'
)
LEDGER_REAL_ESTATE_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Real Estate"'
LEDGER_UBS_PILLAR_CMD = (
    f"{common.LEDGER_PREFIX} -J -n bal "
    '^"Assets:Investments:Retirement:UBS Vested Benefits"'
)
LEDGER_ZURCHER_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Zurcher Kantonal"'


def get_ledger_balance(command):
    """Get account balance from ledger."""
    return float(
        subprocess.check_output(f"{command} | tail -1", shell=True, text=True).split()[
            1
        ]
    )


def main():
    """Main."""
    commodities = get_ledger_balance(LEDGER_COMMODITIES_CMD)
    etfs = get_ledger_balance(LEDGER_ETFS_CMD)
    treasury_direct = get_ledger_balance(LEDGER_TREASURY_DIRECT_CMD)
    total_investing = commodities + etfs + treasury_direct
    total_real_estate = get_ledger_balance(LEDGER_REAL_ESTATE_CMD)

    # Retirement
    schwab_ira = get_ledger_balance(LEDGER_IRA_CMD)
    pillar2 = get_ledger_balance(LEDGER_UBS_PILLAR_CMD)
    zurcher = get_ledger_balance(LEDGER_ZURCHER_CMD)
    total_retirement = pillar2 + zurcher + schwab_ira

    history_df_data = {
        "total_real_estate": total_real_estate,
        "total_liquid": get_ledger_balance(LEDGER_LIQUID_CMD),
        "total_investing": total_investing,
        "total_retirement": total_retirement,
        "etfs": etfs,
        "commodities": commodities,
        "ira": schwab_ira,
        "pillar2": pillar2,
    }
    history_df = pd.DataFrame(
        history_df_data,
        index=[pd.Timestamp.now()],
        columns=history_df_data.keys(),
    )
    common.to_sql(history_df, "history")


if __name__ == "__main__":
    main()
