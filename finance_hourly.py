#!/usr/bin/env python3
"""Run hourly finance functions."""

import etfs
import forex
import history
import ledger_amounts
import ledger_prices_db
import push_web
import schwab_ira


def main():
    """Main."""
    ledger_amounts.main()
    etfs.main()
    forex.main()
    schwab_ira.main()
    ledger_prices_db.main()
    history.main()
    push_web.main()


if __name__ == "__main__":
    main()
