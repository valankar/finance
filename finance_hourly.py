#!/usr/bin/env python3
"""Run hourly finance functions."""

from concurrent.futures import ThreadPoolExecutor

import commodities
import etfs
import history
import plot
import vanguard_401k
import vanguard_trust


def vanguard():
    """Vanguard functions to run in parallel to others."""
    vanguard_trust.main()
    vanguard_401k.main()


def main():
    """Main."""
    with ThreadPoolExecutor() as pool:
        vanguard_future = pool.submit(vanguard)
        commodities.main()
        etfs.main()
        if ex := vanguard_future.exception():
            print(f'{ex} Exception raised but continuing.')
    history.main()
    plot.main()


if __name__ == '__main__':
    main()
