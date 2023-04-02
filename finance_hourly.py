#!/usr/bin/env python3
"""Run hourly finance functions."""

import commodities
import etfs
import history
import plot
import vanguard_401k
import vanguard_trust


def main():
    """Main."""
    commodities.main()
    etfs.main()
    vanguard_trust.main()
    vanguard_401k.main()
    history.main()
    plot.main()


if __name__ == '__main__':
    main()
