#!/usr/bin/env python3
"""Run weekly finance functions."""

import common


def main():
    """Main."""
    # Test all get_ticker methods to make sure they work.
    common.get_ticker("SCHA", test_all=True)


if __name__ == "__main__":
    main()
