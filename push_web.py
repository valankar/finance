#!/usr/bin/env python3
"""Push web directory."""

import subprocess

import common


def main():
    """Push to web directory."""
    subprocess.run(f"chmod -R a+rx {common.PREFIX}", shell=True, check=True)


if __name__ == "__main__":
    main()
