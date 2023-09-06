#!/usr/bin/env python3
"""Push web directory."""

import subprocess

import common

DESTINATION = "debian:/home/valankar/caddy/site/accounts_data/"


def main():
    """Push to web directory."""
    subprocess.run(f"chmod -R a+rx {common.PREFIX}", shell=True, check=True)
    subprocess.run(
        f"rsync -aq --delete {common.PREFIX} {DESTINATION}", shell=True, check=True
    )


if __name__ == "__main__":
    main()
