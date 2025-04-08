#!/usr/bin/env python3
"""Store Wealthfront Cash yield history."""

import re

import common


def get_yield():
    """Get yield from Wealthfront support page with Selenium."""
    with common.run_with_browser_page(
        # pylint: disable-next=line-too-long
        "https://support.wealthfront.com/hc/en-us/articles/360043680212-Interest-rate-for-Cash-Accounts"
    ) as page:
        s = re.search(
            r"is ([\d\.]+)% as of",
            page.get_by_role("paragraph")
            .filter(has_text="The annual percentage yield")
            .inner_text(),
        )
        if not s:
            raise ValueError("Could not find yield in page content")
        return float(s[1])


def main():
    """Writes 7 day yield history to database."""
    common.insert_sql("wealthfront_cash_yield", {"percent": get_yield()})


if __name__ == "__main__":
    main()
