#!/usr/bin/env python3
"""Determine price for Vanguard trust.

For details see:

https://groups.google.com/g/ledger-cli/c/6Uk2BfRAkms?pli=1
"""

import random
import sys
from concurrent.futures import ProcessPoolExecutor
from typing import Tuple, TypedDict
import warnings
from datetime import datetime, timedelta
from urllib3.exceptions import InsecureRequestWarning

import requests
from fp.fp import FreeProxy
import numpy as np

import common

# How many days to go back when querying on weekends.
NUM_DAYS_BACK = 7
# How many proxy fetches to do in parallel.
PROXY_PARALLEL = 5
# Number of seconds timeout when using proxies.
PROXY_FETCH_TIMEOUT = 5
LAST_WORKING_PROXY_FILE = common.PREFIX + 'last_working_proxy.txt'

# pylint: disable=line-too-long
HEADERS = {
    'Accept':
    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'sec-ch-ua':
    '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


class ProxyFetchArgs(TypedDict):
    """Arguments for proxy fetches."""
    url: str
    proxy_address: str


def single_proxy_fetch(
        arg_dict: ProxyFetchArgs) -> Tuple[requests.Response, str] | None:
    """Single url fetch to be called by Pool.map.

    Args:
        arg_dict: dict with 'url' and 'proxy_address'

    Returns:
        (request, working proxy address) on success, else None.
    """
    url = arg_dict['url']
    proxy_address = arg_dict['proxy_address']
    try:
        request = requests.get(url=url,
                               headers=HEADERS,
                               verify=False,
                               timeout=PROXY_FETCH_TIMEOUT,
                               proxies={'https': f'http://{proxy_address}'})
    except requests.exceptions.RequestException:
        return None
    # pylint: disable-next=no-member
    if request and request.status_code == requests.codes.ok:
        if request.text.startswith('['):
            return (request, proxy_address)
    return None


def parallel_proxy_fetch(url, working_proxies, parallel=PROXY_PARALLEL):
    """Fetch in parallel from multiple proxies."""
    # First try with working proxies.
    for proxy_address in working_proxies:
        result = single_proxy_fetch({
            'url': url,
            'proxy_address': proxy_address
        })
        if result:
            return result
    proxy_list = FreeProxy().get_proxy_list()
    random.shuffle(proxy_list)
    # Put working proxies at front of the list.
    if working_proxies:
        proxy_list = (list(working_proxies) +
                      list(set(proxy_list).difference(working_proxies)))
    fetch_args = []
    for proxy_address in proxy_list:
        fetch_args.append({'url': url, 'proxy_address': proxy_address})
    with ProcessPoolExecutor() as pool:
        # Do parallel at a time and check results.
        for chunk in np.array_split(np.array(fetch_args),
                                    int(len(fetch_args) / parallel)):
            results = pool.map(single_proxy_fetch, list(chunk))
            for result in results:
                if result:
                    return result
    return None


def save_proxy(proxy_address):
    """Save proxy address to file for reuse."""
    with open(LAST_WORKING_PROXY_FILE, 'w', encoding='utf-8') as proxy_file:
        proxy_file.write(proxy_address)


def get_last_working_proxy() -> set:
    """Gets last working proxy from file."""
    working_proxy = set()
    try:
        with open(LAST_WORKING_PROXY_FILE, 'r',
                  encoding='utf-8') as proxy_file:
            working_proxy.add(proxy_file.read())
    except FileNotFoundError:
        pass
    return working_proxy


def get_url(days_back):
    """Generate Vanguard API URL."""
    today = datetime.today().strftime('%Y-%m-%d')
    day = (datetime.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    # Vanguard Target Retirement 2040 Trust, fund ID 7741.
    return (
        'https://eds.ecs.gisp.c1.vanguard.com/eds-eip-distributions-service/price/'
        f'daily-nav-history/7741.json?start-date={day}&end-date={today}')


def get_price_direct():
    """Get Vanguard trust price directly without proxy."""
    price = None
    for days_back in range(1, NUM_DAYS_BACK):
        url = get_url(days_back)
        request = requests.get(url=url,
                               headers=HEADERS,
                               verify=False,
                               timeout=PROXY_FETCH_TIMEOUT)
        data = request.json()
        # [{'priceItem': [{'price': 106.26, 'effectiveDate': '2022-06-15',
        # 'currencyCode': 'USD', 'priceTypeCode': 'NAV'}], 'accountingTypeCode': 46,
        # 'portId': '7741'}]
        try:
            price = str(data[0]['priceItem'][-1]['price'])
            break
        except IndexError:
            continue
    return price


def get_price_proxy():
    """Get Vanguard trust via proxy."""
    working_proxies = get_last_working_proxy()
    price = None
    for days_back in range(1, NUM_DAYS_BACK):
        url = get_url(days_back)
        print(f'PROXY: {url}')
        # Go through a bunch of proxies.
        result = parallel_proxy_fetch(url, working_proxies)
        if not result:
            continue
        request, working_proxy = result
        working_proxies.add(working_proxy)
        save_proxy(working_proxy)
        data = request.json()
        # [{'priceItem': [{'price': 106.26, 'effectiveDate': '2022-06-15',
        # 'currencyCode': 'USD', 'priceTypeCode': 'NAV'}], 'accountingTypeCode': 46,
        # 'portId': '7741'}]
        try:
            price = str(data[0]['priceItem'][-1]['price'])
            break
        except IndexError:
            continue
    return price


def main():
    """Main."""
    warnings.simplefilter('ignore', InsecureRequestWarning)
    try:
        price = get_price_direct()
    except requests.exceptions.RequestException:
        price = get_price_proxy()
    if not price:
        sys.exit('Failed to get Vanguard Target Retirement 2040 Trust price.')
    output = common.PREFIX + 'vanguard.txt'
    with common.temporary_file_move(output) as output_file:
        output_file.write(price)


if __name__ == '__main__':
    main()
