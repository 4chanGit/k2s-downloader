import os
import sys
import pathlib
from concurrent.futures import as_completed

import requests
from requests_futures.sessions import FuturesSession
from tqdm import tqdm


def get_working_proxies(refresh: bool = False):

    if pathlib.Path("proxies.txt").exists() and not refresh:
        with open("proxies.txt") as f:
            proxy_urls = [None] + f.read().splitlines()

        return proxy_urls

    proxies = []

    print("No proxies found, fetching proxies from api.proxyscrape.com...")
    r = requests.get("https://api.proxyscrape.com/?request=getproxies&proxytype=https&timeout=10000&country=all&ssl=all&anonymity=all")
    proxies += r.text.splitlines()
    r = requests.get("https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=10000&country=all&ssl=all&anonymity=all")
    proxies += r.text.splitlines()
    working_proxies = []
    print(f"Checking {len(proxies)} proxies...")

    session = FuturesSession(max_workers=100)
    futures = []
    
    for proxy in proxies:
        future = session.get('https://api.myip.com', proxies={'https': f'http://{proxy}'}, timeout=5)
        future.proxy = proxy
        futures.append(future)

    for future in tqdm(as_completed(futures), total=len(futures)):#, disable=True):
        try:
            future.result()
            working_proxies.append(future.proxy)
        except KeyboardInterrupt:
            sys.exit()
        except:
            continue

    with open("proxies.txt", "w") as f:
        f.write("\n".join(working_proxies))

    os.system("cls")

    return [None] + working_proxies
