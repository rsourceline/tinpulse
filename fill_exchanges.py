#!/usr/bin/env python
from pathlib import Path
import sys, json, time, hashlib, os, requests, pandas as pd

MAX_COINS = int(sys.argv[sys.argv.index("--limit")+1]) if "--limit" in sys.argv else 1000
VERBOSE   = "-v" in sys.argv or "--verbose" in sys.argv

CSV_PATH        = Path("cryptos.csv")
EXCH_CACHE_PATH = Path("exch_cache.json")
DEFER_CACHE     = Path("exch_defer.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 TinPulseBot/1.0",
    "x-cg-api-key": os.getenv("COINGECKO_API_KEY", "")
}

EXCH_SLEEP = 1.0
MAX_RETRIES = 3
SKIP_HOURS  = 24

exch_cache = json.loads(EXCH_CACHE_PATH.read_text()) if EXCH_CACHE_PATH.exists() else {}
defer      = json.loads(DEFER_CACHE.read_text())     if DEFER_CACHE.exists() else {}

def safe(v, d=""):
    return d if pd.isna(v) else str(v).strip()

def fetch_exchanges(cid):
    key = hashlib.sha256(cid.encode()).hexdigest()
    if key in exch_cache:
        return exch_cache[key]
    if (ts := defer.get(cid)) and ts + SKIP_HOURS*3600 > time.time():
        return ""

    retries, names, page = 0, set(), 1
    while True:
        url = f"https://api.coingecko.com/api/v3/coins/{cid}/tickers"
        try:
            r = requests.get(
                url,
                params=dict(per_page=100, page=page, include_exchange_logo="false"),
                headers=HEADERS,
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            if VERBOSE:
                print(f"    NET ERR {cid}: {e.__class__.__name__} – defer")
            defer[cid] = time.time()
            return ""

        if r.status_code == 429:
            retries += 1
            if retries > MAX_RETRIES:
                defer[cid] = time.time()
                return ""
            wait = 20 * retries
            if VERBOSE: print(f"    429 {cid} wait {wait}s")
            time.sleep(wait)
            continue

        if r.status_code != 200:
            if VERBOSE: print(f"    HTTP {r.status_code} {cid}")
            return ""

        data = r.json().get("tickers", [])
        for t in data:
            m = safe(t.get("market", {}).get("name"))
            if m: names.add(m)
        if len(data) < 100:
            break
        page += 1
        time.sleep(EXCH_SLEEP)

    exch_str = "|".join(sorted(names))
    if exch_str:
        exch_cache[key] = exch_str
    return exch_str

def main():
    if not CSV_PATH.exists():
        sys.exit("cryptos.csv not found")

    df = pd.read_csv(CSV_PATH, low_memory=False)
    if "exchanges" not in df.columns:
        df["exchanges"] = ""

    blanks = df["exchanges"].isna() | (df["exchanges"].str.strip() == "")
    todo   = df[blanks].index.tolist()[:MAX_COINS]
    if not todo:
        print("Nothing to update.")
        return

    print(f"Processing {len(todo)} coins…")
    updated, start = 0, time.time()
    for n, idx in enumerate(todo, 1):
        cid = safe(df.at[idx, "id"])
        ex  = fetch_exchanges(cid)
        if ex:
            df.at[idx, "exchanges"] = ex
            updated += 1
        if VERBOSE or n % 100 == 0:
            print(f"  {n}/{len(todo)} • added {updated}")
        time.sleep(EXCH_SLEEP)

    df.to_csv(CSV_PATH, index=False)
    EXCH_CACHE_PATH.write_text(json.dumps(exch_cache))
    DEFER_CACHE.write_text(json.dumps(defer))
    print(f"Done – {updated} added; elapsed {int(time.time()-start)}s")

if __name__ == "__main__":
    main()
