#!/usr/bin/env python
"""
update_daily_meta.py
────────────────────────────────────────────────────────────────────────────
Nightly fundamentals & link refresh for TinPulse.

Adds / refreshes:
    ath_price, ath_date, change_30d_pct,
    chains, explorer_url, contract_address,
    telegram_url, reddit_url, discord_url, twitter_url

• Processes up to 1000 coins per run by default (override with --limit N)
• Skips coins whose last_updated hasn't changed.
• Respects free-tier rate limit with 429 back-off and 24-hour defer.
• Writes fixed-point numbers (float_format="%.12f").
"""

from pathlib import Path
import sys, time, json, hashlib, os, requests, pandas as pd

# ─── CLI flags ────────────────────────────────────────────────────────────
MAX_COINS = int(sys.argv[sys.argv.index("--limit")+1]) if "--limit" in sys.argv else 1000
VERBOSE   = "-v" in sys.argv or "--verbose" in sys.argv

# ─── CONFIG ───────────────────────────────────────────────────────────────
CSV_PATH       = Path("cryptos.csv")
LU_CACHE_PATH  = Path("last_updated_cache.json")
DEFER_CACHE    = Path("daily_defer.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 TinPulseBot/1.0",
    "x-cg-api-key": os.getenv("COINGECKO_API_KEY", "")
}

MARKET_PAGE_SLEEP = 0.3
COIN_SLEEP        = 1.0
MAX_429_RETRIES   = 2
DEFER_HOURS       = 24

CORE_RANK_MAX = 1250
CORE_VOL_MIN  = 5_000_000
# ──────────────────────────────────────────────────────────────────────────

lu_cache = json.loads(LU_CACHE_PATH.read_text()) if LU_CACHE_PATH.exists() else {}
defer    = json.loads(DEFER_CACHE.read_text())   if DEFER_CACHE.exists()   else {}

# ─── helpers ──────────────────────────────────────────────────────────────
def deep_get(d,*path):
    for p in path:
        if isinstance(p,int):
            if not isinstance(d,list) or len(d)<=p:return""
            d=d[p]
        else:
            d=d.get(p,{})
    return d if not isinstance(d,dict) else ""

def fetch_markets():
    rows,page,retries=[],1,0
    while True:
        r=requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params=dict(vs_currency="usd",order="market_cap_desc",
                        per_page=250,page=page,sparkline="false"),
            headers=HEADERS,timeout=20)
        if r.status_code==429:
            retries+=1; wait=20*retries
            if VERBOSE: print(f"  /markets 429 page {page} wait {wait}s")
            time.sleep(wait); continue
        if r.status_code!=200:
            raise RuntimeError(f"/markets HTTP {r.status_code}")
        chunk=r.json()
        if not chunk: break
        rows.extend(chunk); page+=1; retries=0
        time.sleep(MARKET_PAGE_SLEEP)

    rank_map={c["id"]: (c.get("market_cap_rank") or 999_999) for c in rows}
    vol_map ={c["id"]: (c.get("total_volume")   or 0)        for c in rows}
    ts_map  ={c["id"]:  c["last_updated"]                    for c in rows}
    return rank_map,vol_map,ts_map

def is_core(cid, rank_map, vol_map):
    return rank_map.get(cid,999_999)<=CORE_RANK_MAX or vol_map.get(cid,0)>=CORE_VOL_MIN

def fetch_meta(cid):
    if (ts:=defer.get(cid)) and ts+DEFER_HOURS*3600>time.time():
        return {}
    tries=0
    while True:
        r=requests.get(
            f"https://api.coingecko.com/api/v3/coins/{cid}",
            params=dict(localization="false",tickers="false",
                        market_data="true",community_data="false",
                        developer_data="false",sparkline="false",
                        price_change_percentage="30d"),
            headers=HEADERS,timeout=30)
        if r.status_code==429:
            tries+=1
            if tries>MAX_429_RETRIES:
                defer[cid]=time.time(); return {}
            wait=20*tries
            if VERBOSE: print(f"  429 {cid} wait {wait}s")
            time.sleep(wait); continue
        if r.status_code!=200:
            if VERBOSE: print(f"  HTTP {r.status_code} {cid}")
            defer[cid]=time.time(); return {}
        try:
            j=r.json()
        except ValueError:
            if VERBOSE: print(f"  Non-JSON {cid} defer")
            defer[cid]=time.time(); return {}

        pct30=deep_get(j,"market_data","price_change_percentage_30d_in_currency","usd")
        plats=j.get("platforms") if isinstance(j.get("platforms"),dict) else {}
        chains="|".join(plats.keys()); contract=plats.get("ethereum","")

        return dict(
            id=cid,
            ath_price       = deep_get(j,"market_data","ath","usd"),
            ath_date        = deep_get(j,"market_data","ath_date","usd")[:10],
            change_30d_pct  = pct30,
            chains          = chains,
            explorer_url    = next((u for u in deep_get(j,"links","blockchain_site") if u), ""),
            contract_address= contract,
            telegram_url    = (f"https://t.me/{deep_get(j,'links','telegram_channel_identifier')}"
                               if deep_get(j,'links','telegram_channel_identifier') else ""),
            reddit_url      = deep_get(j,"links","subreddit_url"),
            discord_url     = next((u for u in deep_get(j,"links","chat_url")
                                    if "discord" in u.lower()), ""),
            twitter_url     = (f"https://twitter.com/{deep_get(j,'links','twitter_screen_name')}"
                               if deep_get(j,'links','twitter_screen_name') else ""),
        )

# ─── main ────────────────────────────────────────────────────────────────
def main():
    if not CSV_PATH.exists():
        sys.exit("cryptos.csv missing – run update_prices.py first.")
    df = pd.read_csv(CSV_PATH, low_memory=False)

    TARGET_COLS = [
        "ath_price","ath_date","change_30d_pct","chains",
        "explorer_url","contract_address","telegram_url",
        "reddit_url","discord_url","twitter_url"
    ]
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = ""

    rank_map, vol_map, ts_map = fetch_markets()

    candidates = [
        (cid, ts) for cid, ts in ts_map.items()
        if is_core(cid, rank_map, vol_map) and lu_cache.get(cid) != ts
    ][:MAX_COINS]

    if not candidates:
        print("Daily meta: nothing to update."); return

    print(f"Updating {len(candidates)} coin(s)…")
    updates=[]
    start=time.time()
    for n,(cid,ts) in enumerate(candidates,1):
        meta=fetch_meta(cid)
        if meta:
            updates.append(meta); lu_cache[cid]=ts
        if VERBOSE or n % 100 == 0:
            print(f"  {n}/{len(candidates)} processed")
        time.sleep(COIN_SLEEP)

    if updates:
        meta_df = pd.DataFrame(updates).set_index("id").astype("object")
        df.set_index("id", inplace=True)
        df[meta_df.columns] = df[meta_df.columns].astype("object")
        df.update(meta_df)
        df.reset_index().to_csv(CSV_PATH, index=False, float_format="%.12f")
        print(f"Saved; {len(updates)} rows updated.")
    else:
        print("No rows updated this batch.")

    LU_CACHE_PATH.write_text(json.dumps(lu_cache))
    DEFER_CACHE.write_text(json.dumps(defer))
    print(f"Run duration: {round(time.time()-start,1)}s")

if __name__ == "__main__":
    main()
