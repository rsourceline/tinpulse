#!/usr/bin/env python
"""
update_prices.py  – writes to /opt/tinpulse/cryptos.csv

• Updates rank, price, % changes, market cap, volume
• Adds/updates status (Ranked / Unranked)
• Adds/updates last_updated timestamp
• Handles first-run empty CSV
• Writes fixed-point numbers to avoid scientific notation
"""

from pathlib import Path
import time, requests, pandas as pd

# <<< absolute path so cron or manual runs always overwrite the right file
CSV_PATH = Path("/opt/tinpulse/cryptos.csv")
# >>>

RANK_CUTOFF = 1250
VS_CURRENCY = "usd"
PAGE_SIZE   = 250
PAGE_SLEEP  = 0.3

LIVE_COLS = [
    "rank", "price_usd",
    "change_24h_pct", "change_7d_pct", "change_1y_pct",
    "market_cap", "volume_24h",
    "status", "last_updated",
]

# ─── fetch snapshot ──────────────────────────────────────────────────────
def fetch_markets():
    rows, page = [], 1
    while True:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params=dict(
                vs_currency=VS_CURRENCY,
                order="market_cap_desc",
                per_page=PAGE_SIZE,
                page=page,
                sparkline="false",
                price_change_percentage="24h,7d,1y",
            ),
            timeout=20,
        )
        chunk = r.json()
        if not chunk:
            break
        rows.extend(chunk)
        page += 1
        time.sleep(PAGE_SLEEP)

    df = pd.json_normalize(rows)[
        [
            "id",
            "last_updated",
            "market_cap_rank",
            "current_price",
            "price_change_percentage_24h",
            "price_change_percentage_7d_in_currency",
            "price_change_percentage_1y_in_currency",
            "market_cap",
            "total_volume",
        ]
    ].rename(
        columns={
            "market_cap_rank": "rank",
            "current_price":    "price_usd",
            "price_change_percentage_24h": "change_24h_pct",
            "total_volume":     "volume_24h",
        }
    )

    df["change_7d_pct"] = df["price_change_percentage_7d_in_currency"].apply(
        lambda d: d.get("usd") if isinstance(d, dict) else None
    )
    df["change_1y_pct"] = df["price_change_percentage_1y_in_currency"].apply(
        lambda d: d.get("usd") if isinstance(d, dict) else None
    )
    df.drop(
        ["price_change_percentage_7d_in_currency",
         "price_change_percentage_1y_in_currency"],
        axis=1,
        inplace=True,
    )

    df["status"] = df["rank"].apply(
        lambda r: "Ranked" if pd.notna(r) and r <= RANK_CUTOFF else "Unranked"
    )
    return df

# ─── helpers ─────────────────────────────────────────────────────────────
def strip_blank(df):
    return df[df["id"].notna() & (df["id"].astype(str).str.strip() != "")]

def safe_merge(local, market):
    market = strip_blank(market)
    if local.empty or "id" not in local.columns:
        base = market.copy()
    else:
        local = strip_blank(local)
        for col in LIVE_COLS:
            if col not in local.columns:
                local[col] = pd.NA
        base = local.copy()
        live = market.set_index("id")[LIVE_COLS].to_dict("index")
        for idx, row in base.iterrows():
            cid = row["id"]
            if cid in live:
                for col in LIVE_COLS:
                    base.at[idx, col] = live[cid][col]
    new_ids = set(market["id"]) - set(base["id"])
    if new_ids:
        base = pd.concat([base, market[market["id"].isin(new_ids)]],
                         ignore_index=True)
    return base

# ─── main ────────────────────────────────────────────────────────────────
def main():
    df_local  = pd.read_csv(CSV_PATH, low_memory=False) if CSV_PATH.exists() else pd.DataFrame()
    df_market = fetch_markets()
    merged    = safe_merge(df_local, df_market)
    merged.to_csv(CSV_PATH, index=False, float_format="%.12f")
    print(f"Rows after update: {len(merged):,}")

if __name__ == "__main__":
    main()
