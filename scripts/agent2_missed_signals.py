"""
Samvex Sandbox — Agent 2: Missed Signals Analyst
=================================================
Runs at 4:30 PM IST daily (GitHub Actions).

For each Nifty 500 stock:
  - Checks if it moved ≥ MISS_THRESHOLD_PCT from its intraday high (Exhaustion Short)
    or from its intraday low (future: long setups) today
  - Cross-checks against today's live signals: was it already caught?
  - If missed: runs through each Exhaustion Short gate in sequence,
    identifies the first gate that rejected it, and logs the blocking value vs threshold

Output: appends one record per missed stock to data/missed_signals.json
"""

import json, os, sys
from datetime import datetime
from pathlib import Path

import pytz, requests, yfinance as yf, pandas as pd

IST       = pytz.timezone("Asia/Kolkata")
NOW       = datetime.now(IST)
TODAY_STR = NOW.strftime("%Y-%m-%d")
API_BASE  = "https://samvex-api.onrender.com"

MISS_THRESHOLD_PCT   = 5.0   # stock must have fallen ≥5% from intraday high to count as "missed short"
MISS_RALLY_1D_PCT    = 6.0   # same as EXH_PREV_DAY_RALLY_PCT
MISS_CUMUL_PCT       = 10.0  # same as EXH_CUMUL_RALLY_PCT
MISS_VOL_RATIO       = 1.3   # same as EXH_VOL_RATIO
MISS_IMPULSE_MOVE    = 1.5   # same as EXH_IMPULSE_MOVE_PCT
MISS_TURNOVER_PCT    = 5.0   # same as EXH_IMPULSE_TURNOVER_PCT

DATA_FILE = Path("data/missed_signals.json")


def load_existing():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except Exception:
            pass
    return []


def fetch_today_signals():
    """Returns set of symbols already caught by the live scanner today."""
    try:
        r = requests.get(API_BASE + "/api/signals/all-today.json", timeout=20)
        r.raise_for_status()
        sigs = r.json().get("signals", [])
        return {s.get("symbol", "").replace(".NS", "") for s in sigs}
    except Exception as e:
        print(f"[Signals] fetch failed: {e}")
        return set()


_NIFTY100_FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","KOTAKBANK","HINDUNILVR","AXISBANK",
    "BAJFINANCE","BHARTIARTL","MARUTI","SUNPHARMA","TITAN","WIPRO","LT","ULTRACEMCO",
    "ONGC","TECHM","NTPC","POWERGRID","ADANIENT","ADANIPORTS","GRASIM","ASIANPAINT",
    "HCLTECH","DIVISLAB","BRITANNIA","DRREDDY","EICHERMOT","CIPLA","COALINDIA","BPCL",
    "IOC","TATACONSUM","HINDALCO","TATASTEEL","JSWSTEEL","TATAMOTORS","M&M","BAJAJ-AUTO",
    "HEROMOTOCO","BAJAJFINSV","SBILIFE","HDFCLIFE","INDUSINDBK","NESTLEIND","ITC","BEL",
    "SBIN","BANKBARODA","CANBK","PNB","FEDERALBNK","IDFCFIRSTB","BANDHANBNK","RBLBANK",
    "SHRIRAMFIN","CHOLAFIN","MUTHOOTFIN","BAJAJHLDNG","PIDILITIND","BERGEPAINT","HAVELLS",
    "SIEMENS","ABB","VOLTAS","COLPAL","GODREJCP","TATAPOWER","ADANIGREEN","VEDL",
    "AMBUJACEM","ACC","SHREECEM","LTIM","MPHASIS","LTTS","PERSISTENT","COFORGE",
    "ZOMATO","IRCTC","NYKAA","POLICYBZR","LICI","SBICARD","DMART","JUBLFOOD",
    "MARICO","EMAMILTD","GODREJIND","WHIRLPOOL","CROMPTON","BATAINDIA","PAGEIND",
    "APOLLOHOSP","MAXHEALTH","FORTIS","LUPIN","AUROPHARMA","ALKEM","TORNTPHARM",
    "GLAND","LAURUSLABS","ABBOTINDIA","SANOFI","PFIZER","BIOCON","BALKRISIND",
]

def fetch_nifty500():
    try:
        r = requests.get(API_BASE + "/api/universe", timeout=20)
        r.raise_for_status()
        universe = [s.replace(".NS", "") for s in r.json()]
        if universe:
            print(f"[Universe] fetched {len(universe)} symbols from API")
            return universe
    except Exception as e:
        print(f"[Universe] API fetch failed: {e}")
    print(f"[Universe] using hardcoded Nifty 100 fallback ({len(_NIFTY100_FALLBACK)} symbols)")
    return _NIFTY100_FALLBACK


def get_daily(symbol):
    try:
        tk = yf.Ticker(symbol + ".NS")
        df = tk.history(period="30d", interval="1d")
        if df is None or len(df) < 6:
            return None
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
        else:
            df.index = df.index.tz_convert(IST)
        return df
    except Exception:
        return None


def get_intraday5(symbol):
    try:
        tk = yf.Ticker(symbol + ".NS")
        df = tk.history(period="5d", interval="5m")
        if df is None or df.empty:
            return None
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
        else:
            df.index = df.index.tz_convert(IST)
        today = df[df.index.date == NOW.date()]
        return today if not today.empty else None
    except Exception:
        return None


def diagnose_exhaustion_short(symbol, daily, bars5):
    """
    Runs each Exhaustion Short gate in sequence.
    Returns (blocking_gate, actual_value, threshold) for the FIRST gate that fails.
    Returns ("passed", None, None) if all gates pass.
    """
    if daily is None or len(daily) < 6:
        return "no_daily_data", None, None

    prev_close      = float(daily["Close"].iloc[-2])
    prev_prev_close = float(daily["Close"].iloc[-3])
    prev_vol        = float(daily["Volume"].iloc[-2])

    if prev_close <= 0 or prev_prev_close <= 0 or prev_vol <= 0:
        return "no_daily_data", None, None

    # Gate 1: rally (single day OR 3-day cumulative)
    prev_day_rally = (prev_close - prev_prev_close) / prev_prev_close * 100
    close_3d_ago   = float(daily["Close"].iloc[-5])
    cumul_rally    = (prev_close - close_3d_ago) / close_3d_ago * 100 if close_3d_ago > 0 else 0.0
    if prev_day_rally < MISS_RALLY_1D_PCT and cumul_rally < MISS_CUMUL_PCT:
        return "prev_day_rally", round(max(prev_day_rally, cumul_rally / 3), 2), MISS_RALLY_1D_PCT

    # Gate 2: 5-min candle checks
    if bars5 is None or bars5.empty:
        return "no_intraday_data", None, None

    # Find best green candle
    best_gate = None
    best_vals = None
    passed_candle = False

    recent_daily   = daily.iloc[-6:-1]
    avg_daily_to   = float(
        (recent_daily["Close"].astype(float) * recent_daily["Volume"].astype(float)).mean()
    )
    turnover_threshold = avg_daily_to * MISS_TURNOVER_PCT / 100

    for _, row in bars5.iterrows():
        o, c, v = float(row["Open"]), float(row["Close"]), float(row["Volume"])
        if o <= 0 or v <= 0:
            continue
        if c <= o:
            continue  # not green
        body_pct = (c - o) / o * 100
        turnover = c * v
        if body_pct < MISS_IMPULSE_MOVE:
            if best_gate is None:
                best_gate = "impulse_move"
                best_vals = (round(body_pct, 2), MISS_IMPULSE_MOVE)
            continue
        if turnover < turnover_threshold:
            if best_gate in (None, "impulse_move"):
                best_gate = "impulse_turnover"
                best_vals = (round(turnover / 1e7, 1), round(turnover_threshold / 1e7, 1))
            continue
        passed_candle = True
        break

    if not passed_candle:
        if best_gate:
            return best_gate, best_vals[0], best_vals[1]
        return "no_green_candle", None, None

    # Gate 3: paced volume ratio
    today_bars_all = bars5  # approximate with 5-min data
    day_vol     = float(bars5["Volume"].sum())
    n_bars      = len(bars5)
    elapsed_min = max(5.0, n_bars * 5.0)
    paced_vol   = (day_vol / elapsed_min) * 375.0
    vol_ratio   = paced_vol / prev_vol if prev_vol > 0 else 0
    if vol_ratio < MISS_VOL_RATIO:
        return "day_volume", round(vol_ratio, 2), MISS_VOL_RATIO

    return "passed", None, None


def main():
    print(f"\n{'='*60}")
    print(f"Agent 2 — Missed Signals | {NOW.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"{'='*60}")

    if NOW.weekday() >= 5:
        print("[Guard] Weekend — skipping.")
        return

    caught_today  = fetch_today_signals()
    universe      = fetch_nifty500()
    existing      = load_existing()
    already_today = {r["symbol"] for r in existing if r["date"] == TODAY_STR}

    print(f"Signals caught today: {len(caught_today)}")
    print(f"Universe size: {len(universe)}")

    new_records = []

    for symbol in universe:
        if symbol in caught_today:
            continue  # already caught — not a miss
        if symbol in already_today:
            continue  # already logged this miss today

        bars5 = get_intraday5(symbol)
        if bars5 is None or bars5.empty:
            continue

        day_high  = float(bars5["High"].max())
        day_close = float(bars5["Close"].iloc[-1])
        if day_high <= 0:
            continue

        drop_pct = (day_high - day_close) / day_high * 100
        if drop_pct < MISS_THRESHOLD_PCT:
            continue  # didn't fall enough to be a "missed short"

        print(f"  [Missed?] {symbol}: fell {drop_pct:.1f}% from intraday high — diagnosing...")
        daily = get_daily(symbol)
        gate, actual, threshold = diagnose_exhaustion_short(symbol, daily, bars5)

        record = {
            "date":           TODAY_STR,
            "symbol":         symbol,
            "actual_fall_pct": round(drop_pct, 2),
            "day_high":       round(day_high, 2),
            "day_close":      round(day_close, 2),
            "blocking_gate":  gate,
            "actual_value":   actual,
            "threshold":      threshold,
        }
        new_records.append(record)
        print(f"           → blocked by: {gate} (actual={actual}, threshold={threshold})")

    if new_records:
        existing.extend(new_records)
        # Keep last 60 days only
        cutoff = pd.Timestamp.now(tz=IST) - pd.Timedelta(days=60)
        existing = [
            r for r in existing
            if pd.Timestamp(r["date"]).tz_localize(IST) >= cutoff
        ]
        DATA_FILE.write_text(json.dumps(existing, indent=2))
        print(f"\n[Done] {len(new_records)} new missed signals logged → {DATA_FILE}")
    else:
        print(f"\n[Done] No new missed signals for {TODAY_STR}")


if __name__ == "__main__":
    main()
