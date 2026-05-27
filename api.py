from flask import Flask, jsonify, redirect, request as flask_req
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from datetime import datetime
import pytz
import time
import math
import threading
import os
import io
import gzip
import requests as _http

app = Flask(__name__)
CORS(app)

# NSE F&O eligible stocks — verified symbols on Yahoo Finance
FNO_STOCKS = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "AXISBANK.NS", "BAJFINANCE.NS", "KOTAKBANK.NS", "LT.NS",
    "WIPRO.NS", "HCLTECH.NS", "SUNPHARMA.NS", "TATASTEEL.NS", "NTPC.NS",
    "POWERGRID.NS", "COALINDIA.NS", "ONGC.NS", "IOC.NS", "BPCL.NS",
    "HINDUNILVR.NS", "ITC.NS", "MARUTI.NS", "M&M.NS", "ASIANPAINT.NS",
    "NESTLEIND.NS", "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "EICHERMOT.NS",
    "ULTRACEMCO.NS", "GRASIM.NS", "SHREECEM.NS", "ADANIENT.NS", "ADANIPORTS.NS",
    "ADANIGREEN.NS", "ADANIPOWER.NS", "JSWSTEEL.NS", "HINDALCO.NS", "VEDL.NS",
    "INDUSINDBK.NS", "FEDERALBNK.NS", "BANDHANBNK.NS", "IDFCFIRSTB.NS", "PNB.NS",
    "BANKBARODA.NS", "CANBK.NS", "UNIONBANK.NS", "SBICARD.NS", "BAJAJFINSV.NS",
    "MUTHOOTFIN.NS", "CHOLAFIN.NS", "MANAPPURAM.NS", "RECLTD.NS", "PFC.NS",
    "APOLLOHOSP.NS", "DRREDDY.NS", "CIPLA.NS", "DIVISLAB.NS", "BIOCON.NS",
    "LUPIN.NS", "AUROPHARMA.NS", "ALKEM.NS", "ABBOTINDIA.NS", "TORNTPHARM.NS",
    "TECHM.NS", "MPHASIS.NS", "COFORGE.NS", "PERSISTENT.NS",
    "PAYTM.NS", "DELHIVERY.NS", "POLICYBZR.NS",
    "IRCTC.NS", "IRFC.NS", "HAL.NS", "BEL.NS", "BHEL.NS",
    "SAIL.NS", "NMDC.NS", "HUDCO.NS", "RVNL.NS",
    "TITAN.NS", "TRENT.NS", "JUBLFOOD.NS", "DEVYANI.NS", "WESTLIFE.NS",
    "VOLTAS.NS", "HAVELLS.NS", "CUMMINSIND.NS", "SIEMENS.NS", "ABB.NS",
    "PIDILITIND.NS", "BERGEPAINT.NS", "KANSAINER.NS", "INDIGO.NS",
    "UNITDSPR.NS", "UBL.NS", "GODREJCP.NS", "DABUR.NS",
    "MARICO.NS", "COLPAL.NS", "EMAMILTD.NS", "PATANJALI.NS", "VBL.NS",
    "PETRONET.NS", "GAIL.NS", "MGL.NS", "IGL.NS", "CONCOR.NS",
    "KPITTECH.NS", "LTTS.NS", "CYIENT.NS", "OFSS.NS",
    "SOLARINDS.NS", "POLYCAB.NS", "KEI.NS", "APLAPOLLO.NS",
    "LODHA.NS", "DLF.NS", "GODREJPROP.NS", "PRESTIGE.NS", "OBEROIRLTY.NS",
    "UCOBANK.NS", "IOB.NS", "CENTRALBK.NS", "MAHABANK.NS",
    "AUBANK.NS", "IDBI.NS", "KFINTECH.NS", "CDSL.NS",
]

# ── In-memory cache ────────────────────────────────────────────────
_cache: dict = {}
_batch_lock  = threading.Lock()
CACHE_TTL    = 300   # 5 min for Yahoo batch + daily data
LIVE_TTL     = 60    # 1 min for Upstox live quotes


def _cached(key, fn, *args, ttl=CACHE_TTL):
    now = time.time()
    if key in _cache:
        data, ts = _cache[key]
        if now - ts < ttl:
            return data
    result = fn(*args)
    _cache[key] = (result, now)
    return result


# ── Upstox OAuth + live data ───────────────────────────────────────
UPSTOX_API_KEY    = os.environ.get("UPSTOX_API_KEY", "")
UPSTOX_API_SECRET = os.environ.get("UPSTOX_API_SECRET", "")
UPSTOX_REDIRECT   = "https://samvex-api.onrender.com/oauth/callback"
UPSTOX_BASE       = "https://api.upstox.com/v2"
FRONTEND_URL      = os.environ.get("FRONTEND_URL", "")

_upstox_token   = {"access_token": None, "expires_at": 0.0}
_instrument_map = {}   # "RELIANCE" → "NSE_EQ|INE002A01018"
SET_TOKEN_SECRET = os.environ.get("SET_TOKEN_SECRET", "")

TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")

# ── Upstash Redis — persistent token store across deploys/restarts ──
UPSTASH_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
_REDIS_KEY    = "samvex_upstox_token"


def _upstash(cmd: list):
    """Fire a single Redis command via Upstash REST API; returns result or None."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    try:
        r = _http.post(
            UPSTASH_URL,
            json=cmd,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            timeout=5,
        )
        return r.json().get("result")
    except Exception as e:
        print(f"[Redis] Error: {e}")
        return None


def _save_token_to_redis(token: str, expires_at: float):
    ttl = max(60, int(expires_at - time.time()))
    result = _upstash(["SETEX", _REDIS_KEY, str(ttl), token])
    print(f"[Redis] Saved token (TTL {ttl}s): {result}")


def _load_token_from_redis():
    token = _upstash(["GET", _REDIS_KEY])
    if not token:
        return None, 0.0
    ttl = _upstash(["TTL", _REDIS_KEY]) or 3600
    expires_at = time.time() + int(ttl)
    print(f"[Redis] Loaded token (TTL {ttl}s remaining)")
    return token, expires_at


def _save_token_to_disk(token: str, expires_at: float):
    try:
        import json
        with open(TOKEN_FILE, "w") as f:
            json.dump({"access_token": token, "expires_at": expires_at}, f)
    except Exception as e:
        print(f"[Token] Disk save error: {e}")


def _load_token_from_disk():
    try:
        import json
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE) as f:
                data = json.load(f)
            if float(data.get("expires_at", 0)) > time.time():
                return data.get("access_token"), float(data["expires_at"])
    except Exception as e:
        print(f"[Token] Disk load error: {e}")
    return None, 0.0


def _save_token(token: str, expires_at: float):
    """Persist token to all available stores."""
    _save_token_to_redis(token, expires_at)
    _save_token_to_disk(token, expires_at)


def _load_token_on_startup():
    """Priority: Redis (survives deploys) → disk → env var."""
    t, exp = _load_token_from_redis()
    if t:
        return t, exp, "redis"
    t, exp = _load_token_from_disk()
    if t:
        return t, exp, "disk"
    env = os.environ.get("UPSTOX_ACCESS_TOKEN", "")
    if env:
        return env, time.time() + 23 * 3600, "env"
    return None, 0.0, None


# ── Startup token load ─────────────────────────────────────────────
_startup_token, _startup_expires, _startup_source = _load_token_on_startup()
if _startup_token:
    _upstox_token["access_token"] = _startup_token
    _upstox_token["expires_at"]   = _startup_expires
    print(f"[Token] Loaded from {_startup_source}")
    # lambdas defer name lookup to thread-start time (after module fully loads)
    threading.Thread(target=lambda: _load_instrument_map(), daemon=True).start()
    threading.Thread(target=lambda: _load_futures_map(), daemon=True).start()


def _is_live():
    return bool(_upstox_token["access_token"] and time.time() < _upstox_token["expires_at"])


def _upstox_headers():
    return {
        "Authorization": f"Bearer {_upstox_token['access_token']}",
        "Accept": "application/json",
    }


def _load_instrument_map():
    """Download Upstox NSE instruments CSV and build symbol → key mapping."""
    global _instrument_map
    if _instrument_map:
        return _instrument_map
    try:
        r = _http.get(
            "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz",
            timeout=30,
        )
        with gzip.open(io.BytesIO(r.content), "rt") as f:
            df = pd.read_csv(f)

        cols    = df.columns.tolist()
        sym_col = next((c for c in ["tradingsymbol", "trading_symbol"] if c in cols), None)
        key_col = "instrument_key" if "instrument_key" in cols else None
        ser_col = next((c for c in ["series", "instrument_type"] if c in cols), None)

        if not sym_col or not key_col:
            print(f"[Upstox] Unexpected CSV columns: {cols[:15]}")
            return _instrument_map

        eq = df[df[ser_col] == "EQ"] if ser_col else df
        _instrument_map = dict(zip(eq[sym_col].astype(str), eq[key_col].astype(str)))
        print(f"[Upstox] Loaded {len(_instrument_map)} NSE EQ instrument keys")
    except Exception as e:
        print(f"[Upstox] Instrument map error: {e}")
    return _instrument_map


def _sym_to_key(symbol):
    """Yahoo .NS symbol → Upstox instrument_key (None if not found)."""
    imap = _load_instrument_map()
    return imap.get(symbol.replace(".NS", ""))


# ── Futures instrument map for OI tracking ────────────────────────
_futures_map: dict = {}   # "RELIANCE" → {"instrument_key": "NSE_FO|...", "lot_size": 250}

OI_TTL        = 120   # 2-min cache for OI data
PRICE_CHG_MIN = 0.3   # minimum price change % to classify activity
OI_CHG_MIN    = 2.0   # minimum OI change % to classify activity


def _load_futures_map():
    """Download Upstox NSE_FO instruments CSV; build symbol → near-month futures key."""
    global _futures_map
    if _futures_map:
        return _futures_map
    try:
        r = _http.get(
            "https://assets.upstox.com/market-quote/instruments/exchange/NSE_FO.csv.gz",
            timeout=30,
        )
        with gzip.open(io.BytesIO(r.content), "rt") as f:
            df = pd.read_csv(f)

        cols = df.columns.tolist()
        print(f"[FO] CSV columns: {cols[:20]}")

        # Filter stock futures only
        type_col = next((c for c in ["instrument_type", "instrumentType"] if c in cols), None)
        if type_col:
            df = df[df[type_col] == "FUTSTK"].copy()
        else:
            df = df.copy()

        # Find near-month expiry (earliest expiry >= today)
        exp_col = next((c for c in ["expiry", "expiry_date"] if c in cols), None)
        if not exp_col:
            print("[FO] No expiry column found")
            return _futures_map

        df["_exp"]   = pd.to_datetime(df[exp_col], errors="coerce")
        today        = pd.Timestamp.now()
        valid        = df[df["_exp"] >= today]
        if valid.empty:
            return _futures_map
        near_expiry  = valid["_exp"].min()
        near_df      = df[df["_exp"] == near_expiry].copy()

        key_col = "instrument_key" if "instrument_key" in cols else None
        sym_col = next((c for c in ["tradingsymbol", "trading_symbol"] if c in cols), None)
        lot_col = next((c for c in ["lot_size", "lotSize"] if c in cols), None)
        nam_col = "name" if "name" in cols else None

        if not key_col:
            print("[FO] No instrument_key column found")
            return _futures_map

        fno_base = {s.replace(".NS", "").upper() for s in FNO_STOCKS}

        for _, row in near_df.iterrows():
            ikey = str(row[key_col])
            lot  = int(row[lot_col]) if lot_col and pd.notna(row.get(lot_col)) else 1

            # Strategy 1: name column (cleanest — typically the base symbol)
            if nam_col and pd.notna(row.get(nam_col)):
                candidate = str(row[nam_col]).upper().strip()
                if candidate in fno_base and candidate not in _futures_map:
                    _futures_map[candidate] = {"instrument_key": ikey, "lot_size": lot}
                    continue

            # Strategy 2: tradingsymbol prefix match
            if sym_col and pd.notna(row.get(sym_col)):
                ts = str(row[sym_col]).upper().strip()
                for base in fno_base:
                    norm = base.replace("&", "").replace("-", "")
                    if (ts.startswith(base) or ts.startswith(norm)) and base not in _futures_map:
                        _futures_map[base] = {"instrument_key": ikey, "lot_size": lot}
                        break

        print(f"[FO] Loaded {len(_futures_map)} FUTSTK near-month keys (expiry: {near_expiry.date()})")
    except Exception as e:
        print(f"[FO] Futures map error: {e}")
    return _futures_map


def _fetch_futures_quotes():
    """Batch fetch near-month futures quotes for all mapped F&O stocks."""
    fmap = _load_futures_map()
    if not fmap:
        return None

    all_keys   = [info["instrument_key"] for info in fmap.values()]
    key_to_sym = {info["instrument_key"]: sym for sym, info in fmap.items()}

    results = {}
    for i in range(0, len(all_keys), 100):
        chunk = all_keys[i:i + 100]
        try:
            r = _http.get(
                f"{UPSTOX_BASE}/market-quote/quotes",
                params={"instrument_key": ",".join(chunk)},
                headers=_upstox_headers(),
                timeout=20,
            )
            if r.status_code == 200:
                for k, v in r.json().get("data", {}).items():
                    sym = key_to_sym.get(k)
                    if sym:
                        results[sym] = v
            else:
                print(f"[OI] Futures quotes HTTP {r.status_code}")
        except Exception as e:
            print(f"[OI] Futures quotes error chunk {i}: {e}")

    return results or None


def _compute_oi_signals():
    """Classify all F&O stocks into OI activity buckets using near-month futures."""
    fmap   = _load_futures_map()
    quotes = _fetch_futures_quotes()
    out    = {"long_buildup": [], "short_buildup": [], "short_covering": [], "long_unwinding": []}

    if not quotes:
        return out

    for sym, q in quotes.items():
        try:
            last_price  = float(q.get("last_price") or 0)
            prev_close  = float(q.get("prev_close_price") or 0)
            oi          = float(q.get("oi") or 0)
            oi_day_high = float(q.get("oi_day_high") or 0)
            oi_day_low  = float(q.get("oi_day_low") or 0)
            volume      = int(q.get("volume") or 0)

            if last_price <= 0 or prev_close <= 0 or oi <= 0:
                continue

            price_chg = (last_price - prev_close) / prev_close * 100
            # Buildup: OI rising from day low; Unwinding: OI falling from day high
            oi_buildup = (oi - oi_day_low)  / oi_day_low  * 100 if oi_day_low  > 0 else 0
            oi_unwind  = (oi - oi_day_high) / oi_day_high * 100 if oi_day_high > 0 else 0

            entry = {
                "symbol":        sym,
                "last_price":    round(last_price, 2),
                "price_chg_pct": round(price_chg, 2),
                "volume":        volume,
                "lot_size":      fmap.get(sym, {}).get("lot_size", 1),
            }

            if price_chg >= PRICE_CHG_MIN and oi_buildup >= OI_CHG_MIN:
                entry.update({"oi_chg_pct": round(oi_buildup, 2), "activity": "long_buildup"})
                out["long_buildup"].append(entry)
            elif price_chg <= -PRICE_CHG_MIN and oi_buildup >= OI_CHG_MIN:
                entry.update({"oi_chg_pct": round(oi_buildup, 2), "activity": "short_buildup"})
                out["short_buildup"].append(entry)
            elif price_chg >= PRICE_CHG_MIN and oi_unwind <= -OI_CHG_MIN:
                entry.update({"oi_chg_pct": round(oi_unwind, 2), "activity": "short_covering"})
                out["short_covering"].append(entry)
            elif price_chg <= -PRICE_CHG_MIN and oi_unwind <= -OI_CHG_MIN:
                entry.update({"oi_chg_pct": round(oi_unwind, 2), "activity": "long_unwinding"})
                out["long_unwinding"].append(entry)

        except Exception:
            continue

    for key in out:
        out[key].sort(key=lambda x: abs(x["oi_chg_pct"]), reverse=True)

    return out


def _fetch_live_quotes():
    """Single batch call → live OHLCV for all F&O stocks."""
    imap = _load_instrument_map()
    keys, key_to_sym = [], {}
    for sym in FNO_STOCKS:
        base = sym.replace(".NS", "")
        key  = imap.get(base)
        if key:
            keys.append(key)
            key_to_sym[key] = sym

    results = {}
    for i in range(0, len(keys), 100):
        chunk = keys[i:i + 100]
        try:
            r = _http.get(
                f"{UPSTOX_BASE}/market-quote/quotes",
                params={"instrument_key": ",".join(chunk)},
                headers=_upstox_headers(),
                timeout=20,
            )
            if r.status_code == 200:
                for k, v in r.json().get("data", {}).items():
                    sym = key_to_sym.get(k)
                    if sym:
                        results[sym] = v
            else:
                print(f"[Upstox] Quotes HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"[Upstox] Quotes error chunk {i}: {e}")

    return results or None


def _fetch_first_candle_live(instrument_key):
    """Fetch today's 1-min candles; aggregate first 5 into a synthetic 5-min candle."""
    try:
        r = _http.get(
            f"{UPSTOX_BASE}/historical-candle/intraday/{instrument_key}/1minute",
            headers=_upstox_headers(),
            timeout=10,
        )
        if r.status_code != 200:
            return None
        candles = r.json().get("data", {}).get("candles", [])
        if not candles:
            return None
        # Each candle: [timestamp, open, high, low, close, volume, oi]
        candles.sort(key=lambda x: x[0])
        first5 = candles[:5]
        return {
            "open":  float(first5[0][1]),
            "high":  max(float(c[2]) for c in first5),
            "low":   min(float(c[3]) for c in first5),
            "close": float(first5[-1][4]),
        }
    except Exception:
        return None


# ── Yahoo Finance batch (daily only when Upstox is live) ──────────
def _fetch_daily_only():
    tickers = " ".join(FNO_STOCKS)
    return yf.download(
        tickers=tickers,
        interval="1d", period="15d",
        group_by="ticker", auto_adjust=False,
        threads=True, progress=False,
    )


def _get_daily_batch():
    """Daily Yahoo data for ADR — 5-min cache, single download at a time."""
    now = time.time()
    if "daily_only" in _cache:
        data, ts = _cache["daily_only"]
        if now - ts < CACHE_TTL:
            return data
    with _batch_lock:
        if "daily_only" in _cache:
            data, ts = _cache["daily_only"]
            if now - ts < CACHE_TTL:
                return data
        result = _fetch_daily_only()
        _cache["daily_only"] = (result, time.time())
        return result


# ── Yahoo Finance full batch (intraday + daily, fallback path) ─────
def _fetch_batch():
    tickers = " ".join(FNO_STOCKS)
    intraday = yf.download(
        tickers=tickers,
        interval="5m", period="1d",
        group_by="ticker", auto_adjust=False,
        threads=True, progress=False,
    )
    daily = yf.download(
        tickers=tickers,
        interval="1d", period="15d",
        group_by="ticker", auto_adjust=False,
        threads=True, progress=False,
    )
    return intraday, daily


def _get_batch():
    now = time.time()
    if "batch" in _cache:
        data, ts = _cache["batch"]
        if now - ts < CACHE_TTL:
            return data
    with _batch_lock:
        if "batch" in _cache:
            data, ts = _cache["batch"]
            if now - ts < CACHE_TTL:
                return data
        result = _fetch_batch()
        _cache["batch"] = (result, time.time())
        return result


def _get_ticker_df(batch, ticker):
    try:
        if isinstance(batch.columns, pd.MultiIndex):
            df = batch[ticker].dropna(how="all")
        else:
            df = batch.dropna(how="all")
        return df if len(df) >= 2 else None
    except Exception:
        return None


# ── Intraday move potential (time-agnostic) ────────────────────────
def compute_move_potential(intraday, daily, direction, paced_vol_ratio):
    today_open    = float(intraday["Open"].iloc[0])
    prev_close    = float(daily["Close"].iloc[-2])
    current_price = float(intraday["Close"].iloc[-1])
    first_close   = float(intraday["Close"].iloc[0])

    gap_pct = abs((today_open - prev_close) / prev_close * 100)

    lookback = daily.iloc[-11:-1] if len(daily) >= 11 else daily.iloc[:-1]
    adr_pct  = float(
        ((lookback["High"] - lookback["Low"]) / lookback["Close"]).mean() * 100
    ) if len(lookback) > 0 else 2.0

    if direction == "bullish":
        aligned       = first_close > today_open
        session_high  = float(intraday["High"].max())
        used_intraday = max((session_high - today_open) / today_open * 100, 0)
    else:
        aligned       = first_close < today_open
        session_low   = float(intraday["Low"].min())
        used_intraday = max((today_open - session_low) / today_open * 100, 0)

    total_used   = gap_pct + used_intraday
    remaining    = max(adr_pct - total_used, 0.0)
    vol_boost    = 1.0 + min(max(paced_vol_ratio - 2, 0) * 0.10, 0.40)
    align_factor = 1.15 if aligned else 0.80
    potential    = remaining * vol_boost * align_factor

    if adr_pct < 2.0:
        potential *= (adr_pct / 2.0)

    confidence = "HIGH" if potential >= 3.0 else "MED" if potential >= 2.0 else "LOW"
    return {
        "projected_pct": round(potential, 2),
        "adr_pct":       round(adr_pct, 2),
        "aligned":       aligned,
        "confidence":    confidence,
    }


# ── Live analysis (Upstox quotes + Yahoo daily ADR) ────────────────
def _analyze_live(symbol, quote, daily_batch, direction, elapsed_min):
    daily = _get_ticker_df(daily_batch, symbol)
    if daily is None or len(daily) < 2:
        return None
    try:
        ohlc          = quote.get("ohlc", {})
        today_open    = float(ohlc.get("open", 0))
        today_high    = float(ohlc.get("high", 0))
        today_low     = float(ohlc.get("low", 0))
        current_price = float(quote.get("last_price", 0))
        today_volume  = float(quote.get("volume", 0))
        prev_close    = float(quote.get("prev_close_price") or daily["Close"].iloc[-2])
        prev_day_vol  = float(daily["Volume"].iloc[-2])

        if today_open <= 0 or current_price <= 0:
            return None

        gap_pct         = (today_open - prev_close) / prev_close * 100
        traded_value_cr = (today_volume * current_price) / 1e7

        MARKET_MINS     = 375.0
        volume_paced    = (today_volume / elapsed_min) * MARKET_MINS
        paced_vol_ratio = volume_paced / prev_day_vol if prev_day_vol > 0 else 0

        gap_ok = (0 < gap_pct < 1) if direction == "bullish" else (-1 < gap_pct < 0)
        if not (gap_ok and paced_vol_ratio >= 2 and traded_value_cr >= 100):
            return None

        # Only fetch 1-min candles for stocks passing the quick filter
        ikey              = _sym_to_key(symbol)
        first_close       = today_open
        first_candle_move = 0.0
        if ikey:
            fc = _fetch_first_candle_live(ikey)
            if fc:
                first_candle_move = abs((fc["close"] - fc["open"]) / fc["open"] * 100)
                first_close       = fc["close"]

        if first_candle_move >= 1.0:
            return None

        # Synthetic intraday DataFrame for compute_move_potential
        intraday = pd.DataFrame({
            "Open":   [today_open,  today_open],
            "High":   [today_high,  today_high],
            "Low":    [today_low,   today_low],
            "Close":  [first_close, current_price],
            "Volume": [today_volume, today_volume],
        })

        pot = compute_move_potential(intraday, daily, direction, paced_vol_ratio)
        if pot["projected_pct"] < 2.0:
            return None

        return {
            "symbol":              symbol.replace(".NS", ""),
            "price":               round(current_price, 2),
            "gap_pct":             round(gap_pct, 2),
            "first_candle_move":   round(first_candle_move, 2),
            "volume_ratio":        round(paced_vol_ratio, 2),
            "traded_value_crores": round(traded_value_cr, 2),
            "projected_pct":       pot["projected_pct"],
            "adr_pct":             pot["adr_pct"],
            "confidence":          pot["confidence"],
            "aligned":             pot["aligned"],
        }
    except Exception as e:
        print(f"[Live] Analyze error {symbol}: {e}")
        return None


# ── Yahoo Finance analysis (delayed fallback) ──────────────────────
def _analyze(symbol, intraday_batch, daily_batch, direction, elapsed_min):
    intraday = _get_ticker_df(intraday_batch, symbol)
    daily    = _get_ticker_df(daily_batch,    symbol)
    if intraday is None or daily is None or len(daily) < 2:
        return None
    try:
        prev_close      = float(daily["Close"].iloc[-2])
        prev_day_volume = float(daily["Volume"].iloc[-2])
        today_open      = float(intraday["Open"].iloc[0])
        first_close     = float(intraday["Close"].iloc[0])
        current_price   = float(intraday["Close"].iloc[-1])
        today_volume    = float(intraday["Volume"].sum())

        gap_pct           = (today_open - prev_close) / prev_close * 100
        first_candle_move = abs((first_close - today_open) / today_open * 100)
        traded_value_cr   = (today_volume * current_price) / 1e7

        MARKET_MINS     = 375.0
        volume_paced    = (today_volume / elapsed_min) * MARKET_MINS
        paced_vol_ratio = volume_paced / prev_day_volume if prev_day_volume > 0 else 0

        gap_ok = (0 < gap_pct < 1) if direction == "bullish" else (-1 < gap_pct < 0)
        if not (gap_ok
                and paced_vol_ratio >= 2
                and first_candle_move < 1
                and traded_value_cr >= 100):
            return None

        pot = compute_move_potential(intraday, daily, direction, paced_vol_ratio)
        if pot["projected_pct"] < 2.0:
            return None

        return {
            "symbol":              symbol.replace(".NS", ""),
            "price":               round(current_price, 2),
            "gap_pct":             round(gap_pct, 2),
            "first_candle_move":   round(first_candle_move, 2),
            "volume_ratio":        round(paced_vol_ratio, 2),
            "traded_value_crores": round(traded_value_cr, 2),
            "projected_pct":       pot["projected_pct"],
            "adr_pct":             pot["adr_pct"],
            "confidence":          pot["confidence"],
            "aligned":             pot["aligned"],
        }
    except Exception:
        return None


# ── Screener ───────────────────────────────────────────────────────
def _screen(direction):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    open_dt     = now.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed_min = max(5.0, (now - open_dt).total_seconds() / 60)
    results     = []

    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)
        daily_batch = _get_daily_batch()
        if live_quotes and daily_batch is not None:
            for symbol in FNO_STOCKS:
                q = live_quotes.get(symbol)
                if q:
                    r = _analyze_live(symbol, q, daily_batch, direction, elapsed_min)
                    if r:
                        results.append(r)

    if not results:
        # Fallback: Yahoo Finance delayed data
        intraday_batch, daily_batch = _get_batch()
        for symbol in FNO_STOCKS:
            r = _analyze(symbol, intraday_batch, daily_batch, direction, elapsed_min)
            if r:
                results.append(r)

    results.sort(key=lambda x: x["projected_pct"], reverse=True)
    return results[:5]


# ── Market index data ──────────────────────────────────────────────
def _fetch_market():
    # When live, use Upstox for real-time index prices
    if _is_live():
        try:
            r = _http.get(
                f"{UPSTOX_BASE}/market-quote/quotes",
                params={"instrument_key": "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank"},
                headers=_upstox_headers(),
                timeout=10,
            )
            if r.status_code == 200:
                result = {}
                for k, v in r.json().get("data", {}).items():
                    lp = float(v.get("last_price", 0) or 0)
                    pc = float(v.get("prev_close_price", 0) or (v.get("ohlc") or {}).get("close", 0) or 0)
                    if lp <= 0:
                        continue
                    chg = round((lp - pc) / pc * 100, 2) if pc > 0 else 0
                    if "Nifty 50" in k:
                        result["NIFTY 50"]   = {"price": round(lp, 2), "change_pct": chg}
                    elif "Nifty Bank" in k:
                        result["BANK NIFTY"] = {"price": round(lp, 2), "change_pct": chg}
                if result:
                    print(f"[Market] Upstox indices: {list(result.keys())}")
                    return result
        except Exception as e:
            print(f"[Market] Upstox index error: {e}")

    # Fallback: Yahoo Finance delayed data
    indices = {"NIFTY 50": "^NSEI", "BANK NIFTY": "^NSEBANK"}
    result  = {}
    for name, sym in indices.items():
        try:
            d = yf.Ticker(sym).history(interval="1d", period="2d")
            if len(d) >= 2:
                prev, curr = d["Close"].iloc[-2], d["Close"].iloc[-1]
                result[name] = {
                    "price":      round(float(curr), 2),
                    "change_pct": round((curr - prev) / prev * 100, 2),
                }
        except Exception:
            pass
    return result


# ── Ticker prices ─────────────────────────────────────────────────
TICKER_SYMBOLS = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "AXISBANK.NS", "BAJFINANCE.NS", "KOTAKBANK.NS", "LT.NS",
    "WIPRO.NS", "HCLTECH.NS", "SUNPHARMA.NS", "TATASTEEL.NS",
    "NTPC.NS", "POWERGRID.NS", "COALINDIA.NS", "ADANIENT.NS",
]


def _fetch_ticker():
    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)
        if live_quotes:
            result = []
            for sym in TICKER_SYMBOLS:
                q = live_quotes.get(sym)
                if q:
                    price = round(float(q.get("last_price", 0)), 2)
                    prev  = float(q.get("prev_close_price") or price)
                    chg   = round((price - prev) / prev * 100, 2) if prev > 0 else 0
                    result.append({"symbol": sym.replace(".NS", ""), "price": price, "change_pct": chg})
            if result:
                return result

    intraday_batch, daily_batch = _get_batch()
    result = []
    for sym in TICKER_SYMBOLS:
        try:
            intra = _get_ticker_df(intraday_batch, sym)
            daily = _get_ticker_df(daily_batch, sym)
            if intra is None or daily is None or len(daily) < 2:
                continue
            price      = round(float(intra["Close"].iloc[-1]), 2)
            prev_close = float(daily["Close"].iloc[-2])
            change_pct = round((price - prev_close) / prev_close * 100, 2)
            result.append({"symbol": sym.replace(".NS", ""), "price": price, "change_pct": change_pct})
        except Exception:
            pass
    return result


# ── Routes ─────────────────────────────────────────────────────────

@app.route("/auth/login")
def auth_login():
    if not UPSTOX_API_KEY:
        return jsonify({"error": "UPSTOX_API_KEY not set on server"}), 500
    url = (
        f"https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code"
        f"&client_id={UPSTOX_API_KEY}"
        f"&redirect_uri={UPSTOX_REDIRECT}"
    )
    return redirect(url)


@app.route("/oauth/callback")
def oauth_callback():
    code = flask_req.args.get("code")
    if not code:
        return jsonify({"error": "No auth code in callback"}), 400

    resp = _http.post(
        f"{UPSTOX_BASE}/login/authorization/token",
        data={
            "code":          code,
            "client_id":     UPSTOX_API_KEY,
            "client_secret": UPSTOX_API_SECRET,
            "redirect_uri":  UPSTOX_REDIRECT,
            "grant_type":    "authorization_code",
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        timeout=15,
    )
    if resp.status_code != 200:
        return jsonify({"error": "Token exchange failed", "detail": resp.text}), 400

    token_data = resp.json()
    _upstox_token["access_token"] = token_data.get("access_token")
    _upstox_token["expires_at"]   = time.time() + 23 * 3600  # valid ~23 hrs
    _save_token(_upstox_token["access_token"], _upstox_token["expires_at"])

    # Warm up instrument maps in background
    threading.Thread(target=_load_instrument_map, daemon=True).start()
    threading.Thread(target=_load_futures_map, daemon=True).start()

    # Clear screener cache so next load uses live data immediately
    for k in ("bullish", "bearish", "live_quotes", "ticker", "oi_buildup", "market"):
        _cache.pop(k, None)

    redirect_url = FRONTEND_URL if FRONTEND_URL else None
    redirect_script = (
        f'<p id="msg" style="color:#8892a4;font-size:13px;">Redirecting to dashboard…</p>'
        f'<script>setTimeout(()=>{{window.location.href="{redirect_url}";}},1800);</script>'
    ) if redirect_url else '<p style="margin-top:32px;font-size:13px;color:#8892a4;">You can close this tab and return to the dashboard.</p>'

    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>Samvex — Authenticated</title>
    <style>
      body {{ font-family: sans-serif; text-align: center; padding: 80px;
             background: #0f1117; color: #e2e8f0; }}
      h2   {{ color: #22c55e; font-size: 28px; margin-bottom: 16px; }}
      p    {{ color: #8892a4; font-size: 15px; }}
    </style></head>
    <body>
      <h2>&#10003; Live Data Active</h2>
      <p>Upstox authenticated successfully.</p>
      <p>The Samvex dashboard is now receiving real-time market data.</p>
      {redirect_script}
    </body>
    </html>
    """


@app.route("/auth/set-token")
def set_token():
    """Daily token refresh: /auth/set-token?secret=XXX&token=YYY"""
    secret = flask_req.args.get("secret", "")
    token  = flask_req.args.get("token", "")
    if not SET_TOKEN_SECRET or secret != SET_TOKEN_SECRET:
        return jsonify({"error": "Unauthorized"}), 403
    if not token:
        return jsonify({"error": "token param required"}), 400
    _upstox_token["access_token"] = token
    _upstox_token["expires_at"]   = time.time() + 23 * 3600
    _save_token(_upstox_token["access_token"], _upstox_token["expires_at"])
    # Invalidate screener cache so next request uses live data immediately
    for k in ("bullish", "bearish", "live_quotes", "ticker", "oi_buildup", "market"):
        _cache.pop(k, None)
    threading.Thread(target=_load_instrument_map, daemon=True).start()
    threading.Thread(target=_load_futures_map, daemon=True).start()
    return """
    <!DOCTYPE html><html>
    <head><title>Samvex — Token Updated</title>
    <style>body{font-family:sans-serif;text-align:center;padding:80px;
    background:#0f1117;color:#e2e8f0;}h2{color:#22c55e;}p{color:#8892a4;}</style>
    </head><body>
      <h2>&#10003; Live Data Active</h2>
      <p>Token accepted. Dashboard is now on real-time Upstox data.</p>
      <p style="margin-top:24px;font-size:13px;">You can close this tab.</p>
    </body></html>
    """


@app.route("/auth/status")
def auth_status():
    expires_in = max(0, int((_upstox_token["expires_at"] - time.time()) / 60)) if _is_live() else 0
    return jsonify({
        "is_live":        _is_live(),
        "data_source":    "upstox_live" if _is_live() else "yahoo_delayed",
        "expires_in_min": expires_in,
    })


@app.route("/api/bullish")
def bullish():
    return jsonify(_cached("bullish", _screen, "bullish"))

@app.route("/api/bearish")
def bearish():
    return jsonify(_cached("bearish", _screen, "bearish"))

@app.route("/api/market")
def market():
    ttl = LIVE_TTL if _is_live() else CACHE_TTL
    return jsonify(_cached("market", _fetch_market, ttl=ttl))

@app.route("/api/ticker")
def ticker():
    return jsonify(_cached("ticker", _fetch_ticker, ttl=LIVE_TTL))

@app.route("/api/status")
def status():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    is_open = (
        now.replace(hour=9, minute=15, second=0, microsecond=0)
        <= now <=
        now.replace(hour=15, minute=30, second=0, microsecond=0)
        and now.weekday() < 5
    )
    return jsonify({
        "time":        now.strftime("%H:%M:%S IST"),
        "market_open": is_open,
        "date":        now.strftime("%d-%b-%Y"),
        "is_live":     _is_live(),
        "data_source": "upstox_live" if _is_live() else "yahoo_delayed",
    })

@app.route("/api/oi-buildup")
def oi_buildup():
    if not _is_live():
        return jsonify({
            "is_live": False,
            "long_buildup": [], "short_buildup": [],
            "short_covering": [], "long_unwinding": [],
        })
    data = _cached("oi_buildup", _compute_oi_signals, ttl=OI_TTL)
    return jsonify({**data, "is_live": True})


@app.route("/api/debug/screener")
def debug_screener():
    """Diagnose why screener returns 0 results: shows per-stock filter breakdown."""
    ist         = pytz.timezone("Asia/Kolkata")
    now         = datetime.now(ist)
    open_dt     = now.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed_min = max(5.0, (now - open_dt).total_seconds() / 60)

    live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL) if _is_live() else None
    daily_batch = _get_daily_batch()

    out = {
        "time_ist":          now.strftime("%H:%M:%S"),
        "elapsed_min":       round(elapsed_min, 1),
        "is_live":           _is_live(),
        "instrument_map_sz": len(_instrument_map),
        "futures_map_sz":    len(_futures_map),
        "live_quote_count":  len(live_quotes) if live_quotes else 0,
        "daily_batch_shape": str(daily_batch.shape) if daily_batch is not None else "None",
        "daily_batch_empty": (daily_batch is None or daily_batch.empty),
    }

    sample = []
    for sym in FNO_STOCKS[:12]:
        q = live_quotes.get(sym) if live_quotes else None
        d = _get_ticker_df(daily_batch, sym) if daily_batch is not None else None
        info = {"sym": sym, "daily_ok": d is not None and len(d) >= 2}
        if q:
            ohlc   = q.get("ohlc") or {}
            lp     = float(q.get("last_price",      0) or 0)
            op     = float(ohlc.get("open",          0) or 0)
            pc     = float(q.get("prev_close_price", 0) or 0)
            vol    = float(q.get("volume",           0) or 0)
            pv     = float(d["Volume"].iloc[-2]) if d is not None and len(d) >= 2 else 0
            gap    = round((op - pc) / pc * 100, 2) if pc > 0 else None
            paced  = round((vol / elapsed_min) * 375 / pv, 2) if pv > 0 else None
            val_cr = round(vol * lp / 1e7, 1)
            info.update({
                "ltp":    lp,
                "gap%":   gap,
                "vol_x":  paced,
                "val_cr": val_cr,
                "FAIL":   (
                    "no_gap"   if gap is None or not (0 < gap < 1)    else
                    "vol<2x"   if paced is None or paced < 2           else
                    "val<100cr" if val_cr < 100                         else
                    "OK_so_far"
                ),
            })
        else:
            info["FAIL"] = "no_quote"
        sample.append(info)

    out["sample"] = sample
    return jsonify(out)


@app.route("/api/debug/oi")
def debug_oi():
    """Diagnostic: returns raw Upstox futures quote for 3 symbols so we can verify oi/oi_day_high/oi_day_low."""
    if not _is_live():
        return jsonify({"error": "not_live"})
    fmap = _load_futures_map()
    if not fmap:
        return jsonify({"error": "futures_map_empty", "hint": "NSE_FO CSV may not have loaded yet"})
    sample_items = list(fmap.items())[:3]
    keys = [info["instrument_key"] for _, info in sample_items]
    try:
        r = _http.get(
            f"{UPSTOX_BASE}/market-quote/quotes",
            params={"instrument_key": ",".join(keys)},
            headers=_upstox_headers(),
            timeout=15,
        )
        return jsonify({
            "map_size": len(fmap),
            "sample_map": {sym: info for sym, info in sample_items},
            "http_status": r.status_code,
            "raw": r.json() if r.status_code == 200 else r.text[:500],
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/ping")
def ping():
    return jsonify({"status": "ok"})

@app.route("/")
def index():
    return jsonify({"service": "Samvex Trading API", "status": "running"})


if __name__ == "__main__":
    print("Starting Samvex Dashboard API on http://localhost:5050")
    app.run(debug=True, port=5050, use_reloader=False)
