from flask import Flask, jsonify, redirect, request as flask_req, Response
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from datetime import datetime, time as _dtime
import pytz
import time
import math
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import io
import gzip
import requests as _http

app = Flask(__name__)
CORS(app)

@app.after_request
def no_cache(response):
    if flask_req.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response

# NSE F&O eligible stocks — verified symbols on Yahoo Finance
FNO_STOCKS = [
    # Large caps / Index heavyweights
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "AXISBANK.NS", "BAJFINANCE.NS", "KOTAKBANK.NS", "LT.NS",
    "HCLTECH.NS", "TATASTEEL.NS", "NTPC.NS",
    "POWERGRID.NS", "COALINDIA.NS", "ONGC.NS", "IOC.NS", "BPCL.NS",
    "HINDUNILVR.NS", "ITC.NS", "MARUTI.NS", "M&M.NS", "ASIANPAINT.NS",
    "NESTLEIND.NS", "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "EICHERMOT.NS",
    "ULTRACEMCO.NS", "GRASIM.NS", "SHREECEM.NS", "ADANIENT.NS", "ADANIPORTS.NS",
    "ADANIGREEN.NS", "ADANIPOWER.NS", "JSWSTEEL.NS", "HINDALCO.NS", "VEDL.NS",
    "TATAMOTORS.NS", "TATAPOWER.NS", "TATACONSUM.NS", "MCDOWELL-N.NS",
    # Banks & NBFCs
    "INDUSINDBK.NS", "FEDERALBNK.NS", "BANDHANBNK.NS", "IDFCFIRSTB.NS", "PNB.NS",
    "BANKBARODA.NS", "CANBK.NS", "UNIONBANK.NS", "SBICARD.NS", "BAJAJFINSV.NS",
    "MUTHOOTFIN.NS", "CHOLAFIN.NS", "MANAPPURAM.NS", "RECLTD.NS", "PFC.NS",
    "AUBANK.NS", "IDBI.NS", "UCOBANK.NS", "IOB.NS", "CENTRALBK.NS", "MAHABANK.NS",
    "KFINTECH.NS", "CDSL.NS", "BSE.NS", "MCX.NS",
    # Pharma
    "APOLLOHOSP.NS", "DRREDDY.NS", "CIPLA.NS", "DIVISLAB.NS", "BIOCON.NS",
    "LUPIN.NS", "AUROPHARMA.NS", "ALKEM.NS", "ABBOTINDIA.NS", "TORNTPHARM.NS",
    "GLENMARK.NS", "IPCALAB.NS", "NATCOPHARM.NS", "GRANULES.NS",
    "LAURUSLABS.NS", "PFIZER.NS", "SANOFI.NS", "SUNPHARMA.NS",
    # IT / Tech
    "TECHM.NS", "MPHASIS.NS", "COFORGE.NS", "PERSISTENT.NS", "LTTS.NS",
    "KPITTECH.NS", "CYIENT.NS", "OFSS.NS", "WIPRO.NS",
    "TATAELXSI.NS", "LTIM.NS", "ZENSARTECH.NS", "MASTEK.NS",
    # New-age / Fintech
    "PAYTM.NS", "DELHIVERY.NS", "POLICYBZR.NS", "NYKAA.NS", "ZOMATO.NS",
    "NAUKRI.NS", "INDIAMART.NS",
    # Defence / PSU
    "IRCTC.NS", "IRFC.NS", "HAL.NS", "BEL.NS", "BHEL.NS",
    "SAIL.NS", "NMDC.NS", "HUDCO.NS", "RVNL.NS", "COCHINSHIP.NS",
    "MIDHANI.NS", "BEML.NS", "NLCINDIA.NS", "SJVN.NS", "NHPC.NS",
    "HINDPETRO.NS", "MRPL.NS", "OIL.NS", "GUJGASLTD.NS",
    # Consumer / Retail
    "TITAN.NS", "TRENT.NS", "JUBLFOOD.NS", "DEVYANI.NS", "WESTLIFE.NS",
    "UNITDSPR.NS", "UBL.NS", "GODREJCP.NS", "DABUR.NS",
    "MARICO.NS", "COLPAL.NS", "EMAMILTD.NS", "PATANJALI.NS", "VBL.NS",
    "RADICO.NS", "GODFRYPHLP.NS",
    # Industrials / Capital goods
    "VOLTAS.NS", "HAVELLS.NS", "CUMMINSIND.NS", "SIEMENS.NS", "ABB.NS",
    "PIDILITIND.NS", "BERGEPAINT.NS", "KANSAINER.NS", "POLYCAB.NS",
    "KEI.NS", "APLAPOLLO.NS", "SOLARINDS.NS",
    "GRINDWELL.NS", "SCHAEFFLER.NS", "TIMKEN.NS", "SKFINDIA.NS",
    "THERMAX.NS", "KIRLOSKAR.NS", "JYOTHYLAB.NS",
    # Oil & Gas / Utilities
    "PETRONET.NS", "GAIL.NS", "MGL.NS", "IGL.NS", "CONCOR.NS",
    "INDIGO.NS", "SPICEJET.NS",
    # Real estate
    "LODHA.NS", "DLF.NS", "GODREJPROP.NS", "PRESTIGE.NS", "OBEROIRLTY.NS",
    "PHOENIXLTD.NS", "SOBHA.NS",
    # Metals / Mining
    "HINDZINC.NS", "NATIONALUM.NS", "RATNAMANI.NS", "WELSPUNIND.NS",
    "JINDALSTEL.NS", "JSWENERGY.NS", "JSPL.NS",
    # Auto ancillaries
    "MOTHERSON.NS", "BOSCHLTD.NS", "BALKRISIND.NS", "APOLLOTYRE.NS",
    "CEATLTD.NS", "MRF.NS", "EXIDEIND.NS", "AMARAJABAT.NS",
    # Chemicals / Specialty
    "UPL.NS", "AARTIIND.NS", "DEEPAKNITRITE.NS", "NAVINFLUOR.NS",
    "FLUOROCHEM.NS", "ALKYLAMINE.NS", "CLEAN.NS",
    # Cement / Building materials
    "ACC.NS", "AMBUJACEM.NS", "RAMCOCEM.NS", "JKCEMENT.NS", "HEIDELBERG.NS",
    # Insurance
    "SBILIFE.NS", "HDFCLIFE.NS", "ICICIPRULI.NS", "LICI.NS",
]

# ── Live Nifty 500 universe ────────────────────────────────────────
# Tries NSE archives first (no JS challenge), then niftyindices.com with
# Referer header. Falls back to FNO_STOCKS if both fail.
_NIFTY500_SOURCES = [
    (
        "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept":          "text/csv,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    ),
    (
        "https://www.niftyindices.com/IndexConstituents/ind_nifty500list.csv",
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Referer":         "https://www.niftyindices.com/",
            "Accept":          "text/csv,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    ),
]
_nifty500_cache: dict = {"symbols": [], "date": "", "source": ""}


def _load_nifty500() -> list:
    """Return Nifty 500 symbol list (.NS suffix). Cached per trading day.
    Tries NSE archives → niftyindices.com → FNO_STOCKS fallback."""
    ist   = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).strftime("%Y-%m-%d")
    if _nifty500_cache["symbols"] and _nifty500_cache["date"] == today:
        return _nifty500_cache["symbols"]

    for url, headers in _NIFTY500_SOURCES:
        try:
            r = _http.get(url, timeout=15, headers=headers)
            r.raise_for_status()
            df  = pd.read_csv(io.StringIO(r.text))
            col = next((c for c in df.columns if "symbol" in c.lower()), None)
            if not col:
                raise ValueError(f"No symbol column — cols: {list(df.columns)}")
            syms = [
                f"{str(s).strip().upper()}.NS"
                for s in df[col].dropna()
                if str(s).strip() and str(s).strip().upper() not in ("SYMBOL", "NAN", "")
            ]
            if len(syms) < 100:
                raise ValueError(f"Only {len(syms)} symbols parsed")
            _nifty500_cache["symbols"] = syms
            _nifty500_cache["date"]    = today
            _nifty500_cache["source"]  = url
            print(f"[Nifty500] {len(syms)} symbols loaded from {url}")
            return syms
        except Exception as e:
            print(f"[Nifty500] {url} failed: {e}")

    fno = _get_fno_universe()
    print(f"[Nifty500] All sources failed — F&O fallback ({len(fno)} stocks)")
    if not _nifty500_cache["symbols"]:
        _nifty500_cache["symbols"] = fno
        _nifty500_cache["source"]  = "fno_fallback"
    return _nifty500_cache["symbols"]

# ── In-memory cache ────────────────────────────────────────────────
_cache: dict = {}
_batch_lock  = threading.Lock()
CACHE_TTL    = 300   # 5 min — general cache (market data, etc.)
DAILY_TTL    = 600   # 10 min — daily batch (changes slowly; staggered from 15m to avoid simultaneous expiry)
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


# ── Day-signal store ────────────────────────────────────────────────
# Signals are locked at first run after 9:30 AM and served unchanged
# for the rest of the day. Persisted to /tmp so they survive page
# refreshes and process restarts without a new container build.
_signal_store: dict = {}          # key: (setup, direction, "YYYY-MM-DD") → list
_smc_history:  dict = {}          # key: (setup, direction, "YYYY-MM-DD") → {symbol: enriched_signal}
_candle_cache: dict = {}          # key: instrument_key → {"date": "YYYY-MM-DD", "candles": [...]}
_TMP_SIGNALS = "/tmp/samvex_signals"
_data_dir_env = os.environ.get("DATA_DIR", _TMP_SIGNALS)
try:
    os.makedirs(_data_dir_env, exist_ok=True)
    _SIGNALS_DIR = _data_dir_env
    print(f"[Storage] Using data dir: {_SIGNALS_DIR}")
except OSError as _e:
    print(f"[Storage] Cannot use DATA_DIR={_data_dir_env!r}: {_e} — falling back to {_TMP_SIGNALS}")
    _SIGNALS_DIR = _TMP_SIGNALS
    os.makedirs(_SIGNALS_DIR, exist_ok=True)

_PANEL_LABELS = {
    (1, "bullish"): "Setup 1 — Liquidity Sweep → BOS (Bullish · Sweep PDL → Break PDH)",
    (1, "bearish"): "Setup 1 — Liquidity Sweep → BOS (Bearish · Sweep PDH → Break PDL)",
    (2, "bearish"): "Exhaustion Short — Profit Booking After Rally (9:15 AM–2:00 PM)",
}

def _signals_path(date_str: str) -> str:
    return os.path.join(_SIGNALS_DIR, f"signals_{date_str}.json")

def _persist_signals(date_str: str) -> None:
    """Write today's _signal_store to disk (called in a background thread)."""
    try:
        payload = {
            f"{k[0]}|{k[1]}": v
            for k, v in _signal_store.items()
            if k[2] == date_str
        }
        with open(_signals_path(date_str), "w") as fh:
            json.dump({"date": date_str, "panels": payload}, fh)
    except Exception as exc:
        print(f"[Signals] persist error: {exc}")

def _load_persisted_signals() -> None:
    """On server start, reload today's signals from disk into _signal_store."""
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    path = _signals_path(today_str)
    if not os.path.exists(path):
        return
    try:
        with open(path) as fh:
            data = json.load(fh)
        for k_str, signals in data.get("panels", {}).items():
            setup_str, direction = k_str.split("|")
            _signal_store[(int(setup_str), direction, today_str)] = signals
        total = sum(len(v) for v in _signal_store.values())
        print(f"[Signals] Restored {total} signals from {path}")
    except Exception as exc:
        print(f"[Signals] load error: {exc}")


# ── Signal history persistence (detected_at + inactive signals) ───
def _history_path(date_str: str) -> str:
    return os.path.join(_SIGNALS_DIR, f"history_{date_str}.json")

def _persist_history(date_str: str) -> None:
    """Write today's _smc_history to disk. Called in a background thread on every state change."""
    try:
        payload = {}
        for k, v in list(_smc_history.items()):
            if k[2] == date_str:
                payload[f"{k[0]}|{k[1]}"] = v
        with open(_history_path(date_str), "w") as fh:
            json.dump({"date": date_str, "panels": payload}, fh)
    except Exception as exc:
        print(f"[History] persist error: {exc}")

def _load_persisted_history() -> None:
    """On server start, reload today's signal history from disk into _smc_history."""
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    path = _history_path(today_str)
    if not os.path.exists(path):
        return
    try:
        with open(path) as fh:
            data = json.load(fh)
        for k_str, signals in data.get("panels", {}).items():
            setup_str, direction = k_str.split("|")
            _smc_history[(int(setup_str), direction, today_str)] = signals
        total = sum(len(v) for v in _smc_history.values())
        print(f"[History] Restored {total} historical signals from {path}")
    except Exception as exc:
        print(f"[History] load error: {exc}")


def _merge_with_history(active: list, setup: int, direction: str) -> list:
    """Merge live screener results with today's signal history.

    Returns: active signals first (sorted by confidence desc),
             then signals that fired earlier but no longer meet criteria
             (sorted by detected_at desc — most recently dropped first).

    Each enriched signal gains three fields:
      is_active      bool — True if currently meeting all screener criteria
      detected_at    str  — HH:MM IST of the BOS candle (when the move happened)
      first_shown_at str  — HH:MM IST when the dashboard first displayed this signal
                             (can be later than detected_at — the screener needs
                             >=2 completed 15-min bars, i.e. ~9:45 AM IST, before
                             it can compute anything)
    """
    ist       = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    now_hm    = datetime.now(ist).strftime("%H:%M")

    key = (setup, direction, today_str)
    if key not in _smc_history:
        _smc_history[key] = {}

    history     = _smc_history[key]
    active_syms = {r["symbol"] for r in active}
    changed     = False

    # Register / refresh currently active signals
    for r in active:
        sym         = r["symbol"]
        if sym not in history:
            changed = True  # new signal firing for the first time today
        if sym in history:
            detected_at    = history[sym]["detected_at"]
            first_shown_at = history[sym].get("first_shown_at", detected_at)
        else:
            detected_at    = r.get("bos_time") or now_hm
            first_shown_at = now_hm
        history[sym] = {**r, "detected_at": detected_at, "first_shown_at": first_shown_at, "is_active": True}

    # Mark signals that are no longer in the active set as inactive
    for sym in list(history.keys()):
        if sym not in active_syms:
            if history[sym].get("is_active", True):
                changed = True  # signal just dropped off the active list
            history[sym]["is_active"] = False

    # Persist to disk whenever state changes so restarts don't lose history
    if changed:
        threading.Thread(target=_persist_history, args=(today_str,), daemon=True).start()

    active_out   = sorted(
        [s for s in history.values() if s["is_active"]],
        key=lambda x: x["confidence_score"], reverse=True,
    )
    inactive_out = sorted(
        [s for s in history.values() if not s["is_active"]],
        key=lambda x: x["detected_at"], reverse=True,
    )
    return active_out + inactive_out


# ── Upstox OAuth + live data ───────────────────────────────────────
UPSTOX_API_KEY    = os.environ.get("UPSTOX_API_KEY", "")
UPSTOX_API_SECRET = os.environ.get("UPSTOX_API_SECRET", "")
UPSTOX_REDIRECT   = "https://samvex-api.onrender.com/oauth/callback"
UPSTOX_BASE       = "https://api.upstox.com/v2"
FRONTEND_URL      = os.environ.get("FRONTEND_URL", "")

_upstox_token   = {"access_token": None, "expires_at": 0.0}
_instrument_map        = {}     # "RELIANCE" → "NSE_EQ|INE002A01018"
_instrument_map_loaded = False  # True once a download was attempted (prevents re-fetch on empty result)
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


# ── Startup: always pre-warm everything auth-independent ──────────
# Load instrument maps, Nifty 500 list, and daily Yahoo batch in background
# so the first screener request doesn't have to wait 30-60s.
threading.Thread(target=lambda: _load_instrument_map(), daemon=True).start()
threading.Thread(target=lambda: _load_futures_map(),    daemon=True).start()
threading.Thread(target=lambda: _load_nifty500(),       daemon=True).start()
threading.Thread(target=lambda: _get_daily_batch(),     daemon=True).start()
threading.Thread(target=_load_persisted_signals,        daemon=True).start()
threading.Thread(target=_load_persisted_history,        daemon=True).start()

# ── Startup token load ─────────────────────────────────────────────
_startup_token, _startup_expires, _startup_source = _load_token_on_startup()
if _startup_token:
    _upstox_token["access_token"] = _startup_token
    _upstox_token["expires_at"]   = _startup_expires
    print(f"[Token] Loaded from {_startup_source}")
else:
    # Redis can be slow on Render cold start — retry in background with backoff
    def _retry_token_load():
        for attempt in range(4):
            time.sleep(3 * (attempt + 1))   # 3s, 6s, 9s, 12s
            t, exp, src = _load_token_on_startup()
            if t:
                _upstox_token["access_token"] = t
                _upstox_token["expires_at"]   = exp
                for k in ("live_quotes", "bullish", "bearish", "ticker", "market"):
                    _cache.pop(k, None)
                print(f"[Token] Retry {attempt+1}: Loaded from {src}")
                return
        print("[Token] All retry attempts failed — will require manual OAuth")
    threading.Thread(target=_retry_token_load, daemon=True).start()


def _is_live():
    return bool(_upstox_token["access_token"] and time.time() < _upstox_token["expires_at"])


def _upstox_headers():
    return {
        "Authorization": f"Bearer {_upstox_token['access_token']}",
        "Accept": "application/json",
    }


def _load_instrument_map():
    """Download Upstox NSE instruments CSV and build symbol → key mapping."""
    global _instrument_map, _instrument_map_loaded
    if _instrument_map_loaded:
        return _instrument_map
    _instrument_map_loaded = True
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

        if ser_col:
            # Upstox CSV uses "EQ" (older) or "EQUITY" (newer instrument_type column)
            eq = df[df[ser_col].isin(["EQ", "EQUITY"])]
            if eq.empty:
                print(f"[Upstox] EQ/EQUITY filter returned 0 rows. "
                      f"ser_col={ser_col!r}, unique values: {df[ser_col].unique()[:10].tolist()}")
                eq = df  # fallback: include all instruments
        else:
            eq = df

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
PRICE_CHG_MIN = 0.2   # minimum price change % to classify activity
OI_CHG_MIN    = 1.0   # minimum OI change % to classify activity


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


def _get_fno_universe() -> list:
    """Return the live F&O eligible stock list derived from the Upstox NSE_FO
    instruments CSV (exact NSE current list, no manual maintenance needed).
    Falls back to the hardcoded FNO_STOCKS only if the futures map is empty."""
    fmap = _load_futures_map()
    if len(fmap) > 50:
        return [f"{sym}.NS" for sym in fmap]
    return FNO_STOCKS


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
    for sym in _get_fno_universe():
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




# ── Yahoo Finance batch (daily — Nifty 500 universe) ──────────────
def _fetch_chunked(interval: str, period: str) -> dict:
    """Per-ticker yfinance downloads, 10 concurrent workers.

    Uses yf.Ticker(sym).history() instead of yf.download() batch calls.
    This never creates MultiIndex DataFrames, so peak memory is
    10 workers × ~50 KB per stock ≈ 500 KB regardless of universe size.
    Previous approach (yf.download + threads=True) was spawning 100–200
    internal threads whose stack overhead was pushing us past 512 MB.
    """
    universe = _load_nifty500() or _get_fno_universe()

    def _fetch_one(sym):
        try:
            df = yf.Ticker(sym).history(interval=interval, period=period)
            if df is not None and not df.empty and len(df) >= 1:
                return sym, df
        except Exception:
            pass
        return sym, None

    results: dict = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch_one, sym): sym for sym in universe}
        for fut in as_completed(futures):
            try:
                sym, df = fut.result()
                if df is not None:
                    results[sym] = df
            except Exception:
                pass
    print(f"[Batch] {interval}/{period} — {len(results)}/{len(universe)} symbols loaded")
    return results


def _fetch_daily_only() -> dict:
    return _fetch_chunked("1d", "10d")   # 10d sufficient for PDH/PDL (RSI removed)


def _get_daily_batch():
    """Daily Yahoo data for ADR — 10-min cache, single download at a time."""
    now = time.time()
    if "daily_only" in _cache:
        data, ts = _cache["daily_only"]
        if now - ts < DAILY_TTL:
            return data
    with _batch_lock:
        if "daily_only" in _cache:
            data, ts = _cache["daily_only"]
            if now - ts < DAILY_TTL:
                return data
        result = _fetch_daily_only()
        _cache["daily_only"] = (result, time.time())
        return result


# ── Yahoo Finance 15-min intraday batch (for first-candle screener) ─
SCREEN_TTL   = 300   # 5-min screener result cache
INSIGHTS_TTL = 300   # 5-min AI insights cache (aligned with screener)

def _fetch_intraday_15m() -> dict:
    """Batch 15-min bars for Nifty 500 universe, chunked to stay within 512 MB.
    5d gives ~125 bars/stock — enough history for RSI-14 on the 15-min timeframe."""
    return _fetch_chunked("15m", "5d")


def _get_15m_batch():
    now = time.time()
    if "15m_batch" in _cache:
        data, ts = _cache["15m_batch"]
        if now - ts < SCREEN_TTL:
            return data
    with _batch_lock:
        if "15m_batch" in _cache:
            data, ts = _cache["15m_batch"]
            if now - ts < SCREEN_TTL:
                return data
        result = _fetch_intraday_15m()
        _cache["15m_batch"] = (result, time.time())
        return result


def _get_ticker_df(batch, ticker):
    try:
        if isinstance(batch, dict):
            return batch.get(ticker)  # already extracted and validated at download time
        if isinstance(batch.columns, pd.MultiIndex):
            df = batch[ticker].dropna(how="all")
        else:
            df = batch.dropna(how="all")
        return df if len(df) >= 2 else None
    except Exception:
        return None


# ── SMC/ICT Screener ───────────────────────────────────────────────
#
# Setup 1 — Liquidity Sweep → BOS (Break of Structure)
#   Bullish: today's low swept below PDL (stop hunt), price then broke above PDH.
#   Bearish: today's high swept above PDH (stop hunt), price then broke below PDL.
#
# Gates:
#   • >= 2 intraday 15-min bars (ready ~9:45 AM IST — sweep + BOS candle)
#   • Price > ₹100
#   • Paced day volume ≥ 1.2× prev day
#   • Volume spike on BOS candle ≥ 1.5× avg intraday candle volume
#   • Current price within 2% of day extreme — no reversal
#   • Nifty 50 not opposing direction by > 1%


def _detect_swings(highs: list, lows: list, lookback: int = 2):
    """Identify confirmed swing highs and lows from a list of OHLC bar values.
    A swing point requires 'lookback' bars on each side to be confirmed.
    Returns (swing_highs, swing_lows) as lists of (index, price)."""
    n = len(highs)
    sh, sl = [], []
    for i in range(lookback, n - lookback):
        if (all(highs[i] > highs[i - j] for j in range(1, lookback + 1)) and
                all(highs[i] > highs[i + j] for j in range(1, lookback + 1))):
            sh.append((i, highs[i]))
        if (all(lows[i] < lows[i - j] for j in range(1, lookback + 1)) and
                all(lows[i] < lows[i + j] for j in range(1, lookback + 1))):
            sl.append((i, lows[i]))
    return sh, sl


def _screen_smc(direction: str) -> list:
    """SMC/ICT screener — see module comment block above for full logic."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_15m   = _get_15m_batch()
    batch_daily = _get_daily_batch()
    bullish     = direction == "bullish"
    ist         = pytz.timezone("Asia/Kolkata")
    today_date  = datetime.now(ist).date()

    live_quotes = None
    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)

    try:
        market_data = _cached("market", _fetch_market,
                              ttl=LIVE_TTL if _is_live() else CACHE_TTL)
        nifty_chg   = float(market_data.get("NIFTY 50", {}).get("change_pct", 0) or 0)
    except Exception:
        nifty_chg = 0.0

    results = []

    for symbol in universe:
        try:
            intra = _get_ticker_df(batch_15m, symbol)
            daily = _get_ticker_df(batch_daily, symbol)

            if intra is None or daily is None or len(daily) < 2:
                continue

            # Filter to today's 15-min bars
            try:
                if intra.index.tz is not None:
                    today_mask = intra.index.tz_convert(ist).date == today_date
                else:
                    today_mask = [ts.date() == today_date for ts in intra.index]
                today_bars = intra[today_mask]
            except Exception:
                today_bars = intra.iloc[:0]

            if len(today_bars) < 2:    # Need >= 2 bars: a sweep candle + a BOS candle
                continue

            pdh        = float(daily["High"].iloc[-2])
            pdl        = float(daily["Low"].iloc[-2])
            prev_close = float(daily["Close"].iloc[-2])
            prev_vol   = float(daily["Volume"].iloc[-2])

            if pdh <= 0 or pdl <= 0 or prev_close <= 0 or prev_vol <= 0:
                continue

            highs  = today_bars["High"].tolist()
            lows   = today_bars["Low"].tolist()
            closes = today_bars["Close"].tolist()
            vols   = today_bars["Volume"].tolist()
            n_bars = len(closes)

            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else closes[-1]
            else:
                current_price = closes[-1]

            if current_price < 100:
                continue

            day_high = max(highs)
            day_low  = min(lows)
            day_vol  = sum(vols)
            elapsed_min   = max(15.0, n_bars * 15.0)
            paced_vol     = (day_vol / elapsed_min) * 375.0
            vol_ratio     = paced_vol / prev_vol

            if vol_ratio < 1.2:
                continue

            # No-reversal gate: must be within 2% of day extreme
            if bullish and current_price < day_high * 0.98:
                continue
            if not bullish and current_price > day_low * 1.02:
                continue

            avg_candle_vol = day_vol / n_bars if n_bars > 0 else 1
            sw_highs, sw_lows = _detect_swings(highs, lows, lookback=2)

            # Demand / Supply zones from confirmed intraday swing points
            above_sh    = [(idx, p) for idx, p in sw_highs if p > current_price]
            below_sl    = [(idx, p) for idx, p in sw_lows  if p < current_price]
            supply_zone = round(min(above_sh, key=lambda x: x[1])[1], 2) if above_sh else round(pdh, 2)
            demand_zone = round(max(below_sl, key=lambda x: x[1])[1], 2) if below_sl else round(pdl, 2)

            signal = None

            # BOS must have formed within the last 3 bars (45 min).
            # Without this, a 9:45 AM move fires again at 14:52 PM.
            _FRESH = 3

            def _bar_time(bar_idx):
                try:
                    ts = today_bars.index[bar_idx]
                    ts = ts.astimezone(ist) if ts.tzinfo else pytz.utc.localize(ts).astimezone(ist)
                    return ts.strftime("%H:%M")
                except Exception:
                    return datetime.now(ist).strftime("%H:%M")

            # ── Liquidity Sweep → BOS ─────────────────────────────
            if bullish:
                sweep_bar = next(
                    (i for i, lo in enumerate(lows) if lo < pdl), -1
                )
                if sweep_bar < 0:
                    continue
                bos_bar = next(
                    (i for i in range(sweep_bar + 1, n_bars) if closes[i] > pdh), -1
                )
                if bos_bar < 0:
                    continue
                if n_bars - 1 - bos_bar > _FRESH:
                    continue
                if vols[bos_bar] < avg_candle_vol * 1.5:
                    continue
                signal = {
                    "setup_tag": "Liq. Sweep → BOS Bullish",
                    "sl_level":  pdl,
                    "sl_label":  "Below PDL (swept level)",
                    "key_level": pdh,
                    "key_label": "PDH",
                    "bos_time":  _bar_time(bos_bar),
                }
            else:
                sweep_bar = next(
                    (i for i, hi in enumerate(highs) if hi > pdh), -1
                )
                if sweep_bar < 0:
                    continue
                bos_bar = next(
                    (i for i in range(sweep_bar + 1, n_bars) if closes[i] < pdl), -1
                )
                if bos_bar < 0:
                    continue
                if n_bars - 1 - bos_bar > _FRESH:
                    continue
                if vols[bos_bar] < avg_candle_vol * 1.5:
                    continue
                signal = {
                    "setup_tag": "Liq. Sweep → BOS Bearish",
                    "sl_level":  pdh,
                    "sl_label":  "Above PDH (swept level)",
                    "key_level": pdl,
                    "key_label": "PDL",
                    "bos_time":  _bar_time(bos_bar),
                }

            if not signal:
                continue

            # Nifty alignment gate
            if bullish and nifty_chg < -1.0:
                continue
            if not bullish and nifty_chg > 1.0:
                continue

            # Trade plan: structural SL + 1.5R / 3R targets
            sl_raw = signal["sl_level"]
            if bullish:
                sl   = round(sl_raw * 0.997, 2)
                risk = current_price - sl
            else:
                sl   = round(sl_raw * 1.003, 2)
                risk = sl - current_price

            if risk <= 0:
                continue

            sl_pct = round(risk / current_price * 100, 2)
            sign_  = 1 if bullish else -1
            t1     = round(current_price + sign_ * risk * 1.5, 2)
            t2     = round(current_price + sign_ * risk * 3.0, 2)

            # Confidence score 0–100: volume (40) + extreme proximity (35) + structure (25)
            vol_s   = min((vol_ratio - 1.2) / 1.3, 1.0) * 40
            prox_r  = (current_price / day_high) if bullish else (day_low / current_price)
            prox_s  = max(prox_r - 0.98, 0.0) / 0.02 * 35
            str_s   = 25
            conf_score = round(vol_s + prox_s + str_s)
            conf_label = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            gap_pct = round((float(today_bars["Open"].iloc[0]) - prev_close) / prev_close * 100, 2)

            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          gap_pct,
                "day_high":         round(day_high, 2),
                "day_low":          round(day_low, 2),
                "pdh":              round(pdh, 2),
                "pdl":              round(pdl, 2),
                "key_level":        round(signal["key_level"], 2),
                "key_label":        signal["key_label"],
                "volume_ratio":     round(vol_ratio, 2),
                "setup":            signal["setup_tag"],
                "entry":            round(current_price, 2),
                "sl":               sl,
                "sl_pct":           sl_pct,
                "sl_label":         signal["sl_label"],
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      demand_zone,
                "supply_zone":      supply_zone,
                "bos_time":         signal.get("bos_time", datetime.now(ist).strftime("%H:%M")),
            })

        except Exception:
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    # Only GOOD (≥40) or STRONG (≥65) — drop WATCH signals, cap at 5 per panel
    return [r for r in results if r["confidence_score"] >= 40][:5]


# Exhaustion Short — Profit Booking After Rally
#   A stock that has rallied hard intraday on strong volume but has started
#   pulling back from the day's high, with the latest completed 15-min candle
#   turning red — classic "profit booking" exhaustion, good for an intraday
#   short in the cash segment.
#
# Gates:
#   • Active only 9:15 AM – 2:00 PM IST (avoid fresh shorts into the close)
#   • Day change vs prev close ≥ +3% (the "huge rally")
#   • Current price ≥ 0.3% off the day high (pullback already underway)
#   • Paced day volume ≥ 1.3× prev day volume (real participation)
#   • Latest completed candle closed red (close < open)
#   • Nifty 50 not up more than 1% (don't fight a strongly bullish market)
EXH_RALLY_PCT    = 3.0   # min day gain vs prev close to qualify as a "huge rally"
EXH_PULLBACK_PCT = 0.3   # min pullback off day high
EXH_VOL_RATIO    = 1.3   # min paced-volume ratio vs prev day


def _screen_exhaustion_short() -> list:
    """Exhaustion Short / profit-booking screener — see module comment above."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_15m   = _get_15m_batch()
    batch_daily = _get_daily_batch()
    ist         = pytz.timezone("Asia/Kolkata")
    now         = datetime.now(ist)
    today_date  = now.date()

    # Only generate fresh shorts between 9:15 AM and 2:00 PM IST
    if not (_dtime(9, 15) <= now.time() <= _dtime(14, 0)):
        return []

    live_quotes = None
    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)

    try:
        market_data = _cached("market", _fetch_market,
                              ttl=LIVE_TTL if _is_live() else CACHE_TTL)
        nifty_chg   = float(market_data.get("NIFTY 50", {}).get("change_pct", 0) or 0)
    except Exception:
        nifty_chg = 0.0

    results = []

    for symbol in universe:
        try:
            intra = _get_ticker_df(batch_15m, symbol)
            daily = _get_ticker_df(batch_daily, symbol)

            if intra is None or daily is None or len(daily) < 2:
                continue

            try:
                if intra.index.tz is not None:
                    today_mask = intra.index.tz_convert(ist).date == today_date
                else:
                    today_mask = [ts.date() == today_date for ts in intra.index]
                today_bars = intra[today_mask]
            except Exception:
                today_bars = intra.iloc[:0]

            if len(today_bars) < 2:
                continue

            pdl        = float(daily["Low"].iloc[-2])
            prev_close = float(daily["Close"].iloc[-2])
            prev_vol   = float(daily["Volume"].iloc[-2])

            if pdl <= 0 or prev_close <= 0 or prev_vol <= 0:
                continue

            opens  = today_bars["Open"].tolist()
            highs  = today_bars["High"].tolist()
            lows   = today_bars["Low"].tolist()
            closes = today_bars["Close"].tolist()
            vols   = today_bars["Volume"].tolist()
            n_bars = len(closes)

            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else closes[-1]
            else:
                current_price = closes[-1]

            if current_price < 100:
                continue

            day_high = max(highs)
            day_vol  = sum(vols)
            elapsed_min = max(15.0, n_bars * 15.0)
            paced_vol   = (day_vol / elapsed_min) * 375.0
            vol_ratio   = paced_vol / prev_vol

            day_chg_pct = (current_price - prev_close) / prev_close * 100

            # ── Gates ──────────────────────────────────────────────
            if day_chg_pct < EXH_RALLY_PCT:
                continue
            if current_price > day_high * (1 - EXH_PULLBACK_PCT / 100):
                continue
            if vol_ratio < EXH_VOL_RATIO:
                continue
            if closes[-1] >= opens[-1]:        # latest candle must have turned red
                continue
            if nifty_chg > 1.0:                # don't fight a strongly bullish Nifty
                continue

            # Trade plan: short with structural SL above day high
            sl   = round(day_high * 1.003, 2)
            risk = sl - current_price
            if risk <= 0:
                continue

            sl_pct = round(risk / current_price * 100, 2)
            t1     = round(current_price - risk * 1.5, 2)
            t2     = round(current_price - risk * 3.0, 2)

            # Confidence 0–100: rally size (40) + pullback depth (35) + volume (25)
            rally_s      = min(day_chg_pct / 6.0, 1.0) * 40
            pullback_pct = (day_high - current_price) / day_high * 100
            pull_s       = min(pullback_pct / 1.5, 1.0) * 35
            vol_s        = min((vol_ratio - EXH_VOL_RATIO) / 1.0, 1.0) * 25
            conf_score   = round(rally_s + pull_s + vol_s)
            conf_label   = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            gap_pct = round((float(today_bars["Open"].iloc[0]) - prev_close) / prev_close * 100, 2)

            try:
                ts = today_bars.index[-1]
                ts = ts.astimezone(ist) if ts.tzinfo else pytz.utc.localize(ts).astimezone(ist)
                bos_time = ts.strftime("%H:%M")
            except Exception:
                bos_time = now.strftime("%H:%M")

            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          gap_pct,
                "day_chg_pct":      round(day_chg_pct, 2),
                "day_high":         round(day_high, 2),
                "day_low":          round(min(lows), 2),
                "pdh":              round(float(daily["High"].iloc[-2]), 2),
                "pdl":              round(pdl, 2),
                "key_level":        round(day_high, 2),
                "key_label":        "Day High",
                "volume_ratio":     round(vol_ratio, 2),
                "setup":            "Exhaustion Short — Profit Booking",
                "entry":            round(current_price, 2),
                "sl":               sl,
                "sl_pct":           sl_pct,
                "sl_label":         "Above Day High",
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      round(pdl, 2),
                "supply_zone":      round(day_high, 2),
                "bos_time":         bos_time,
            })

        except Exception:
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    return [r for r in results if r["confidence_score"] >= 40][:5]


# ── Market index data ──────────────────────────────────────────────
_INDEX_YF = {"NIFTY 50": "^NSEI", "SENSEX": "^BSESN", "BANK NIFTY": "^NSEBANK"}
_INDEX_KEY = {
    "Nifty 50":  "NIFTY 50",
    "Nifty Bank": "BANK NIFTY",
    "SENSEX":    "SENSEX",
}

def _yf_prev_closes() -> dict:
    """Fetch yesterday's confirmed close for each index from Yahoo Finance.
    Uses 5d period so we always get at least 2 confirmed trading days even
    across weekends or holidays."""
    out = {}
    for name, sym in _INDEX_YF.items():
        try:
            d = yf.Ticker(sym).history(interval="1d", period="5d")
            if len(d) >= 2:
                out[name] = float(d["Close"].iloc[-2])
        except Exception:
            pass
    return out

def _fetch_market():
    # Pre-fetch prev_close from yfinance — reliable baseline for % change.
    # Upstox NSE_INDEX quotes return prev_close_price = 0, so we always need
    # this as a fallback regardless of live/delayed mode.
    yf_prev = _yf_prev_closes()

    # When live, use Upstox for real-time index prices
    if _is_live():
        try:
            r = _http.get(
                f"{UPSTOX_BASE}/market-quote/quotes",
                params={"instrument_key": "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank,BSE_INDEX|SENSEX"},
                headers=_upstox_headers(),
                timeout=10,
            )
            if r.status_code == 200:
                result = {}
                for k, v in r.json().get("data", {}).items():
                    lp = float(v.get("last_price", 0) or 0)
                    if lp <= 0:
                        continue
                    # Determine index name
                    idx_name = next((n for frag, n in _INDEX_KEY.items() if frag in k), None)
                    if not idx_name:
                        continue
                    # Upstox NSE_INDEX instruments return prev_close_price = 0.
                    # Cascade: Upstox field → ohlc.close → yfinance prev day.
                    pc = float(v.get("prev_close_price", 0) or
                               (v.get("ohlc") or {}).get("close", 0) or
                               yf_prev.get(idx_name, 0))
                    chg = round((lp - pc) / pc * 100, 2) if pc > 0 else 0
                    result[idx_name] = {"price": round(lp, 2), "change_pct": chg}
                    print(f"[Market] {idx_name}: lp={lp} pc={pc} chg={chg}%")
                if result:
                    return result
        except Exception as e:
            print(f"[Market] Upstox index error: {e}")

    # Fallback: Yahoo Finance delayed data (5d period avoids holiday edge cases)
    result = {}
    for name, sym in _INDEX_YF.items():
        try:
            d = yf.Ticker(sym).history(interval="1d", period="5d")
            if len(d) >= 2:
                prev = float(d["Close"].iloc[-2])
                curr = float(d["Close"].iloc[-1])
                result[name] = {
                    "price":      round(curr, 2),
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

    # Fallback: use the shared daily batch (already chunked + cached, no extra download)
    daily_batch = _get_daily_batch()
    result = []
    for sym in TICKER_SYMBOLS:
        try:
            daily = _get_ticker_df(daily_batch, sym)
            if daily is None or len(daily) < 2:
                continue
            price      = round(float(daily["Close"].iloc[-1]), 2)
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
    for k in ("s1_bull", "s1_bear", "exh_short",
              "live_quotes", "15m_batch", "ticker", "oi_buildup", "market"):
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
    for k in ("s1_bull", "s1_bear", "exh_short",
              "live_quotes", "15m_batch", "ticker", "oi_buildup", "market"):
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


def _smc_ready() -> bool:
    """SMC signals need >= 2 completed 15-min bars (sweep + BOS candle), i.e. ~9:45 AM IST.
    Earlier than this there's no second bar for a BOS to even be possible."""
    return datetime.now(pytz.timezone("Asia/Kolkata")).time() >= _dtime(9, 45)

@app.route("/api/setup1/bullish")
def api_s1_bull():
    if not _smc_ready():
        return jsonify([])
    active = _cached("s1_bull", _screen_smc, "bullish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 1, "bullish"))

@app.route("/api/setup1/bearish")
def api_s1_bear():
    if not _smc_ready():
        return jsonify([])
    active = _cached("s1_bear", _screen_smc, "bearish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 1, "bearish"))

@app.route("/api/exhaustion/short")
def api_exhaustion_short():
    if not _smc_ready():
        return jsonify([])
    active = _cached("exh_short", _screen_exhaustion_short, ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 2, "bearish"))

@app.route("/api/signals/backfill")
def signals_backfill():
    """Re-run the SMC screener for both directions and refresh the signal store.
    Use this when the server was restarted after market open and _signal_store is empty."""
    if not _smc_ready():
        return jsonify({"error": "Not enough intraday bars yet — SMC requires 9:45 AM IST."}), 400
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    panels = {}
    for direction in ["bullish", "bearish"]:
        label   = _PANEL_LABELS.get((1, direction), "")
        signals = _screen_smc(direction)
        panels[label] = signals
        key = (1, direction, today_str)
        if key not in _signal_store:
            _signal_store[key] = signals
    threading.Thread(target=_persist_signals, args=(today_str,), daemon=True).start()
    total = sum(len(v) for v in panels.values())
    print(f"[Backfill] Refreshed {total} SMC signals for {today_str}")
    return jsonify({
        "date":   today_str,
        "panels": panels,
        "total":  total,
        "note":   "SMC screener refreshed. Signal: Liquidity Sweep→BOS (S1).",
    })


@app.route("/api/signals/backfill.csv")
def signals_backfill_csv():
    """CSV version of /api/signals/backfill for direct download."""
    if not _smc_ready():
        return Response("# SMC screener needs 9:45 AM IST", mimetype="text/csv")
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    headers = ["Panel","Symbol","Setup","Price","Gap%","PDH","PDL",
               "Key Level","Key Label","Vol Ratio","Score","Label",
               "Demand Zone","Supply Zone","Entry","SL","SL%","T1","T2","R:R"]
    rows = [
        "# Samvex LLP — SMC Screener Backfill",
        f"# Date: {today_str}  |  Generated: {datetime.now(ist).strftime('%H:%M IST')}",
        "",
        ",".join(headers),
    ]
    for direction in ["bullish", "bearish"]:
        label   = _PANEL_LABELS.get((1, direction), "")
        signals = _screen_smc(direction)
        for s in signals:
            rows.append(",".join(str(x) for x in [
                f'"{label}"',
                s.get("symbol",""), s.get("setup",""),
                s.get("price",""), s.get("gap_pct",""),
                s.get("pdh",""), s.get("pdl",""),
                s.get("key_level",""), s.get("key_label",""),
                s.get("volume_ratio",""),
                s.get("confidence_score",""), s.get("confidence_label",""),
                s.get("demand_zone",""), s.get("supply_zone",""),
                s.get("entry",""), s.get("sl",""), s.get("sl_pct",""),
                s.get("t1",""), s.get("t2",""), s.get("risk_reward",""),
            ]))
    return Response(
        "\n".join(rows),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=samvex_backfill_{today_str}.csv"},
    )


@app.route("/api/signals/today")
def signals_today_json():
    """Today's morning signals locked at first 9:30 AM run.
    Safe to call at any time — returns the same data all day even after
    the live screener would otherwise show 0 results by afternoon."""
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    panels = {}
    for (setup, direction, date_str), signals in _signal_store.items():
        if date_str == today_str:
            label = _PANEL_LABELS.get((setup, direction), f"Setup {setup} {direction}")
            panels[label] = signals
    return jsonify({
        "date":   today_str,
        "panels": panels,
        "total":  sum(len(v) for v in panels.values()),
        "note":   "Locked at first 9:30 AM IST run. Unchanged for the rest of the trading day.",
    })


@app.route("/api/signals/today.csv")
def signals_today_csv():
    """Download today's morning signals as a CSV file."""
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    headers = ["Panel","Symbol","Setup","Price","Gap%","PDH","PDL",
               "Key Level","Key Label","Vol Ratio","Score","Label",
               "Demand Zone","Supply Zone","Entry","SL","SL%","T1","T2","R:R"]
    rows = ["# Samvex LLP — Today's SMC Signals",
            f"# Date: {today_str}",
            "",
            ",".join(headers)]
    for (setup, direction, date_str), signals in _signal_store.items():
        if date_str != today_str or not signals:
            continue
        label = _PANEL_LABELS.get((setup, direction), f"Setup {setup} {direction}")
        for s in signals:
            rows.append(",".join(str(x) for x in [
                f'"{label}"',
                s.get("symbol",""), s.get("setup",""),
                s.get("price",""), s.get("gap_pct",""),
                s.get("pdh",""), s.get("pdl",""),
                s.get("key_level",""), s.get("key_label",""),
                s.get("volume_ratio",""),
                s.get("confidence_score",""), s.get("confidence_label",""),
                s.get("demand_zone",""), s.get("supply_zone",""),
                s.get("entry",""), s.get("sl",""), s.get("sl_pct",""),
                s.get("t1",""), s.get("t2",""), s.get("risk_reward",""),
            ]))
    csv_text = "\n".join(rows)
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=samvex_signals_{today_str}.csv"},
    )


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

@app.route("/api/chart/<symbol>")
def chart_candles(symbol):
    """Live intraday 30-min candles from Upstox for the chart modal.
    Upstox v2 intraday endpoint only supports 1minute and 30minute intervals.
    Returns 401 if Upstox is not authenticated — no Yahoo Finance fallback."""
    if not _is_live():
        return jsonify({
            "error":    "upstox_not_authenticated",
            "message":  "Upstox token not active. Please re-authenticate to view live charts.",
            "auth_url": "/auth/login",
        }), 401

    base = symbol.upper().replace(".NS", "")
    # Read cached map — never trigger a synchronous CSV download inside a request handler
    if not _instrument_map_loaded:
        return jsonify({
            "error":   "map_loading",
            "message": "Instrument map is still loading on server start — please try again in 30 seconds.",
        }), 503
    ikey = _instrument_map.get(base) or _instrument_map.get(base + "-EQ") or _instrument_map.get(base + "EQ")
    if not ikey:
        return jsonify({
            "error":   "symbol_not_found",
            "message": f"{base} is not available in Upstox live instrument map. "
                       f"The screener may have picked this up from historical data. "
                       f"Use TradingView to view its chart.",
        }), 404

    ist        = pytz.timezone("Asia/Kolkata")
    today_str  = datetime.now(ist).strftime("%Y-%m-%d")
    cached     = _candle_cache.get(ikey, {})

    # Serve from in-memory cache if we already fetched today's candles
    if cached.get("date") == today_str and cached.get("candles"):
        return jsonify({"symbol": base, "interval": "15m", "candles": cached["candles"]})

    try:
        encoded_key = ikey.replace("|", "%7C")
        headers     = _upstox_headers()

        r = _http.get(
            f"{UPSTOX_BASE}/historical-candle/intraday/{encoded_key}/30minute",
            headers=headers, timeout=15,
        )

        if r.status_code == 400:
            # Market closed — return stale cache (any date) if available, else empty
            if cached.get("candles"):
                return jsonify({"symbol": base, "interval": "15m", "candles": cached["candles"]})
            return jsonify({"symbol": base, "interval": "15m", "candles": []})

        if r.status_code != 200:
            return jsonify({
                "error":   "upstox_api_error",
                "message": f"Upstox returned HTTP {r.status_code}.",
            }), 502

        raw     = r.json().get("data", {}).get("candles", [])
        candles = []
        for c in raw:
            try:
                unix_ts = int(pd.Timestamp(c[0]).timestamp())
                candles.append({
                    "time":   unix_ts,
                    "open":   round(float(c[1]), 2),
                    "high":   round(float(c[2]), 2),
                    "low":    round(float(c[3]), 2),
                    "close":  round(float(c[4]), 2),
                    "volume": int(c[5]),
                })
            except Exception:
                continue

        candles.sort(key=lambda x: x["time"])
        # Cache so after-hours requests are served without hitting Upstox
        _candle_cache[ikey] = {"date": today_str, "candles": candles}
        return jsonify({"symbol": base, "interval": "30m", "candles": candles})

    except Exception as e:
        return jsonify({"error": "fetch_error", "message": str(e)}), 500


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


@app.route("/api/debug/imap")
def debug_imap():
    """Quick check: how many symbols are in the instrument map, and is a given symbol findable."""
    # Read the cached map directly — never trigger a synchronous CSV download in a request handler
    imap   = _instrument_map
    sample = dict(list(imap.items())[:5]) if imap else {}
    syms   = [s for s in request.args.get("sym", "").upper().split(",") if s]
    lookup = {s: imap.get(s) for s in syms} if syms else {}
    return jsonify({
        "loaded":         _instrument_map_loaded,
        "symbol_count":   len(imap),
        "sample_entries": sample,
        "lookup":         lookup if syms else "pass ?sym=RELIANCE,ALKYLAMINE to check specific symbols",
    })


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
        "daily_batch_shape": f"{len(daily_batch)} symbols" if isinstance(daily_batch, dict) else str(daily_batch.shape) if daily_batch is not None else "None",
        "daily_batch_empty": (not daily_batch),
    }

    sample = []
    for sym in (_get_fno_universe() or FNO_STOCKS)[:12]:
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


def _build_screen_debug() -> dict:
    """Shared data builder for both JSON and HTML debug endpoints."""
    ist         = pytz.timezone("Asia/Kolkata")
    now         = datetime.now(ist)
    today       = now.date()
    universe    = _load_nifty500() or _get_fno_universe()
    batch_15m   = _get_15m_batch()
    batch_daily = _get_daily_batch()

    funnel = {
        "universe":        len(universe),
        "no_intra_data":   0,
        "few_bars":        0,
        "no_prev_data":    0,
        "price_below_100": 0,
        "vol_under_1_2x":  0,
        "no_reversal_fail":0,
        "setup_fail":      0,
        "s1_bull":         0,
        "s1_bear":         0,
    }

    stale_sample = []
    near_misses  = []

    for symbol in universe:
        intra = _get_ticker_df(batch_15m, symbol)
        daily = _get_ticker_df(batch_daily, symbol)

        if intra is None or len(intra) < 1 or daily is None or len(daily) < 2:
            funnel["no_intra_data"] += 1
            continue

        try:
            if intra.index.tz is not None:
                today_mask = intra.index.tz_convert(ist).date == today
            else:
                today_mask = [ts.date() == today for ts in intra.index]
            today_bars = intra[today_mask]
        except Exception:
            today_bars = intra.iloc[:0]

        if len(today_bars) < 2:
            funnel["few_bars"] += 1
            if len(stale_sample) < 3:
                stale_sample.append({"symbol": symbol.replace(".NS", ""), "bars_today": len(today_bars)})
            continue

        try:
            pdh        = float(daily["High"].iloc[-2])
            pdl        = float(daily["Low"].iloc[-2])
            prev_close = float(daily["Close"].iloc[-2])
            prev_vol   = float(daily["Volume"].iloc[-2])
            if pdh <= 0 or pdl <= 0 or prev_close <= 0 or prev_vol <= 0:
                raise ValueError("zero")
        except Exception:
            funnel["no_prev_data"] += 1
            continue

        highs   = today_bars["High"].tolist()
        lows    = today_bars["Low"].tolist()
        closes  = today_bars["Close"].tolist()
        vols    = today_bars["Volume"].tolist()
        n_bars  = len(closes)
        current = closes[-1]
        day_high = max(highs)
        day_low  = min(lows)
        gap_pct  = round((float(today_bars["Open"].iloc[0]) - prev_close) / prev_close * 100, 2)

        if current < 100:
            funnel["price_below_100"] += 1
            continue

        day_vol     = sum(vols)
        elapsed_min = max(15.0, n_bars * 15.0)
        vol_ratio   = round(((day_vol / elapsed_min) * 375) / prev_vol, 2) if prev_vol > 0 else 0
        if vol_ratio < 1.2:
            funnel["vol_under_1_2x"] += 1
            continue

        near_high = current >= day_high * 0.98
        near_low  = current <= day_low  * 1.02

        sw_highs, sw_lows = _detect_swings(highs, lows, lookback=2)
        avg_vol  = day_vol / n_bars if n_bars > 0 else 1

        row = {
            "symbol":       symbol.replace(".NS", ""),
            "price":        round(current, 2),
            "day_high":     round(day_high, 2),
            "day_low":      round(day_low, 2),
            "pdh":          round(pdh, 2),
            "pdl":          round(pdl, 2),
            "gap_pct":      gap_pct,
            "vol_ratio":    vol_ratio,
            "n_bars":       n_bars,
            "near_high":    near_high,
            "near_low":     near_low,
            "sw_highs":     len(sw_highs),
            "sw_lows":      len(sw_lows),
            "setups_hit":   [],
            "why_no_setup": [],
        }

        # S1 Bull: sweep PDL → BOS above PDH
        sweep_bull = next((i for i, lo in enumerate(lows) if lo < pdl), -1)
        bos_bull   = next((i for i in range(max(sweep_bull, 0) + 1, n_bars)
                           if closes[i] > pdh), -1) if sweep_bull >= 0 else -1
        if sweep_bull >= 0 and bos_bull >= 0 and vols[bos_bull] >= avg_vol * 1.5 and near_high:
            funnel["s1_bull"] += 1
            row["setups_hit"].append("S1_BULL")
        elif not near_high:
            row["why_no_setup"].append("S1 Bull: price not near day high (reversed >2%)")
        elif sweep_bull < 0:
            row["why_no_setup"].append(f"S1 Bull: no PDL sweep (day_low Rs{round(day_low,1)} vs PDL Rs{round(pdl,1)})")
        elif bos_bull < 0:
            row["why_no_setup"].append("S1 Bull: swept PDL but no BOS above PDH yet")
        else:
            row["why_no_setup"].append("S1 Bull: BOS candle volume insufficient")

        # S1 Bear: sweep PDH → BOS below PDL
        sweep_bear = next((i for i, hi in enumerate(highs) if hi > pdh), -1)
        bos_bear   = next((i for i in range(max(sweep_bear, 0) + 1, n_bars)
                           if closes[i] < pdl), -1) if sweep_bear >= 0 else -1
        if sweep_bear >= 0 and bos_bear >= 0 and vols[bos_bear] >= avg_vol * 1.5 and near_low:
            funnel["s1_bear"] += 1
            row["setups_hit"].append("S1_BEAR")
        elif not near_low:
            row["why_no_setup"].append("S1 Bear: price not near day low (reversed >2%)")
        elif sweep_bear < 0:
            row["why_no_setup"].append(f"S1 Bear: no PDH sweep (day_high Rs{round(day_high,1)} vs PDH Rs{round(pdh,1)})")
        elif bos_bear < 0:
            row["why_no_setup"].append("S1 Bear: swept PDH but no BOS below PDL yet")
        else:
            row["why_no_setup"].append("S1 Bear: BOS candle volume insufficient")

        if not row["setups_hit"]:
            funnel["setup_fail"] += 1

        near_misses.append(row)

    near_misses.sort(key=lambda x: x["vol_ratio"], reverse=True)
    passed = [r for r in near_misses if r["setups_hit"]]

    return {
        "time_ist":          now.strftime("%H:%M:%S"),
        "date":              str(today),
        "data_source":       "upstox_live" if _is_live() else "yahoo_15m",
        "15m_batch_size":    len(batch_15m) if batch_15m else 0,
        "daily_batch_size":  len(batch_daily) if batch_daily else 0,
        "funnel":            funnel,
        "stale_sample":      stale_sample,
        "passed_setups":     passed,
        "near_misses_top20": near_misses[:20],
    }


@app.route("/api/debug/screen-new")
def debug_screen_new():
    """JSON version of the filter funnel — for programmatic use."""
    d = _build_screen_debug()
    d["legend"] = {
        "funnel":            "Stocks eliminated at each SMC filter stage (read top to bottom)",
        "near_misses_top20": "Stocks that passed price/volume gates — shows exactly which SMC condition each failed",
        "passed_setups":     "Stocks that triggered at least one SMC setup — should match screener output",
    }
    return jsonify(d)


@app.route("/debug/screen")
def debug_screen_ui():
    """Human-readable HTML debug page for the trading team."""
    d   = _build_screen_debug()
    f   = d["funnel"]
    u   = f["universe"]

    src_label = "UPSTOX LIVE" if d["data_source"] == "upstox_live" else "YAHOO 15-MIN DELAY"
    src_color = "#22c55e"     if d["data_source"] == "upstox_live" else "#f59e0b"
    fired     = f["s1_bull"] + f["s1_bear"]
    fired_cls = "bg" if fired > 0 else ""
    fired_s   = "s" if fired != 1 else ""

    # Funnel waterfall rows
    remaining = u
    wf = ""
    for label, key, color in [
        ("No intraday data",      "no_intra_data",    "#8892a4"),
        ("< 2 bars (pre-9:45)",  "few_bars",          "#8892a4"),
        ("No prev-day data",      "no_prev_data",     "#8892a4"),
        ("Price < Rs100",         "price_below_100",  "#8892a4"),
        ("Volume < 1.2x",         "vol_under_1_2x",   "#fb923c"),
        ("Reversed >2% from ext.","no_reversal_fail", "#f59e0b"),
        ("SMC setup conditions",  "setup_fail",       "#ef4444"),
    ]:
        dropped    = f[key]
        remaining -= dropped
        pct        = round(dropped / u * 100) if u > 0 else 0
        bar_w      = min(dropped * 2, 280)
        minus      = "−" if dropped else ""
        wf += (
            f"<tr>"
            f"<td class='fl'>{label}</td>"
            f"<td class='fd' style='color:{color}'>{minus}{dropped}"
            f"<span class='fp'>({pct}%)</span></td>"
            f"<td class='fb'><div class='bar' style='width:{bar_w}px;background:{color}'></div></td>"
            f"<td class='fr'>{remaining}</td>"
            f"</tr>\n"
        )
    fw = min(fired * 40, 280)
    wf += (
        f"<tr class='fired-row'>"
        f"<td class='fl'>SIGNALS FIRED</td>"
        f"<td class='fd'>+{fired}</td>"
        f"<td class='fb'><div class='bar' style='width:{fw}px;background:#22c55e'></div></td>"
        f"<td class='fr'>{fired}</td>"
        f"</tr>"
    )

    # Signals fired
    ph = ""
    for s in d["passed_setups"]:
        gc = s.get("gap_pct", 0)
        gc_col = "#22c55e" if gc >= 0 else "#ef4444"
        setup_col = "#22c55e" if any("BULL" in x for x in s["setups_hit"]) else "#ef4444"
        gp = ("+" if gc >= 0 else "") + str(gc) + "%"
        ph += (
            f"<tr>"
            f"<td class='sc'>{s['symbol']}</td>"
            f"<td>Rs{s['price']}</td>"
            f"<td style='color:{gc_col}'>{gp}</td>"
            f"<td style='color:{setup_col};font-weight:700'>{', '.join(s['setups_hit'])}</td>"
            f"<td class='nc'>{s.get('n_bars',0)} bars</td>"
            f"<td class='nc'>{s['vol_ratio']}x</td>"
            f"<td></td>"
            f"</tr>\n"
        )
    if not ph:
        ph = "<tr><td colspan='7' class='em'>No SMC signals fired — check debug for why</td></tr>"

    # Near-misses
    nm = ""
    for s in d["near_misses_top20"]:
        gc = s.get("gap_pct", 0)
        gc_col  = "#22c55e" if gc >= 0 else "#ef4444"
        setups  = ", ".join(s["setups_hit"]) if s["setups_hit"] else ""
        why     = "; ".join(s.get("why_no_setup", [])) or "Passed all setups"
        hit_sty = "color:#22c55e;font-weight:700" if s["setups_hit"] else ""
        gp      = ("+" if gc >= 0 else "") + str(gc) + "%"
        nm += (
            f"<tr>"
            f"<td class='sc' style='{hit_sty}'>{s['symbol']}</td>"
            f"<td>Rs{s['price']}</td>"
            f"<td style='color:{gc_col}'>{gp}</td>"
            f"<td class='nc'>{s.get('n_bars',0)} bars / {s.get('sw_highs',0)}H {s.get('sw_lows',0)}L swings</td>"
            f"<td class='nc'>{s['vol_ratio']}x</td>"
            f"<td style='color:#22c55e;font-weight:700'>{setups}</td>"
            f"<td class='wc'>{why}</td>"
            f"</tr>\n"
        )
    if not nm:
        nm = "<tr><td colspan='7' class='em'>No near-misses — all stocks eliminated at common filters</td></tr>"

    css = (
        "*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}"
        "body{background:#0f1117;color:#e2e8f0;font-family:'Segoe UI',system-ui,sans-serif;font-size:13px;padding:0 0 60px}"
        "a{color:#4f8ef7;text-decoration:none}a:hover{text-decoration:underline}"
        ".hdr{background:#181c27;border-bottom:1px solid #2a2f45;padding:16px 32px;display:flex;align-items:center;gap:14px}"
        ".logo{width:32px;height:32px;background:linear-gradient(135deg,#4f8ef7,#7c3aed);border-radius:8px;flex-shrink:0}"
        ".htitle{font-size:18px;font-weight:800;letter-spacing:-.3px}"
        ".hsub{color:#8892a4;font-size:12px;margin-top:2px}"
        ".sbadge{margin-left:auto;padding:4px 10px;border-radius:20px;font-size:10px;font-weight:700;letter-spacing:.5px;border:1px solid;flex-shrink:0}"
        ".main{max-width:1100px;margin:0 auto;padding:28px 24px 0}"
        ".section{background:#181c27;border:1px solid #2a2f45;border-radius:10px;margin-bottom:24px;overflow:hidden}"
        ".sh{padding:14px 20px;border-bottom:1px solid #2a2f45;display:flex;align-items:center;gap:10px}"
        ".sh h2{font-size:14px;font-weight:700}"
        ".badge{background:#1a2a4a;color:#4f8ef7;padding:2px 9px;border-radius:20px;font-size:11px;font-weight:700}"
        ".bg{background:#16422b;color:#22c55e}"
        "table{width:100%;border-collapse:collapse}"
        "th{text-align:left;padding:10px 16px;font-size:11px;color:#8892a4;font-weight:600;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #2a2f45}"
        "td{padding:9px 16px;border-bottom:1px solid #1e2336;vertical-align:top}"
        "tbody tr:last-child td{border-bottom:none}"
        "tbody tr:hover{background:#1e2336}"
        ".sc{color:#4f8ef7;font-weight:700}"
        ".nc{color:#8892a4}"
        ".wc{color:#8892a4;font-size:11px;line-height:1.55;max-width:400px}"
        ".em{color:#8892a4;text-align:center;padding:20px}"
        ".fl{color:#e2e8f0;font-weight:600;min-width:170px;padding:9px 16px}"
        ".fd{min-width:110px;font-weight:700;font-size:13px;padding:9px 8px}"
        ".fp{color:#8892a4;font-weight:400;font-size:11px;margin-left:4px}"
        ".fb{min-width:300px;padding:9px 8px}"
        ".bar{height:8px;border-radius:4px;min-width:2px}"
        ".fr{color:#4f8ef7;font-weight:700;text-align:right;min-width:60px;padding:9px 16px}"
        ".fired-row{background:#0c1a10}"
        ".fired-row td{color:#22c55e!important;font-weight:800!important;font-size:14px!important}"
        ".meta{background:#141820;border-top:1px solid #2a2f45;padding:10px 32px;font-size:11px;color:#8892a4;display:flex;flex-wrap:wrap;gap:20px}"
        ".legend{background:#111520;border:1px solid #2a2f45;border-radius:8px;padding:14px 18px;margin-bottom:24px;font-size:11px;color:#8892a4;line-height:2}"
        ".legend strong{color:#e2e8f0}"
        ".fbreaks{display:flex;gap:24px;padding:10px 16px 14px;font-size:11px;color:#8892a4;flex-wrap:wrap}"
    )

    html = (
        "<!DOCTYPE html><html lang='en'><head>"
        "<meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>Samvex Signal Debugger - {d['date']}</title>"
        f"<style>{css}</style></head><body>"
        "<div class='hdr'>"
        "<div class='logo'></div>"
        f"<div><div class='htitle'>Signal Debugger</div>"
        f"<div class='hsub'>Samvex LLP &middot; {d['date']} at {d['time_ist']} IST</div></div>"
        f"<span class='sbadge' style='color:{src_color};border-color:{src_color}'>{src_label}</span>"
        "</div>"
        "<div class='main'>"
        "<div class='legend'><strong>How to read this page:</strong> "
        "The funnel shows stocks eliminated at each filter gate. "
        "Near-misses passed all price / range / volume filters — they are the closest to firing. "
        "The last column shows exactly which condition each stock failed. "
        "Refresh during live market hours (9:30 – 15:30 IST) for real-time results.</div>"
        "<div class='section'>"
        "<div class='sh'><h2>Filter Funnel</h2>"
        f"<span class='badge'>{u} stocks scanned</span></div>"
        "<table><thead><tr>"
        "<th style='min-width:170px'>Filter Gate</th>"
        "<th>Dropped</th><th style='min-width:300px'></th>"
        "<th style='text-align:right'>Remaining</th>"
        f"</tr></thead><tbody>{wf}</tbody></table>"
        "<div class='fbreaks'>"
        f"<span>S1 Breakout: <strong style='color:#22c55e'>{f['s1_bull']}</strong> bull "
        f"/ <strong style='color:#ef4444'>{f['s1_bear']}</strong> bear</span>"
        "</div></div>"
        "<div class='section'>"
        f"<div class='sh'><h2>SMC Signals That Fired</h2>"
        f"<span class='badge {fired_cls}'>{fired} signal{fired_s}</span></div>"
        "<table><thead><tr>"
        "<th>Symbol</th><th>Price</th><th>Gap</th>"
        "<th>Setup</th><th>Bars</th><th>Vol Ratio</th><th>Notes</th>"
        f"</tr></thead><tbody>{ph}</tbody></table></div>"
        "<div class='section'>"
        "<div class='sh'><h2>Near-Misses &mdash; passed common filters, failed SMC setup</h2>"
        f"<span class='badge'>{len(d['near_misses_top20'])} shown</span></div>"
        "<table><thead><tr>"
        "<th>Symbol</th><th>Price</th><th>Gap</th>"
        "<th>Bars / Swings</th><th>Vol Ratio</th><th>Setup Hit</th>"
        "<th>Why Setup Didn't Fire</th>"
        f"</tr></thead><tbody>{nm}</tbody></table></div>"
        "</div>"
        "<div class='meta'>"
        f"<span>15m batch: {d['15m_batch_size']} symbols</span>"
        f"<span>Daily batch: {d['daily_batch_size']} symbols</span>"
        f"<span>Generated {d['date']} {d['time_ist']} IST</span>"
        "<span style='margin-left:auto'><a href='/api/debug/screen-new'>Raw JSON &#x2197;</a></span>"
        "</div>"
        "</body></html>"
    )

    return Response(html, mimetype="text/html")


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


@app.route("/api/debug/universe")
def debug_universe():
    """Shows which source the screener universe was loaded from and key coverage checks."""
    symbols  = _load_nifty500()
    fno_live = _get_fno_universe()
    checks   = {
        "TATAELXSI":  "TATAELXSI.NS"  in symbols,
        "LTIM":       "LTIM.NS"        in symbols,
        "INFY":       "INFY.NS"        in symbols,
        "TCS":        "TCS.NS"         in symbols,
        "HDFCBANK":   "HDFCBANK.NS"    in symbols,
        "COFORGE":    "COFORGE.NS"     in symbols,
        "PERSISTENT": "PERSISTENT.NS"  in symbols,
    }
    return jsonify({
        "screener_universe_size": len(symbols),
        "source":                 _nifty500_cache.get("source", "unknown"),
        "date":                   _nifty500_cache.get("date", ""),
        "fno_live_universe_size": len(fno_live),
        "fno_source":             "upstox_futures_map" if len(fno_live) > 50 else "hardcoded_fallback",
        "coverage_checks":        checks,
        "sample_first10":         symbols[:10],
        "note": (
            "source=fno_fallback means Nifty 500 CSV failed — "
            "screener used the live F&O list from Upstox futures map. "
            "source=https://... means full Nifty 500 loaded successfully."
        ),
    })


@app.route("/api/ping")
def ping():
    return jsonify({"status": "ok"})

@app.route("/")
def index():
    return jsonify({"service": "Samvex Trading API", "status": "running"})


# ── AI Insights (Claude Haiku) ─────────────────────────────────────
def _get_ai_client():
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=key)
    except ImportError:
        print("[AI] anthropic package not installed")
        return None


def _compute_breadth() -> dict:
    """Count Nifty 500 stocks up vs down today using cached daily batch."""
    batch = _get_daily_batch()
    if not batch:
        return {"up": 0, "down": 0, "total": 0, "up_pct": 0}
    up = down = 0
    for df in batch.values():
        try:
            if len(df) >= 2:
                if float(df["Close"].iloc[-1]) > float(df["Close"].iloc[-2]):
                    up += 1
                else:
                    down += 1
        except Exception:
            pass
    total = up + down
    return {"up": up, "down": down, "total": total,
            "up_pct": round(up / total * 100) if total > 0 else 0}


def _generate_market_brief() -> dict:
    breadth = _compute_breadth()
    if breadth["total"] < 50:
        return {"up": 0, "down": 0, "total": 0, "up_pct": 0, "sentiment": "", "brief": ""}

    up_pct = breadth["up_pct"]
    if up_pct >= 60:
        sentiment = "Bullish"
    elif up_pct <= 40:
        sentiment = "Bearish"
    else:
        sentiment = "Neutral"

    result = {
        "up":        breadth["up"],
        "down":      breadth["down"],
        "total":     breadth["total"],
        "up_pct":    up_pct,
        "sentiment": sentiment,
        "brief":     "",
    }

    # Optional Claude insight — added only if API key present
    client = _get_ai_client()
    if client:
        try:
            market    = _fetch_market()
            nifty     = market.get("NIFTY 50",   {})
            banknifty = market.get("BANK NIFTY", {})
            if nifty or banknifty:
                prompt = (
                    "You are a concise market analyst for Indian equity markets. "
                    "Write exactly 1 sentence (max 20 words) for a professional equity trader. "
                    "Name one specific key level or sector to watch today and why. "
                    "No emojis. No filler. Be direct.\n\n"
                    f"Nifty 50: {nifty.get('price','N/A')} ({nifty.get('change_pct',0):+.2f}%)\n"
                    f"Bank Nifty: {banknifty.get('price','N/A')} ({banknifty.get('change_pct',0):+.2f}%)\n"
                    f"Breadth: {up_pct}% of Nifty 500 above previous close "
                    f"({breadth['up']} up / {breadth['down']} down)"
                )
                resp = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=60,
                    messages=[{"role": "user", "content": prompt}],
                )
                result["brief"] = resp.content[0].text.strip()
        except Exception as e:
            print(f"[AI] Market brief error: {e}")

    return result


def _generate_setup_explanations(setup: int, direction: str, results: list) -> list:
    client = _get_ai_client()
    if not client or not results:
        return []
    try:
        setup_names = {
            (1, "bullish"): "SMC Bullish — liquidity sweep below PDL followed by BOS above PDH",
            (1, "bearish"): "SMC Bearish — liquidity sweep above PDH followed by BOS below PDL",
            (2, "bearish"): "Exhaustion Short — huge intraday rally pulling back from day high on a red candle",
        }
        setup_name = setup_names.get((setup, direction), "")

        lines = []
        for s in results:
            lines.append(
                f"{s['symbol']}: price ₹{s['price']}, gap {s['gap_pct']:+.1f}%, "
                f"setup {s['setup']}, key level {s.get('key_label','')} ₹{s.get('key_level','')}, "
                f"volume {s['volume_ratio']:.1f}x prev day"
            )

        prompt = (
            f'These Indian stocks triggered a "{setup_name}" setup today.\n'
            "For each stock write ONE sentence (max 15 words) describing the specific signal. "
            "Use the actual numbers. Write like a trader briefing another trader. "
            "No generic phrases like 'the stock showed strength'.\n\n"
            + "\n".join(lines)
            + '\n\nReturn ONLY valid JSON: {"SYMBOL": "sentence", ...}. No markdown.'
        )
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        import json
        text = resp.content[0].text.strip()
        if "```" in text:
            parts = text.split("```")
            text  = parts[1].lstrip("json\n").strip() if len(parts) >= 2 else text
        parsed = json.loads(text)
        return [{"symbol": s["symbol"], "explanation": parsed[s["symbol"]]}
                for s in results if parsed.get(s["symbol"])]
    except Exception as e:
        print(f"[AI] Setup explanation error: {e}")
        return []


@app.route("/api/insights/market-brief")
def insights_market_brief():
    data = _cached("ai_market_brief", _generate_market_brief, ttl=INSIGHTS_TTL)
    if not data.get("total", 0):
        return jsonify({"enabled": False})
    return jsonify({**data, "enabled": True})


@app.route("/api/insights/setup-explain")
def insights_setup_explain():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return jsonify({"explanations": [], "enabled": False})
    setup_key = flask_req.args.get("setup", "s1_bull")
    setup_map = {
        "s1_bull": (1, "bullish"), "s1_bear": (1, "bearish"),
        "exh_short": (2, "bearish"),
    }
    if setup_key not in setup_map:
        return jsonify({"error": "invalid setup"}), 400
    setup_num, direction = setup_map[setup_key]
    cached  = _cache.get(setup_key)
    if cached:
        results = cached[0]
    elif setup_key == "exh_short":
        results = _screen_exhaustion_short()
    else:
        results = _screen_smc(direction)
    if not results:
        return jsonify({"explanations": [], "enabled": True})
    cache_key    = f"ai_explain_{setup_key}"
    explanations = _cached(cache_key, _generate_setup_explanations, setup_num, direction, results,
                           ttl=INSIGHTS_TTL)
    return jsonify({"explanations": explanations, "enabled": True})


if __name__ == "__main__":
    print("Starting Samvex Dashboard API on http://localhost:5050")
    app.run(debug=True, port=5050, use_reloader=False)
