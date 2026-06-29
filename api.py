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
import uuid
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
    (2, "bearish"): "Exhaustion Short — Profit Booking After Rally (9:15 AM–2:00 PM)",
    (3, "bullish"): "PDH Breakout — Trend + Momentum (Close > PDH · Price > 200 EMA(15m) · RSI(14) ≥ 60 · Vol ≥ 10L)",
    (4, "bullish"): "ORB — Opening Range Breakout (Bullish · Narrow OR · Multi-day Consolidation · Breakout on Volume)",
    (4, "bearish"): "ORB — Opening Range Breakout (Bearish · Narrow OR · Multi-day Consolidation · Breakdown on Volume)",
    (5, "bullish"): "OI Options Buy — Fresh Long Buildup (Buy Call Candidates)",
    (5, "bearish"): "OI Options Buy — Fresh Short Buildup (Buy Put Candidates)",
    (6, "bullish"): "Demand Zone — Institutional Order Block Retest (Bullish)",
    (6, "bearish"): "Supply Zone — Institutional Order Block Retest (Bearish)",
    (7, "bullish"): "Momentum Breakout — Range Open + Clean PDH Break (Bullish)",
    (7, "bearish"): "Momentum Breakdown — Range Open + Clean PDL Break (Bearish)",
    (8, "bullish"): "Bear Trap Reversal — Failed PDL Breakdown (Bullish)",
    (8, "bearish"): "Bull Trap Reversal — Failed PDH Breakout (Bearish)",
}

def _signals_path(date_str: str) -> str:
    return os.path.join(_SIGNALS_DIR, f"signals_{date_str}.json")

def _persist_signals(date_str: str) -> None:
    """Write today's _signal_store to disk + Redis (called in a background thread).
    Redis is the durable copy — Render free tier wipes /tmp on every cold-start,
    so disk alone silently loses today's signal history mid-session."""
    try:
        payload = {
            f"{k[0]}|{k[1]}": v
            for k, v in _signal_store.items()
            if k[2] == date_str
        }
        blob = {"date": date_str, "panels": payload}
        with open(_signals_path(date_str), "w") as fh:
            json.dump(blob, fh)
        _upstash(["SETEX", f"samvex_signal_store_{date_str}", "259200", json.dumps(blob)])
    except Exception as exc:
        print(f"[Signals] persist error: {exc}")

def _load_persisted_signals() -> None:
    """On server start, reload today's signals — Redis first (survives cold-starts),
    disk as a fallback for local/non-Redis environments."""
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    data = None
    try:
        raw = _upstash(["GET", f"samvex_signal_store_{today_str}"])
        if raw:
            data = json.loads(raw)
    except Exception as exc:
        print(f"[Signals] Redis load error: {exc}")
    if data is None:
        path = _signals_path(today_str)
        if not os.path.exists(path):
            return
        try:
            with open(path) as fh:
                data = json.load(fh)
        except Exception as exc:
            print(f"[Signals] disk load error: {exc}")
            return
    try:
        for k_str, signals in data.get("panels", {}).items():
            setup_str, direction = k_str.split("|")
            _signal_store[(int(setup_str), direction, today_str)] = signals
        total = sum(len(v) for v in _signal_store.values())
        print(f"[Signals] Restored {total} signals for {today_str}")
    except Exception as exc:
        print(f"[Signals] restore error: {exc}")


# ── Signal history persistence (detected_at + inactive signals) ───
# This is what drives the "greyed out / Detected HH:MM / Signal gone" rows in
# Full Table View. It's continuously updated all day, so losing it mid-session
# to a Render cold-start makes previously-detected stocks vanish without a
# trace instead of fading to inactive — hence Redis as the durable copy.
def _history_path(date_str: str) -> str:
    return os.path.join(_SIGNALS_DIR, f"history_{date_str}.json")

def _persist_history(date_str: str) -> None:
    """Write today's _smc_history to disk + Redis (called in a background thread)."""
    try:
        payload = {}
        for k, v in list(_smc_history.items()):
            if k[2] == date_str:
                payload[f"{k[0]}|{k[1]}"] = v
        blob = {"date": date_str, "panels": payload}
        with open(_history_path(date_str), "w") as fh:
            json.dump(blob, fh)
        _upstash(["SETEX", f"samvex_smc_history_{date_str}", "259200", json.dumps(blob)])
    except Exception as exc:
        print(f"[History] persist error: {exc}")

def _load_persisted_history() -> None:
    """On server start, reload today's signal history — Redis first (survives
    cold-starts), disk as a fallback for local/non-Redis environments."""
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    data = None
    try:
        raw = _upstash(["GET", f"samvex_smc_history_{today_str}"])
        if raw:
            data = json.loads(raw)
    except Exception as exc:
        print(f"[History] Redis load error: {exc}")
    if data is None:
        path = _history_path(today_str)
        if not os.path.exists(path):
            return
        try:
            with open(path) as fh:
                data = json.load(fh)
        except Exception as exc:
            print(f"[History] disk load error: {exc}")
            return
    try:
        for k_str, signals in data.get("panels", {}).items():
            setup_str, direction = k_str.split("|")
            _smc_history[(int(setup_str), direction, today_str)] = signals
        total = sum(len(v) for v in _smc_history.values())
        print(f"[History] Restored {total} historical signals for {today_str}")
    except Exception as exc:
        print(f"[History] restore error: {exc}")


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

    # Persist to disk + Redis synchronously whenever state changes. This used to
    # be a background thread, but a Render restart landing between "state
    # changed in memory" and "thread finishes its Redis write" permanently
    # loses that change (this happened in practice — see the NIACL incident).
    # Blocking ~100-300ms here on a detected-signal request is cheap insurance
    # against silently losing a captured signal.
    if changed:
        _persist_history(today_str)

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


# ── Trade Journal — shared, persistent across restarts ──────────────
# Stored as one JSON blob in Upstash Redis (survives Render free-tier
# cold-starts, unlike /tmp) with a local-disk copy as a fallback/backup
# for when Redis isn't configured.
_JOURNAL_REDIS_KEY = "samvex_trade_journal"
_JOURNAL_FILE       = os.path.join(_SIGNALS_DIR, "trade_journal.json")
_JOURNAL_DIRECTIONS = {"Long", "Short"}
_JOURNAL_OUTCOMES   = {"Open", "Win", "Loss", "Breakeven"}


def _load_journal() -> list:
    raw = _upstash(["GET", _JOURNAL_REDIS_KEY])
    if raw:
        try:
            return json.loads(raw)
        except Exception as e:
            print(f"[Journal] Redis parse error: {e}")
    try:
        if os.path.exists(_JOURNAL_FILE):
            with open(_JOURNAL_FILE) as fh:
                return json.load(fh)
    except Exception as e:
        print(f"[Journal] disk load error: {e}")
    return []


def _save_journal(entries: list) -> None:
    payload = json.dumps(entries)
    _upstash(["SET", _JOURNAL_REDIS_KEY, payload])
    try:
        with open(_JOURNAL_FILE, "w") as fh:
            fh.write(payload)
    except Exception as e:
        print(f"[Journal] disk save error: {e}")


def _num_or_none(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _sanitize_journal_entry(data: dict) -> dict:
    """Build a clean entry dict from raw request JSON, auto-computing
    P&L / outcome / R:R when the caller didn't supply them directly."""
    ist = pytz.timezone("Asia/Kolkata")

    direction = data.get("direction") if data.get("direction") in _JOURNAL_DIRECTIONS else "Long"
    sign      = 1 if direction == "Long" else -1

    entry_price = _num_or_none(data.get("entry_price"))
    exit_price  = _num_or_none(data.get("exit_price"))
    quantity    = _num_or_none(data.get("quantity")) or 0
    stop_loss   = _num_or_none(data.get("stop_loss"))
    target      = _num_or_none(data.get("target"))

    pnl_amount = _num_or_none(data.get("pnl_amount"))
    if pnl_amount is None and entry_price is not None and exit_price is not None and quantity:
        pnl_amount = round(sign * (exit_price - entry_price) * quantity, 2)

    pnl_pct = _num_or_none(data.get("pnl_pct"))
    if pnl_pct is None and entry_price not in (None, 0) and exit_price is not None:
        pnl_pct = round(sign * (exit_price - entry_price) / entry_price * 100, 2)

    # R:R = reward achieved / risk taken. Risk is always the planned distance
    # to the stop-loss. Reward prefers the ACTUAL exit (realized R:R) over the
    # original target (planned R:R) once the trade is closed — otherwise a
    # trade exited well short of its target would misleadingly show its
    # full planned ratio instead of what was actually achieved.
    risk_reward = _num_or_none(data.get("risk_reward"))
    if risk_reward is None and entry_price is not None and stop_loss is not None:
        risk         = abs(entry_price - stop_loss)
        reward_basis = exit_price if exit_price is not None else target
        if risk > 0 and reward_basis is not None:
            risk_reward = round(abs(reward_basis - entry_price) / risk, 2)

    outcome = data.get("outcome") if data.get("outcome") in _JOURNAL_OUTCOMES else None
    if not outcome:
        if exit_price is None or pnl_amount is None:
            outcome = "Open"
        elif pnl_amount > 0:
            outcome = "Win"
        elif pnl_amount < 0:
            outcome = "Loss"
        else:
            outcome = "Breakeven"

    return {
        "date":        data.get("date") or datetime.now(ist).strftime("%Y-%m-%d"),
        "trader":      (data.get("trader") or "").strip(),
        "symbol":      (data.get("symbol") or "").strip().upper(),
        "direction":   direction,
        "setup":       (data.get("setup") or "").strip(),
        "entry_price": entry_price,
        "entry_time":  data.get("entry_time") or "",
        "exit_price":  exit_price,
        "exit_time":   data.get("exit_time") or "",
        "quantity":    quantity,
        "stop_loss":   stop_loss,
        "target":      target,
        "pnl_amount":  pnl_amount,
        "pnl_pct":     pnl_pct,
        "risk_reward": risk_reward,
        "outcome":     outcome,
        "notes":       data.get("notes") or "",
        "lessons":     data.get("lessons") or "",
    }


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


# ── Upstox session expiry alerting ──────────────────────────────────
# Upstox tokens expire daily by SEBI mandate — there is no refresh token
# and no long-lived option (confirmed directly with Upstox support: "you
# are required to log in to your account daily... no immediate plans to
# extend the token validity"). The dashboard banner (_updateUpstoxBanner
# in index.html) covers anyone actively looking at the page; this adds an
# out-of-band nudge so a forgotten re-auth doesn't go unnoticed until it
# dies mid-trade. Channel-agnostic: configure EITHER SMTP or Telegram env
# vars (or both, or neither — it just logs if nothing is configured).
ALERT_REMINDER_LEAD_MIN = 150   # send the reminder 2.5h before expiry

_alert_state = {"reminder_sent_for": None, "expired_sent_for": None}


def _send_alert(subject: str, message: str) -> None:
    """Fire-and-forget notification. No-ops to console-only if no channel
    is configured — safe to call unconditionally."""
    print(f"[Alert] {subject}: {message}")

    smtp_host = os.environ.get("ALERT_SMTP_HOST")
    smtp_to   = os.environ.get("ALERT_EMAIL_TO")
    if smtp_host and smtp_to:
        try:
            import smtplib
            from email.mime.text import MIMEText
            smtp_port = int(os.environ.get("ALERT_SMTP_PORT", "587"))
            smtp_user = os.environ.get("ALERT_SMTP_USER", "")
            smtp_pass = os.environ.get("ALERT_SMTP_PASS", "")
            msg = MIMEText(message)
            msg["Subject"] = subject
            msg["From"]    = smtp_user
            msg["To"]      = smtp_to
            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                server.starttls()
                if smtp_user:
                    server.login(smtp_user, smtp_pass)
                server.sendmail(smtp_user, smtp_to.split(","), msg.as_string())
        except Exception as e:
            print(f"[Alert] SMTP send failed: {e}")

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat  = os.environ.get("TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        try:
            _http.post(
                f"https://api.telegram.org/bot{tg_token}/sendMessage",
                json={"chat_id": tg_chat, "text": f"{subject}\n{message}"},
                timeout=10,
            )
        except Exception as e:
            print(f"[Alert] Telegram send failed: {e}")


def _check_token_expiry_and_alert() -> None:
    """Runs periodically in the background. Fires a reminder ~2.5h before
    expiry and an urgent alert once it actually lapses — each only once per
    token (deduped by expires_at), not every cycle."""
    expires_at = _upstox_token.get("expires_at", 0.0)
    if not _upstox_token.get("access_token") or expires_at <= 0:
        return

    now = time.time()
    mins_left = (expires_at - now) / 60

    if mins_left <= 0:
        if _alert_state["expired_sent_for"] != expires_at:
            _send_alert(
                "Samvex Dashboard — Upstox session expired",
                "Live data has dropped to delayed Yahoo Finance. Re-authenticate: "
                "https://samvex-api.onrender.com/auth/login",
            )
            _alert_state["expired_sent_for"] = expires_at
        return

    if mins_left <= ALERT_REMINDER_LEAD_MIN:
        if _alert_state["reminder_sent_for"] != expires_at:
            h, m = int(mins_left // 60), int(mins_left % 60)
            _send_alert(
                "Samvex Dashboard — Upstox session expiring soon",
                f"Live data expires in {h}h {m}m. Re-authenticate before then to avoid "
                f"dropping to delayed data mid-trade: https://samvex-api.onrender.com/auth/login",
            )
            _alert_state["reminder_sent_for"] = expires_at


def _token_expiry_watch_loop() -> None:
    while True:
        try:
            _check_token_expiry_and_alert()
        except Exception as e:
            print(f"[Alert] watch loop error: {e}")
        time.sleep(600)   # check every 10 min


# ── Startup: always pre-warm everything auth-independent ──────────
# Load instrument maps, Nifty 500 list, and daily Yahoo batch in background
# so the first screener request doesn't have to wait 30-60s.
threading.Thread(target=lambda: _load_instrument_map(), daemon=True).start()
threading.Thread(target=lambda: _load_futures_map(),    daemon=True).start()
threading.Thread(target=lambda: _load_nifty500(),       daemon=True).start()
threading.Thread(target=lambda: _get_daily_batch(),     daemon=True).start()

# These two are a single quick Redis GET each (not a slow batch fetch), so
# they're run synchronously, not threaded — a deploy/cold-start restart
# resets _signal_store / _smc_history in memory, and if the first request
# races a background thread that hasn't finished its Redis round-trip yet,
# that request sees empty history and starts overwriting it, permanently
# losing whatever was persisted (this happened in practice: a redeploy
# wiped a still-active Exhaustion Short signal's detected_at history).
# Blocking here for the ~100-300ms a Redis REST call takes is cheap
# insurance against that race.
_load_persisted_signals()
_load_persisted_history()

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

threading.Thread(target=_token_expiry_watch_loop, daemon=True).start()


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


# ── Sector classification + sector-index strength ──────────────────
# Used by the OI Options screener's "Strong Sector" gate. Stock -> sector
# comes from NSE's own sectoral index constituent CSVs (same fetch pattern
# as the Nifty 500 list above); sector strength comes from that sector
# index's own daily % change on Yahoo Finance. Two sectors — Healthcare and
# Oil & Gas — have no reliable Yahoo daily history for their index (only a
# single snapshot row, no usable prev-close); stocks in those sectors are
# excluded from the gate entirely rather than silently skipped or passed.
_SECTOR_CSV_SLUGS = {
    "IT":                 "niftyitlist",
    "Bank":               "niftybanklist",
    "Auto":               "niftyautolist",
    "Pharma":             "niftypharmalist",
    "FMCG":               "niftyfmcglist",
    "Metal":              "niftymetallist",
    "Realty":             "niftyrealtylist",
    "Energy":             "niftyenergylist",
    "PSU Bank":           "niftypsubanklist",
    "Private Bank":       "nifty_privatebanklist",
    "Financial Services": "niftyfinancelist",
    "Media":              "niftymedialist",
    "Infra":              "niftyinfralist",
}
# More specific sub-sectors take priority over the generic "Bank" bucket
# when a stock appears in more than one constituent list (e.g. a private
# bank is also in the generic Bank list)
_SECTOR_PRIORITY = ["Private Bank", "PSU Bank", "IT", "Auto", "Pharma", "FMCG",
                    "Metal", "Realty", "Energy", "Financial Services", "Media",
                    "Infra", "Bank"]
_SECTOR_INDEX_YF = {
    "IT": "^CNXIT", "Bank": "^NSEBANK", "Auto": "^CNXAUTO", "Pharma": "^CNXPHARMA",
    "FMCG": "^CNXFMCG", "Metal": "^CNXMETAL", "Realty": "^CNXREALTY",
    "Energy": "^CNXENERGY", "PSU Bank": "^CNXPSUBANK",
    "Private Bank": "NIFTY_PVT_BANK.NS", "Financial Services": "NIFTY_FIN_SERVICE.NS",
    "Media": "^CNXMEDIA", "Infra": "^CNXINFRA",
}
_sector_map_cache: dict = {"map": {}, "date": ""}

def _load_sector_map() -> dict:
    """Symbol (no .NS) -> sector name, from NSE's sectoral index constituent
    CSVs. Cached per trading day."""
    ist   = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).strftime("%Y-%m-%d")
    if _sector_map_cache["map"] and _sector_map_cache["date"] == today:
        return _sector_map_cache["map"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept":          "text/csv,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    sector_map = {}
    # Iterate lowest -> highest priority so higher-priority sectors are
    # written last and win when a symbol is in more than one list
    for sector in reversed(_SECTOR_PRIORITY):
        slug = _SECTOR_CSV_SLUGS.get(sector)
        if not slug:
            continue
        try:
            url = f"https://nsearchives.nseindia.com/content/indices/ind_{slug}.csv"
            r   = _http.get(url, timeout=15, headers=headers)
            r.raise_for_status()
            df  = pd.read_csv(io.StringIO(r.text))
            col = next((c for c in df.columns if "symbol" in c.lower()), None)
            if not col:
                continue
            for s in df[col].dropna():
                sym = str(s).strip().upper()
                if sym and sym not in ("SYMBOL", "NAN"):
                    sector_map[sym] = sector
        except Exception as e:
            print(f"[Sector] {sector} list fetch error: {e}")

    if sector_map:
        _sector_map_cache["map"]  = sector_map
        _sector_map_cache["date"] = today
    return _sector_map_cache["map"]


def _fetch_sector_strength() -> dict:
    """Sector name -> today's % change of its sectoral index (delayed,
    Yahoo Finance). This is the 'Strong Sector' leg of the OI Options gate."""
    out = {}
    for sector, sym in _SECTOR_INDEX_YF.items():
        try:
            d = yf.Ticker(sym).history(period="1mo", interval="1d")
            if len(d) >= 2:
                prev = float(d["Close"].iloc[-2])
                curr = float(d["Close"].iloc[-1])
                out[sector] = round((curr - prev) / prev * 100, 2) if prev > 0 else 0.0
        except Exception:
            pass
    return out


# ── OI Options Buy Screener — fresh buildup only ────────────────────
#
# For buying Calls/Puts specifically, fresh directional conviction matters
# more than unwinding: Long Buildup (price up + OI up) and Short Buildup
# (price down + OI up) are NEW positions being added — the strongest signal.
# Short Covering / Long Unwinding are existing positions being closed, an
# exhaustion-flavored signal that's a worse fit for buying options (decaying
# momentum into an unwind) — deliberately excluded here, unlike the raw OI
# Buildup classification above which still tags all four buckets.
#
# On top of OI buildup, this also requires the other four legs of a
# "high-probability" intraday options-buying setup: a Strong Sector tailwind,
# the underlying's own relative volume > 2x, price aligned with VWAP, and a
# First 15-Minute opening-range breakout — all five must line up together.
OI_OPT_PRICE_CHG_MIN = 0.5     # min price change % — tighter than the raw classifier's 0.2%
OI_OPT_OI_CHG_MIN    = 3.0     # min OI buildup % — tighter than the raw classifier's 1.0%
OI_OPT_MIN_TURNOVER  = 50_00_00_000   # ₹50 crore min day turnover on the futures contract —
                                       # proxy for the stock's options chain also being liquid
OI_OPT_BUILDUP_MAX   = 10.0    # OI buildup % that maxes the confidence score
OI_OPT_SECTOR_MIN_PCT = 0.3    # min sector-index % change (same direction as the stock) — "Strong Sector"
OI_OPT_REL_VOL_MIN    = 2.0    # underlying stock's own paced volume vs prev day — the "> 2x" leg
OI_OPT_REL_VOL_MAX    = 6.0    # relative volume that maxes the confidence score


# ── Per-panel diagnostics ───────────────────────────────────────────
# Screener functions take an optional `_debug` dict and record which gate
# eliminated each stock that didn't qualify, plus a handful of named-gate
# sample failures. This is the SAME code path as the live screener (just a
# few extra lines at each meaningful gate) so the "why didn't X show up"
# answer can never drift from the actual filtering logic.
def _dbg_fail(_debug, stage, sym, **extra):
    if _debug is None:
        return
    _debug["funnel"][stage] = _debug["funnel"].get(stage, 0) + 1
    bucket = _debug["samples"].setdefault(stage, [])
    if len(bucket) < 8:
        bucket.append({"symbol": sym.replace(".NS", ""), **extra})

def _dbg_pass(_debug, sym, **extra):
    if _debug is None:
        return
    _debug["funnel"]["PASSED"] = _debug["funnel"].get("PASSED", 0) + 1
    bucket = _debug["samples"].setdefault("PASSED", [])
    if len(bucket) < 8:
        bucket.append({"symbol": sym.replace(".NS", ""), **extra})


def _screen_oi_options(direction: str, _debug: dict = None) -> list:
    """OI-based stock-options-buying screener. Returns Buy-Call candidates
    (bullish, fresh Long Buildup) or Buy-Put candidates (bearish, fresh Short
    Buildup) that ALSO clear a Strong Sector + Relative Volume + VWAP +
    First 15-Minute Breakout check on the underlying stock. Entry/SL/targets
    are quoted on the underlying futures price — options buyers size risk
    off the underlying breaking a level, not the option premium itself,
    since we don't have a live options-chain feed for strike-specific
    greeks. Exact strike/premium selection is left to the trader's broker;
    this only ranks which underlyings to look at."""
    if not _is_live():
        if _debug is not None:
            _debug["blocked"] = "Upstox not live — OI Options requires live futures OI data, no delayed-data fallback exists."
        return []

    bullish     = direction == "bullish"
    fmap        = _load_futures_map()
    quotes      = _fetch_futures_quotes()
    if not quotes:
        if _debug is not None:
            _debug["blocked"] = "Could not fetch futures quotes from Upstox just now."
        return []

    sector_map      = _load_sector_map()
    sector_strength = _cached("sector_strength", _fetch_sector_strength, ttl=SCREEN_TTL)
    batch_daily     = _get_daily_batch()
    batch_15m       = _get_15m_batch()
    batch_5m        = _get_5m_batch()
    ist        = pytz.timezone("Asia/Kolkata")
    today_date = datetime.now(ist).date()

    results = []
    for sym, q in quotes.items():
        try:
            last_price = float(q.get("last_price") or 0)
            prev_close = float(q.get("prev_close_price") or 0)
            oi         = float(q.get("oi") or 0)
            oi_day_low = float(q.get("oi_day_low") or 0)
            volume     = int(q.get("volume") or 0)
            lot_size   = fmap.get(sym, {}).get("lot_size", 1)
            ohlc       = q.get("ohlc") or {}
            day_high   = float(ohlc.get("high", 0) or 0)
            day_low    = float(ohlc.get("low", 0) or 0)

            if last_price <= 0 or prev_close <= 0 or oi <= 0 or oi_day_low <= 0:
                _dbg_fail(_debug, "no_quote_data", sym)
                continue
            if day_high <= 0 or day_low <= 0:
                _dbg_fail(_debug, "no_quote_data", sym)
                continue

            price_chg  = (last_price - prev_close) / prev_close * 100
            oi_buildup = (oi - oi_day_low) / oi_day_low * 100

            # Leg 1: fresh buildup only — Long Buildup for bullish, Short Buildup for bearish
            if bullish:
                if price_chg < OI_OPT_PRICE_CHG_MIN or oi_buildup < OI_OPT_OI_CHG_MIN:
                    _dbg_fail(_debug, "fresh_buildup", sym, price_chg=round(price_chg,2), oi_buildup_pct=round(oi_buildup,2))
                    continue
            else:
                if price_chg > -OI_OPT_PRICE_CHG_MIN or oi_buildup < OI_OPT_OI_CHG_MIN:
                    _dbg_fail(_debug, "fresh_buildup", sym, price_chg=round(price_chg,2), oi_buildup_pct=round(oi_buildup,2))
                    continue

            # Liquidity gate: today's futures turnover must be substantial
            turnover = volume * lot_size * last_price
            if turnover < OI_OPT_MIN_TURNOVER:
                _dbg_fail(_debug, "liquidity_turnover", sym, turnover_cr=round(turnover/1e7,1), needed_cr=round(OI_OPT_MIN_TURNOVER/1e7,1))
                continue

            # Leg 2: Strong Sector — exclude entirely if we can't classify
            # the stock or don't have a reliable strength reading for its sector
            sector = sector_map.get(sym)
            if not sector or sector not in sector_strength:
                _dbg_fail(_debug, "sector_unclassified", sym, sector=sector or "unknown")
                continue
            sector_chg = sector_strength[sector]
            if bullish and sector_chg < OI_OPT_SECTOR_MIN_PCT:
                _dbg_fail(_debug, "strong_sector", sym, sector=sector, sector_chg_pct=round(sector_chg,2))
                continue
            if not bullish and sector_chg > -OI_OPT_SECTOR_MIN_PCT:
                _dbg_fail(_debug, "strong_sector", sym, sector=sector, sector_chg_pct=round(sector_chg,2))
                continue

            # Underlying stock's own intraday data (separate from the futures
            # quote above) for legs 3-5: relative volume, VWAP, opening-range breakout
            daily  = _get_ticker_df(batch_daily, f"{sym}.NS")
            intra15 = _get_ticker_df(batch_15m, f"{sym}.NS")
            intra5  = _get_ticker_df(batch_5m, f"{sym}.NS")
            if daily is None or intra15 is None or intra5 is None or len(daily) < 2:
                _dbg_fail(_debug, "no_intraday_data", sym)
                continue

            try:
                if intra15.index.tz is not None:
                    mask15 = intra15.index.tz_convert(ist).date == today_date
                else:
                    mask15 = [ts.date() == today_date for ts in intra15.index]
                today_bars15 = intra15[mask15]
            except Exception:
                today_bars15 = intra15.iloc[:0]
            try:
                if intra5.index.tz is not None:
                    mask5 = intra5.index.tz_convert(ist).date == today_date
                else:
                    mask5 = [ts.date() == today_date for ts in intra5.index]
                today_bars5 = intra5[mask5]
            except Exception:
                today_bars5 = intra5.iloc[:0]

            if len(today_bars15) < 1 or len(today_bars5) < 1:
                _dbg_fail(_debug, "no_today_bars", sym)
                continue

            # Leg 3: Relative Volume > 2x — underlying's own paced volume vs prev day
            prev_vol = float(daily["Volume"].iloc[-2])
            if prev_vol <= 0:
                _dbg_fail(_debug, "no_intraday_data", sym)
                continue
            day_vol_so_far = float(today_bars15["Volume"].sum())
            elapsed_min    = max(15.0, len(today_bars15) * 15.0)
            paced_vol      = (day_vol_so_far / elapsed_min) * 375.0
            rel_vol_ratio  = paced_vol / prev_vol
            if rel_vol_ratio < OI_OPT_REL_VOL_MIN:
                _dbg_fail(_debug, "relative_volume", sym, rel_vol_ratio=round(rel_vol_ratio,2), needed=OI_OPT_REL_VOL_MIN)
                continue

            spot_price = float(today_bars5["Close"].iloc[-1])
            if spot_price <= 0:
                _dbg_fail(_debug, "no_intraday_data", sym)
                continue

            # Leg 4: VWAP Alignment — spot price on the right side of today's VWAP
            highs5  = today_bars5["High"].astype(float).tolist()
            lows5   = today_bars5["Low"].astype(float).tolist()
            closes5 = today_bars5["Close"].astype(float).tolist()
            vols5   = today_bars5["Volume"].astype(float).tolist()
            vwap_den = sum(vols5)
            if vwap_den <= 0:
                _dbg_fail(_debug, "no_intraday_data", sym)
                continue
            vwap_num = sum(((highs5[i] + lows5[i] + closes5[i]) / 3) * vols5[i] for i in range(len(closes5)))
            vwap = vwap_num / vwap_den
            if bullish and spot_price <= vwap:
                _dbg_fail(_debug, "vwap_alignment", sym, spot_price=round(spot_price,2), vwap=round(vwap,2))
                continue
            if not bullish and spot_price >= vwap:
                _dbg_fail(_debug, "vwap_alignment", sym, spot_price=round(spot_price,2), vwap=round(vwap,2))
                continue

            # Leg 5: First 15-Minute Breakout — broke the opening 15-min
            # range and is still on the breakout side of it right now
            or_high15 = float(today_bars15["High"].iloc[0])
            or_low15  = float(today_bars15["Low"].iloc[0])
            day_high15 = float(today_bars15["High"].max())
            day_low15  = float(today_bars15["Low"].min())
            if bullish:
                if day_high15 <= or_high15 or spot_price <= or_high15:
                    _dbg_fail(_debug, "opening_range_breakout", sym, spot_price=round(spot_price,2), or_high15=round(or_high15,2))
                    continue
            else:
                if day_low15 >= or_low15 or spot_price >= or_low15:
                    _dbg_fail(_debug, "opening_range_breakout", sym, spot_price=round(spot_price,2), or_low15=round(or_low15,2))
                    continue

            entry = round(last_price, 2)
            sl    = round(day_low * 0.997, 2) if bullish else round(day_high * 1.003, 2)
            risk  = abs(entry - sl)
            if risk <= 0:
                _dbg_fail(_debug, "invalid_risk", sym)
                continue
            sign_ = 1 if bullish else -1
            t1    = round(entry + sign_ * risk * 1.5, 2)
            t2    = round(entry + sign_ * risk * 3.0, 2)

            # Confidence 0-100, evenly weighted across the formula's 5 legs:
            # OI buildup (20) + relative volume (20) + VWAP distance (20) +
            # sector strength (20) + opening-range breakout distance (20)
            oi_s     = min(oi_buildup / OI_OPT_BUILDUP_MAX, 1.0) * 20
            relvol_s = min((rel_vol_ratio - OI_OPT_REL_VOL_MIN) / (OI_OPT_REL_VOL_MAX - OI_OPT_REL_VOL_MIN), 1.0) * 20
            vwap_dist_pct = abs(spot_price - vwap) / vwap * 100 if vwap > 0 else 0
            vwap_s   = min(vwap_dist_pct / 1.0, 1.0) * 20
            sector_s = min(abs(sector_chg) / 1.0, 1.0) * 20
            or_dist_pct = (abs(spot_price - or_high15) / or_high15 * 100 if bullish
                           else abs(or_low15 - spot_price) / or_low15 * 100)
            or_s     = min(or_dist_pct / 1.0, 1.0) * 20
            conf_score = round(oi_s + relvol_s + vwap_s + sector_s + or_s)
            conf_label = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            _dbg_pass(_debug, sym, confidence_score=conf_score, rel_vol_ratio=round(rel_vol_ratio,2))
            results.append({
                "symbol":           sym,
                "price":            entry,
                "gap_pct":          round(price_chg, 2),
                "oi_chg_pct":       round(oi_buildup, 2),
                "sector":           sector,
                "sector_chg_pct":   sector_chg,
                "vwap":             round(vwap, 2),
                "day_high":         round(day_high, 2),
                "day_low":          round(day_low, 2),
                "pdh":              round(day_high, 2),
                "pdl":              round(day_low, 2),
                "key_level":        round(day_low if bullish else day_high, 2),
                "key_label":        "Day Low" if bullish else "Day High",
                "volume_ratio":     round(rel_vol_ratio, 2),
                "turnover_cr":      round(turnover / 1e7, 1),
                "lot_size":         lot_size,
                "setup":            "Buy Call (CE) — OI Long Buildup" if bullish else "Buy Put (PE) — OI Short Buildup",
                "entry":            entry,
                "sl":               sl,
                "sl_pct":           round(risk / entry * 100, 2),
                "sl_label":         "Underlying SL — below day low" if bullish else "Underlying SL — above day high",
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      round(day_low, 2),
                "supply_zone":      round(day_high, 2),
                "bos_time":         datetime.now(ist).strftime("%H:%M"),
            })

        except Exception:
            _dbg_fail(_debug, "exception", sym)
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    return [r for r in results if r["confidence_score"] >= 40][:5]


# ── Demand / Supply Zone — institutional order-block retest ────────────────
#
# Concept: a "narrow base" daily candle (tight range — quiet accumulation/
# distribution) immediately followed by a strong, high-volume "impulse"
# candle moving away from it is read as an institutional order block — the
# base's range is the zone. A zone is valid as long as price hasn't closed
# back through it since the impulse (a close-through means it failed/got
# absorbed). The setup: price has pulled back to retest a still-valid zone
# (within DZ_PROXIMITY_PCT of it) — the same kind of level smart money
# originally defended, so it has a real chance of holding and rallying again.
DZ_BASE_MAX_RANGE_PCT = 2.0   # base candle's high-low range, as % of its close — defines a "narrow base"
DZ_IMPULSE_MIN_PCT    = 4.0   # min % move away from the base on the very next daily candle
DZ_IMPULSE_VOL_RATIO  = 1.8   # impulse candle's volume vs the avg volume of the days before the base
DZ_ZONE_LOOKBACK_DAYS = 15    # how far back (trading days) to search for a qualifying base+impulse
DZ_VOL_BASELINE_DAYS  = 5     # candles before the base used to compute the volume baseline
DZ_PROXIMITY_PCT      = 1.0   # current price must be within this % of the zone level — the retest gate


def _screen_demand_supply_zone(direction: str, _debug: dict = None) -> list:
    """Demand Zone (bullish) / Supply Zone (bearish) screener — see module comment above."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_daily = _get_daily_batch()
    ist         = pytz.timezone("Asia/Kolkata")
    bullish     = direction == "bullish"

    live_quotes = None
    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)

    results = []

    for symbol in universe:
        try:
            daily = _get_ticker_df(batch_daily, symbol)
            # +1 for today's in-progress row, +1 buffer so a base/impulse pair
            # always has at least one volume-baseline candle before it
            if daily is None or len(daily) < DZ_ZONE_LOOKBACK_DAYS + DZ_VOL_BASELINE_DAYS + 2:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            opens  = daily["Open"].astype(float).tolist()
            highs  = daily["High"].astype(float).tolist()
            lows   = daily["Low"].astype(float).tolist()
            closes = daily["Close"].astype(float).tolist()
            vols   = daily["Volume"].astype(float).tolist()
            n      = len(closes)
            today_idx = n - 1   # in-progress candle — never used as base or impulse

            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else closes[today_idx]
            else:
                current_price = closes[today_idx]

            if current_price < 100:
                _dbg_fail(_debug, "price_below_100", symbol, price=round(current_price,2))
                continue

            zone_level  = None
            zone_age    = None
            impulse_pct = None
            impulse_vr  = None

            # Search backwards from the most recent completed candle so the
            # freshest valid zone wins
            earliest_base = max(DZ_VOL_BASELINE_DAYS, today_idx - 1 - DZ_ZONE_LOOKBACK_DAYS)
            for i in range(today_idx - 2, earliest_base - 1, -1):
                base_close = closes[i]
                if base_close <= 0:
                    continue
                base_range_pct = (highs[i] - lows[i]) / base_close * 100
                if base_range_pct > DZ_BASE_MAX_RANGE_PCT:
                    continue

                imp = i + 1
                if bullish:
                    move_pct = (closes[imp] - base_close) / base_close * 100
                    is_directional = closes[imp] > opens[imp]
                else:
                    move_pct = (base_close - closes[imp]) / base_close * 100
                    is_directional = closes[imp] < opens[imp]
                if move_pct < DZ_IMPULSE_MIN_PCT or not is_directional:
                    continue

                baseline_vols = vols[max(0, i - DZ_VOL_BASELINE_DAYS):i]
                if not baseline_vols:
                    continue
                avg_vol_before = sum(baseline_vols) / len(baseline_vols)
                if avg_vol_before <= 0:
                    continue
                vol_ratio = vols[imp] / avg_vol_before
                if vol_ratio < DZ_IMPULSE_VOL_RATIO:
                    continue

                candidate_zone = lows[i] if bullish else highs[i]

                # Validity: no close since the impulse (up through yesterday)
                # has broken back through the zone
                broken = False
                for j in range(imp + 1, today_idx):
                    if bullish and closes[j] < candidate_zone:
                        broken = True
                        break
                    if not bullish and closes[j] > candidate_zone:
                        broken = True
                        break
                if broken:
                    continue

                zone_level  = candidate_zone
                zone_age    = today_idx - i
                impulse_pct = move_pct
                impulse_vr  = vol_ratio
                break

            if zone_level is None or zone_level <= 0:
                _dbg_fail(_debug, "no_valid_zone", symbol)
                continue

            # Retest gate: current price must be within DZ_PROXIMITY_PCT of the zone
            zone_dist_pct = abs(current_price - zone_level) / zone_level * 100
            if zone_dist_pct > DZ_PROXIMITY_PCT:
                _dbg_fail(_debug, "not_near_zone", symbol, zone_level=round(zone_level,2), dist_pct=round(zone_dist_pct,2), price=round(current_price,2))
                continue

            entry = round(current_price, 2)
            sl    = round(zone_level * 0.99, 2) if bullish else round(zone_level * 1.01, 2)
            risk  = abs(entry - sl)
            if risk <= 0:
                _dbg_fail(_debug, "invalid_risk", symbol)
                continue
            sign_ = 1 if bullish else -1
            t1    = round(entry + sign_ * risk * 1.5, 2)
            t2    = round(entry + sign_ * risk * 3.0, 2)

            prev_close = closes[today_idx - 1]
            gap_pct    = round((opens[today_idx] - prev_close) / prev_close * 100, 2) if prev_close > 0 and today_idx < len(opens) else 0.0

            # Confidence 0-100: impulse strength (40) + volume confirmation
            # (35) + zone freshness — more recent zones are more reliable (25)
            impulse_s = min(impulse_pct / 10.0, 1.0) * 40
            vol_s     = min((impulse_vr - DZ_IMPULSE_VOL_RATIO) / 2.0, 1.0) * 35
            fresh_s   = max(0.0, (DZ_ZONE_LOOKBACK_DAYS - zone_age) / DZ_ZONE_LOOKBACK_DAYS) * 25
            conf_score = round(impulse_s + vol_s + fresh_s)
            conf_label = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            _dbg_pass(_debug, symbol, confidence_score=conf_score, zone_level=round(zone_level,2))
            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            entry,
                "gap_pct":          gap_pct,
                "day_high":         round(max(highs[today_idx], current_price), 2),
                "day_low":          round(min(lows[today_idx], current_price), 2),
                "pdh":              round(highs[today_idx - 1], 2),
                "pdl":              round(lows[today_idx - 1], 2),
                "key_level":        round(zone_level, 2),
                "key_label":        "Demand Zone" if bullish else "Supply Zone",
                "volume_ratio":     round(impulse_vr, 2),
                "zone_age_days":    zone_age,
                "setup":            "Demand Zone Retest" if bullish else "Supply Zone Retest",
                "entry":            entry,
                "sl":               sl,
                "sl_pct":           round(risk / entry * 100, 2),
                "sl_label":         "Below Demand Zone" if bullish else "Above Supply Zone",
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      round(zone_level, 2) if bullish else round(lows[today_idx - 1], 2),
                "supply_zone":      round(zone_level, 2) if not bullish else round(highs[today_idx - 1], 2),
                "bos_time":         datetime.now(ist).strftime("%H:%M"),
            })

        except Exception:
            _dbg_fail(_debug, "exception", symbol)
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    return [r for r in results if r["confidence_score"] >= 40][:5]


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
    return _fetch_chunked("1d", "45d")   # 45d gives ~30 trading days — enough for a 20-day Bollinger Band


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
    60d is Yahoo's max lookback for the 15m interval — needed so the PDH
    Breakout screener has enough bars for a real 200-period EMA."""
    return _fetch_chunked("15m", "60d")


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


# ── Yahoo Finance 5-min intraday batch (for impulsive-candle confirmation) ─
def _fetch_intraday_5m() -> dict:
    """Batch 5-min bars for Nifty 500 universe — only needed for today's
    most recent candle, so a short lookback period is enough."""
    return _fetch_chunked("5m", "5d")


def _get_5m_batch():
    now = time.time()
    if "5m_batch" in _cache:
        data, ts = _cache["5m_batch"]
        if now - ts < SCREEN_TTL:
            return data
    with _batch_lock:
        if "5m_batch" in _cache:
            data, ts = _cache["5m_batch"]
            if now - ts < SCREEN_TTL:
                return data
        result = _fetch_intraday_5m()
        _cache["5m_batch"] = (result, time.time())
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



# Exhaustion Short — Profit Booking After Rally
#   A stock that rallied huge YESTERDAY on strong volume, and today is
#   pulling back from the day's high with an impulsive, high-turnover red
#   5-min candle confirming sellers stepping in — classic "profit booking"
#   exhaustion, good for an intraday short in the cash segment.
#
# Gates:
#   • Active only 9:15 AM – 2:00 PM IST (avoid fresh shorts into the close)
#   • Previous day's rally (prev close vs day-before close) ≥ +11%
#   • Current price ≥ 0.3% off the day high (pullback already underway)
#   • Paced day volume ≥ 1.3× prev day volume (real participation)
#   • Latest completed 5-min candle is impulsive: body ≥ 60% of its range,
#     closed red, with turnover (close × volume) ≥ ₹50 crore
#   • That same 5-min candle's volume > avg volume of the same time slot
#     on the previous 2–3 trading days (unusual participation, not routine)
#   • Price has crossed above the 20-day Bollinger upper band (overextended)
#   • Nifty 50 not up more than 1% (don't fight a strongly bullish market)
EXH_PREV_DAY_RALLY_PCT = 11.0     # min previous-day gain to qualify as a "huge rally"
EXH_PULLBACK_PCT       = 0.3      # min pullback off day high
EXH_VOL_RATIO          = 1.3      # min paced-volume ratio vs prev day
EXH_IMPULSE_BODY_RATIO = 0.6      # min body/range ratio on the confirming 5-min candle
EXH_IMPULSE_TURNOVER   = 50_00_00_000  # ₹50 crore min turnover on that 5-min candle
EXH_BB_PERIOD          = 20       # Bollinger Band lookback (daily closes)
EXH_BB_STD_MULT        = 2.0      # Bollinger Band std-dev multiplier


def _screen_exhaustion_short(_debug: dict = None) -> list:
    """Exhaustion Short / profit-booking screener — see module comment above."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_15m   = _get_15m_batch()
    batch_5m    = _get_5m_batch()
    batch_daily = _get_daily_batch()
    ist         = pytz.timezone("Asia/Kolkata")
    now         = datetime.now(ist)
    today_date  = now.date()

    # Only generate fresh shorts between 9:15 AM and 2:00 PM IST
    if not (_dtime(9, 15) <= now.time() <= _dtime(14, 0)):
        if _debug is not None:
            _debug["blocked"] = "Exhaustion Short only fires 9:15 AM–2:00 PM IST — outside that window right now."
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
            intra5 = _get_ticker_df(batch_5m, symbol)
            daily = _get_ticker_df(batch_daily, symbol)

            if intra is None or intra5 is None or daily is None or len(daily) < EXH_BB_PERIOD + 1:
                _dbg_fail(_debug, "no_data", symbol)
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

            try:
                if intra5.index.tz is not None:
                    today_mask5 = intra5.index.tz_convert(ist).date == today_date
                else:
                    today_mask5 = [ts.date() == today_date for ts in intra5.index]
                today_bars5 = intra5[today_mask5]
            except Exception:
                today_bars5 = intra5.iloc[:0]

            if len(today_bars5) < 1:
                continue

            pdl             = float(daily["Low"].iloc[-2])
            prev_close      = float(daily["Close"].iloc[-2])
            prev_vol        = float(daily["Volume"].iloc[-2])
            prev_prev_close = float(daily["Close"].iloc[-3])

            if pdl <= 0 or prev_close <= 0 or prev_vol <= 0 or prev_prev_close <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            prev_day_rally_pct = (prev_close - prev_prev_close) / prev_prev_close * 100
            if prev_day_rally_pct < EXH_PREV_DAY_RALLY_PCT:
                _dbg_fail(_debug, "prev_day_rally", symbol, rally_pct=round(prev_day_rally_pct,2), needed=EXH_PREV_DAY_RALLY_PCT)
                continue

            # Latest completed 5-min candle must be an impulsive, high-turnover red candle
            c5_open  = float(today_bars5["Open"].iloc[-1])
            c5_high  = float(today_bars5["High"].iloc[-1])
            c5_low   = float(today_bars5["Low"].iloc[-1])
            c5_close = float(today_bars5["Close"].iloc[-1])
            c5_vol   = float(today_bars5["Volume"].iloc[-1])

            # That same 5-min slot's volume must beat its own recent (2–3 day) average
            try:
                c5_ts = today_bars5.index[-1]
                c5_ts_ist = c5_ts.astimezone(ist) if c5_ts.tzinfo else pytz.utc.localize(c5_ts).astimezone(ist)
                target_hm = c5_ts_ist.strftime("%H:%M")
                idx_ist = intra5.index.tz_convert(ist) if intra5.index.tz is not None else intra5.index
                same_slot_vols = [
                    float(v) for ts, v in zip(idx_ist, intra5["Volume"])
                    if ts.date() < today_date and ts.strftime("%H:%M") == target_hm
                ][-3:]
            except Exception:
                same_slot_vols = []

            if len(same_slot_vols) < 2:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            avg_same_slot_vol = sum(same_slot_vols) / len(same_slot_vols)
            if c5_vol <= avg_same_slot_vol:
                _dbg_fail(_debug, "unusual_volume", symbol, c5_vol=round(c5_vol), avg_same_slot_vol=round(avg_same_slot_vol))
                continue

            c5_range = c5_high - c5_low
            if c5_range <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            c5_body_ratio = abs(c5_close - c5_open) / c5_range
            c5_turnover   = c5_close * c5_vol

            if c5_close >= c5_open:
                _dbg_fail(_debug, "not_red_candle", symbol)
                continue
            if c5_body_ratio < EXH_IMPULSE_BODY_RATIO:
                _dbg_fail(_debug, "impulse_body", symbol, body_ratio=round(c5_body_ratio,2), needed=EXH_IMPULSE_BODY_RATIO)
                continue
            if c5_turnover < EXH_IMPULSE_TURNOVER:
                _dbg_fail(_debug, "impulse_turnover", symbol, turnover_cr=round(c5_turnover/1e7,1), needed_cr=round(EXH_IMPULSE_TURNOVER/1e7,1))
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
                _dbg_fail(_debug, "price_below_100", symbol, price=round(current_price,2))
                continue

            # Price must have crossed above the 20-day Bollinger upper band
            closes_hist = daily["Close"].iloc[-(EXH_BB_PERIOD + 1):-1].astype(float)
            bb_sma   = float(closes_hist.mean())
            bb_std   = float(closes_hist.std(ddof=0))
            bb_upper = bb_sma + EXH_BB_STD_MULT * bb_std
            if current_price <= bb_upper:
                _dbg_fail(_debug, "bollinger_band", symbol, price=round(current_price,2), bb_upper=round(bb_upper,2))
                continue

            day_high = max(highs)
            day_vol  = sum(vols)
            elapsed_min = max(15.0, n_bars * 15.0)
            paced_vol   = (day_vol / elapsed_min) * 375.0
            vol_ratio   = paced_vol / prev_vol

            day_chg_pct = (current_price - prev_close) / prev_close * 100

            # ── Gates ──────────────────────────────────────────────
            if current_price > day_high * (1 - EXH_PULLBACK_PCT / 100):
                _dbg_fail(_debug, "pullback_depth", symbol, price=round(current_price,2), day_high=round(day_high,2))
                continue
            if vol_ratio < EXH_VOL_RATIO:
                _dbg_fail(_debug, "day_volume", symbol, vol_ratio=round(vol_ratio,2), needed=EXH_VOL_RATIO)
                continue
            if nifty_chg > 1.0:                # don't fight a strongly bullish Nifty
                _dbg_fail(_debug, "nifty_alignment", symbol, nifty_chg=round(nifty_chg,2))
                continue

            # Trade plan: short with structural SL above day high
            sl   = round(day_high * 1.003, 2)
            risk = sl - current_price
            if risk <= 0:
                _dbg_fail(_debug, "invalid_risk", symbol)
                continue

            sl_pct = round(risk / current_price * 100, 2)
            t1     = round(current_price - risk * 1.5, 2)
            t2     = round(current_price - risk * 3.0, 2)

            # Confidence 0–100: prev-day rally size (40) + pullback depth (35) + volume (25)
            rally_s      = min(prev_day_rally_pct / 22.0, 1.0) * 40
            pullback_pct = (day_high - current_price) / day_high * 100
            pull_s       = min(pullback_pct / 1.5, 1.0) * 35
            vol_s        = min((vol_ratio - EXH_VOL_RATIO) / 1.0, 1.0) * 25
            conf_score   = round(rally_s + pull_s + vol_s)
            conf_label   = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            gap_pct = round((float(today_bars["Open"].iloc[0]) - prev_close) / prev_close * 100, 2)

            try:
                ts = today_bars5.index[-1]
                ts = ts.astimezone(ist) if ts.tzinfo else pytz.utc.localize(ts).astimezone(ist)
                bos_time = ts.strftime("%H:%M")
            except Exception:
                bos_time = now.strftime("%H:%M")

            _dbg_pass(_debug, symbol, confidence_score=conf_score)
            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          gap_pct,
                "day_chg_pct":      round(day_chg_pct, 2),
                "prev_day_rally_pct": round(prev_day_rally_pct, 2),
                "bb_upper":         round(bb_upper, 2),
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
            _dbg_fail(_debug, "exception", symbol)
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    return [r for r in results if r["confidence_score"] >= 40][:5]


# PDH Breakout — Trend + Momentum (ported from a trading-team-vetted
# Chartink screen: "UNIVERSEPDHEMA200RSI60V10LKH")
#   Close crossed above PDH, price holding above the 200-period EMA on the
#   15-min chart (trend filter), daily RSI(14) >= 60 (momentum filter), and
#   day volume >= 10 lakh shares (liquidity filter). Plain trend-continuation
#   breakout — no liquidity sweep required first (unlike Setup 1).
#
# Gates:
#   • Close crossed above PDH within the last 3 bars (45 min) — fresh breakout
#   • Current price >= 200-period EMA on 15-min closes
#   • Daily RSI(14) >= 60
#   • Day volume so far >= 10,00,000 shares
PDH_EMA_PERIOD = 200
PDH_RSI_PERIOD = 14
PDH_RSI_MIN    = 60.0
PDH_VOL_MIN    = 1_400_000   # 10 lakh shares
_PDH_FRESH     = 3           # breakout candle must be within the last 3 bars (45 min)


def _ema(values: list, period: int):
    """Simple EMA seeded with the first value — fine for a trend filter
    even when fewer than `period` bars are available."""
    if not values:
        return None
    alpha = 2 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = alpha * v + (1 - alpha) * ema
    return ema


def _wilder_rsi(closes: list, period: int = 14):
    """Wilder's RSI over a list of closes (oldest → newest)."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _screen_pdh_trend(_debug: dict = None) -> list:
    """PDH Breakout / Trend + Momentum screener — see module comment above."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_15m   = _get_15m_batch()
    batch_daily = _get_daily_batch()
    ist         = pytz.timezone("Asia/Kolkata")
    today_date  = datetime.now(ist).date()

    live_quotes = None
    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)

    results = []

    for symbol in universe:
        try:
            intra = _get_ticker_df(batch_15m, symbol)
            daily = _get_ticker_df(batch_daily, symbol)

            if intra is None or daily is None or len(daily) < PDH_RSI_PERIOD + 2:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            try:
                if intra.index.tz is not None:
                    today_mask = intra.index.tz_convert(ist).date == today_date
                else:
                    today_mask = [ts.date() == today_date for ts in intra.index]
                today_bars = intra[today_mask]
            except Exception:
                today_bars = intra.iloc[:0]

            if len(today_bars) < 1:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            pdh = float(daily["High"].iloc[-2])
            if pdh <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            closes = today_bars["Close"].tolist()
            vols   = today_bars["Volume"].tolist()
            n_bars = len(closes)

            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else closes[-1]
            else:
                current_price = closes[-1]

            if current_price < 100 or current_price <= pdh:
                _dbg_fail(_debug, "not_above_pdh", symbol, price=round(current_price,2), pdh=round(pdh,2))
                continue

            # Fresh breakout: the bar that first closed above PDH must be recent
            bos_bar = next((i for i, c in enumerate(closes) if c > pdh), -1)
            if bos_bar < 0:
                _dbg_fail(_debug, "not_above_pdh", symbol, price=round(current_price,2), pdh=round(pdh,2))
                continue
            if n_bars - 1 - bos_bar > _PDH_FRESH:
                _dbg_fail(_debug, "breakout_too_stale", symbol, bars_ago=n_bars-1-bos_bar, allowed=_PDH_FRESH)
                continue

            # Trend filter: price above 200-period EMA on all available 15-min closes
            all_closes_15m = intra["Close"].dropna().astype(float).tolist()
            if len(all_closes_15m) < 30:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            ema200 = _ema(all_closes_15m, PDH_EMA_PERIOD)
            if ema200 is None or current_price < ema200:
                _dbg_fail(_debug, "below_200ema", symbol, price=round(current_price,2), ema200=round(ema200,2) if ema200 else None)
                continue

            # Momentum filter: daily RSI(14) >= 60
            daily_closes = daily["Close"].astype(float).tolist()
            rsi = _wilder_rsi(daily_closes, PDH_RSI_PERIOD)
            if rsi is None or rsi < PDH_RSI_MIN:
                _dbg_fail(_debug, "rsi_too_low", symbol, rsi=round(rsi,1) if rsi is not None else None, needed=PDH_RSI_MIN)
                continue

            # Liquidity filter: day volume so far >= 10 lakh shares
            day_vol = sum(vols)
            if day_vol < PDH_VOL_MIN:
                _dbg_fail(_debug, "day_volume", symbol, day_vol=int(day_vol), needed=PDH_VOL_MIN)
                continue

            prev_close = float(daily["Close"].iloc[-2])
            pdl        = float(daily["Low"].iloc[-2])
            prev_vol   = float(daily["Volume"].iloc[-2])
            elapsed_min = max(15.0, n_bars * 15.0)
            paced_vol   = (day_vol / elapsed_min) * 375.0
            vol_ratio   = (paced_vol / prev_vol) if prev_vol > 0 else 0.0

            # Trade plan: structural SL just below PDH (the breakout level)
            sl   = round(pdh * 0.997, 2)
            risk = current_price - sl
            if risk <= 0:
                _dbg_fail(_debug, "invalid_risk", symbol)
                continue

            sl_pct = round(risk / current_price * 100, 2)
            t1     = round(current_price + risk * 1.5, 2)
            t2     = round(current_price + risk * 3.0, 2)

            # Confidence 0–100: RSI strength (35) + EMA distance (30) + volume (35)
            rsi_s      = min(max(rsi - PDH_RSI_MIN, 0) / 20.0, 1.0) * 35
            ema_dist   = (current_price - ema200) / ema200 * 100
            ema_s      = min(max(ema_dist, 0) / 5.0, 1.0) * 30
            vol_s      = min(day_vol / (PDH_VOL_MIN * 3), 1.0) * 35
            conf_score = round(rsi_s + ema_s + vol_s)
            conf_label = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            gap_pct = round((float(today_bars["Open"].iloc[0]) - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

            try:
                ts = today_bars.index[bos_bar]
                ts = ts.astimezone(ist) if ts.tzinfo else pytz.utc.localize(ts).astimezone(ist)
                bos_time = ts.strftime("%H:%M")
            except Exception:
                bos_time = datetime.now(ist).strftime("%H:%M")

            _dbg_pass(_debug, symbol, confidence_score=conf_score)
            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          gap_pct,
                "rsi":              round(rsi, 1),
                "ema200_15m":       round(ema200, 2),
                "day_high":         round(max(today_bars["High"].tolist()), 2),
                "day_low":          round(min(today_bars["Low"].tolist()), 2),
                "pdh":              round(pdh, 2),
                "pdl":              round(pdl, 2),
                "key_level":        round(pdh, 2),
                "key_label":        "PDH",
                "volume_ratio":     round(vol_ratio, 2),
                "setup":            "PDH Breakout — Trend + Momentum",
                "entry":            round(current_price, 2),
                "sl":               sl,
                "sl_pct":           sl_pct,
                "sl_label":         "Below PDH (breakout level)",
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      round(pdl, 2),
                "supply_zone":      round(pdh, 2),
                "bos_time":         bos_time,
            })

        except Exception:
            _dbg_fail(_debug, "exception", symbol)
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    return [r for r in results if r["confidence_score"] >= 40][:5]


# ── ORB — Opening Range Breakout ────────────────────────────────────────
#
# Concept: the first 5-min candle (9:15-9:20) sets a tight "opening range",
# on a stock that's also been rangebound over the last few days. A decisive,
# high-volume breakout out of that range tends to run one-sided, since it's
# effectively a fresh intraday BOS with a well-defined risk level (the
# opposite side of the range).
ORB_MAX_RANGE_PCT         = 0.4    # opening 5-min candle range must be <= 0.4% of price (narrow)
ORB_VOL_RATIO             = 1.6    # breakout candle volume vs avg volume of the consolidation bars
ORB_FRESHNESS_BARS        = 6      # breakout candle must be within the last 6 5-min bars (30 min)
ORB_NO_REVERSAL_PCT       = 1.5    # current price must stay within 1.5% of the day extreme (one-side rally, no round-trip)
ORB_PRIOR_DAYS            = 3      # also require the stock to have been rangebound over the last N trading days
ORB_PRIOR_RANGE_MAX_PCT   = 5.0    # (highest high - lowest low) over those N days, as % of price — multi-day consolidation, not just an intraday narrow open


def _screen_orb(direction: str, _debug: dict = None) -> list:
    """Opening Range Breakout screener — see module comment above."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_5m    = _get_5m_batch()
    batch_daily = _get_daily_batch()
    ist         = pytz.timezone("Asia/Kolkata")
    today_date  = datetime.now(ist).date()
    bullish     = direction == "bullish"

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
            intra5 = _get_ticker_df(batch_5m, symbol)
            daily  = _get_ticker_df(batch_daily, symbol)
            if intra5 is None or daily is None or len(daily) < ORB_PRIOR_DAYS + 1:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            # Multi-day consolidation gate: the stock must already have been
            # rangebound over the last ORB_PRIOR_DAYS trading days (excluding
            # today's in-progress bar) — the opening-range narrowness check
            # alone only looks at today, this confirms it's not narrow by
            # fluke on a stock that was actually trending into the open.
            prior_days = daily.iloc[-(ORB_PRIOR_DAYS + 1):-1]
            prior_high = float(prior_days["High"].max())
            prior_low  = float(prior_days["Low"].min())
            if prior_low <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            prior_range_pct = (prior_high - prior_low) / prior_low * 100
            if prior_range_pct > ORB_PRIOR_RANGE_MAX_PCT:
                _dbg_fail(_debug, "multi_day_not_consolidating", symbol, prior_range_pct=round(prior_range_pct,2), needed_max=ORB_PRIOR_RANGE_MAX_PCT)
                continue

            try:
                if intra5.index.tz is not None:
                    today_mask5 = intra5.index.tz_convert(ist).date == today_date
                else:
                    today_mask5 = [ts.date() == today_date for ts in intra5.index]
                today_bars5 = intra5[today_mask5]
            except Exception:
                today_bars5 = intra5.iloc[:0]

            # Need the opening candle plus at least one more bar to check for a breakout
            if len(today_bars5) < 2:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            opens  = today_bars5["Open"].tolist()
            highs  = today_bars5["High"].tolist()
            lows   = today_bars5["Low"].tolist()
            closes = today_bars5["Close"].tolist()
            vols   = today_bars5["Volume"].tolist()
            n_bars = len(closes)

            or_high = highs[0]
            or_low  = lows[0]
            or_close = closes[0]
            or_range = or_high - or_low
            if or_close <= 0 or or_range <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            # Narrow opening range gate
            or_range_pct = or_range / or_close * 100
            if or_range_pct > ORB_MAX_RANGE_PCT:
                _dbg_fail(_debug, "opening_range_too_wide", symbol, or_range_pct=round(or_range_pct,2), needed_max=ORB_MAX_RANGE_PCT)
                continue

            # Find the first bar (after the opening candle) whose close breaks
            # the range in our direction, while confirming every bar before it
            # stayed inside the range — i.e. a genuine, undisturbed consolidation.
            breakout_bar = -1
            for i in range(1, n_bars):
                broke = (closes[i] > or_high) if bullish else (closes[i] < or_low)
                if broke:
                    breakout_bar = i
                    break
                if closes[i] > or_high or closes[i] < or_low:
                    # broke the *other* side first — this isn't a clean ORB setup
                    breakout_bar = -1
                    break
            if breakout_bar < 0:
                _dbg_fail(_debug, "no_clean_breakout", symbol)
                continue

            # Freshness: breakout must be recent
            if n_bars - 1 - breakout_bar > ORB_FRESHNESS_BARS:
                _dbg_fail(_debug, "breakout_too_stale", symbol, bars_ago=n_bars-1-breakout_bar, allowed=ORB_FRESHNESS_BARS)
                continue

            # VWAP confirmation: the breakout candle itself must close on the
            # right side of the day's volume-weighted average price (computed
            # from the open through the breakout bar) — confirms real
            # participation behind the move, not just a thin print.
            vwap_den = sum(vols[:breakout_bar + 1])
            if vwap_den <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            vwap_num = sum(
                ((highs[i] + lows[i] + closes[i]) / 3) * vols[i]
                for i in range(breakout_bar + 1)
            )
            vwap = vwap_num / vwap_den
            if bullish:
                if closes[breakout_bar] <= vwap:
                    _dbg_fail(_debug, "vwap_alignment", symbol, breakout_close=round(closes[breakout_bar],2), vwap=round(vwap,2))
                    continue
            else:
                if closes[breakout_bar] >= vwap:
                    _dbg_fail(_debug, "vwap_alignment", symbol, breakout_close=round(closes[breakout_bar],2), vwap=round(vwap,2))
                    continue

            # Volume confirmation: breakout candle vs avg volume of the
            # consolidation bars that preceded it
            consolidation_vols = vols[1:breakout_bar]
            avg_consol_vol = (sum(consolidation_vols) / len(consolidation_vols)
                               if consolidation_vols else vols[0])
            if avg_consol_vol <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            vol_ratio = vols[breakout_bar] / avg_consol_vol
            if vol_ratio < ORB_VOL_RATIO:
                _dbg_fail(_debug, "breakout_volume", symbol, vol_ratio=round(vol_ratio,2), needed=ORB_VOL_RATIO)
                continue

            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else closes[-1]
            else:
                current_price = closes[-1]

            if current_price < 100:
                _dbg_fail(_debug, "price_below_100", symbol, price=round(current_price,2))
                continue

            day_high = max(highs)
            day_low  = min(lows)

            # One-side rally gate: no round-trip back through the breakout level
            if bullish:
                if current_price < day_high * (1 - ORB_NO_REVERSAL_PCT / 100):
                    _dbg_fail(_debug, "round_tripped", symbol, price=round(current_price,2), day_high=round(day_high,2))
                    continue
                if nifty_chg < -1.0:
                    _dbg_fail(_debug, "nifty_alignment", symbol, nifty_chg=round(nifty_chg,2))
                    continue
            else:
                if current_price > day_low * (1 + ORB_NO_REVERSAL_PCT / 100):
                    _dbg_fail(_debug, "round_tripped", symbol, price=round(current_price,2), day_low=round(day_low,2))
                    continue
                if nifty_chg > 1.0:
                    _dbg_fail(_debug, "nifty_alignment", symbol, nifty_chg=round(nifty_chg,2))
                    continue

            prev_close = float(daily["Close"].iloc[-2])
            pdh        = float(daily["High"].iloc[-2])
            pdl        = float(daily["Low"].iloc[-2])
            gap_pct    = round((opens[0] - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

            # Trade plan: SL on the opposite side of the opening range
            if bullish:
                sl   = round(or_low * 0.997, 2)
                risk = current_price - sl
            else:
                sl   = round(or_high * 1.003, 2)
                risk = sl - current_price
            if risk <= 0:
                _dbg_fail(_debug, "invalid_risk", symbol)
                continue

            sign_  = 1 if bullish else -1
            sl_pct = round(risk / current_price * 100, 2)
            t1     = round(current_price + sign_ * risk * 1.5, 2)
            t2     = round(current_price + sign_ * risk * 3.0, 2)

            # Confidence 0-100: breakout volume (40) + proximity to day extreme,
            # i.e. how one-sided the rally has stayed (35) + how narrow/clean
            # the opening range was (25)
            vol_s    = min((vol_ratio - ORB_VOL_RATIO) / 1.5, 1.0) * 40
            prox_r   = (current_price / day_high) if bullish else (day_low / current_price)
            prox_s   = max(prox_r - (1 - ORB_NO_REVERSAL_PCT / 100), 0.0) / (ORB_NO_REVERSAL_PCT / 100) * 35
            range_s  = max(ORB_MAX_RANGE_PCT - or_range_pct, 0.0) / ORB_MAX_RANGE_PCT * 25
            conf_score = round(vol_s + prox_s + range_s)
            conf_label = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            try:
                ts = today_bars5.index[breakout_bar]
                ts = ts.astimezone(ist) if ts.tzinfo else pytz.utc.localize(ts).astimezone(ist)
                bos_time = ts.strftime("%H:%M")
            except Exception:
                bos_time = datetime.now(ist).strftime("%H:%M")

            _dbg_pass(_debug, symbol, confidence_score=conf_score)
            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          gap_pct,
                "day_high":         round(day_high, 2),
                "day_low":          round(day_low, 2),
                "pdh":              round(pdh, 2),
                "pdl":              round(pdl, 2),
                "vwap":             round(vwap, 2),
                "key_level":        round(or_high if bullish else or_low, 2),
                "key_label":        "OR High" if bullish else "OR Low",
                "volume_ratio":     round(vol_ratio, 2),
                "setup":            "ORB Bullish" if bullish else "ORB Bearish",
                "entry":            round(current_price, 2),
                "sl":               sl,
                "sl_pct":           sl_pct,
                "sl_label":         "Below OR Low" if bullish else "Above OR High",
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      round(or_low, 2),
                "supply_zone":      round(or_high, 2),
                "bos_time":         bos_time,
            })

        except Exception:
            _dbg_fail(_debug, "exception", symbol)
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    return [r for r in results if r["confidence_score"] >= 40][:5]


# ── Momentum Breakout — clean range-open break + tight confirmation ────────
#
# Concept: a stock that opened WITHIN yesterday's range (no gap either way)
# and then breaks PDH/PDL on good volume is a clean, un-telegraphed breakout
# (no overnight gap already pricing it in). The very next 5-min candle being
# small and in the same direction (green after a PDH break, red after a PDL
# break) shows the move being absorbed calmly rather than immediately
# round-tripping — read as room for a real follow-through rally/fall.
MB_VOL_RATIO          = 1.9   # breakout candle volume vs avg volume of bars so far today
MB_CONFIRM_MAX_RANGE_PCT = 0.5   # the very next 5-min candle's range, as % of price, must be < 0.5%
MB_FRESHNESS_BARS     = 6     # the breakout+confirm pair must be within the last 6 5-min bars (30 min)
MB_NO_REVERSAL_PCT    = 1.5   # current price must stay within 1.5% of the day extreme


def _screen_momentum_breakout(direction: str, _debug: dict = None) -> list:
    """Momentum Breakout screener — see module comment above."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_5m    = _get_5m_batch()
    batch_daily = _get_daily_batch()
    ist         = pytz.timezone("Asia/Kolkata")
    today_date  = datetime.now(ist).date()
    bullish     = direction == "bullish"

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
            intra5 = _get_ticker_df(batch_5m, symbol)
            daily  = _get_ticker_df(batch_daily, symbol)
            if intra5 is None or daily is None or len(daily) < 2:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            try:
                if intra5.index.tz is not None:
                    today_mask5 = intra5.index.tz_convert(ist).date == today_date
                else:
                    today_mask5 = [ts.date() == today_date for ts in intra5.index]
                today_bars5 = intra5[today_mask5]
            except Exception:
                today_bars5 = intra5.iloc[:0]

            # Need the open bar + at least 1 bar to break out + 1 bar to confirm
            if len(today_bars5) < 3:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            pdh = float(daily["High"].iloc[-2])
            pdl = float(daily["Low"].iloc[-2])
            if pdh <= 0 or pdl <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            opens  = today_bars5["Open"].tolist()
            highs  = today_bars5["High"].tolist()
            lows   = today_bars5["Low"].tolist()
            closes = today_bars5["Close"].tolist()
            vols   = today_bars5["Volume"].tolist()
            n_bars = len(closes)

            # Opened within yesterday's range — no gap either way
            if opens[0] > pdh or opens[0] < pdl:
                _dbg_fail(_debug, "gapped_open", symbol, open=round(opens[0],2), pdh=round(pdh,2), pdl=round(pdl,2))
                continue

            # Find the first bar (after at least 1 prior bar, so a volume
            # baseline exists) whose close breaks PDH/PDL
            breakout_bar = -1
            for i in range(1, n_bars - 1):   # leave room for a confirm bar after it
                broke = (closes[i] > pdh) if bullish else (closes[i] < pdl)
                if broke:
                    breakout_bar = i
                    break
            if breakout_bar < 0:
                _dbg_fail(_debug, "no_clean_breakout", symbol)
                continue

            # Volume confirmation on the breakout candle itself
            avg_vol_before = sum(vols[:breakout_bar]) / breakout_bar
            if avg_vol_before <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            vol_ratio = vols[breakout_bar] / avg_vol_before
            if vol_ratio < MB_VOL_RATIO:
                _dbg_fail(_debug, "breakout_volume", symbol, vol_ratio=round(vol_ratio,2), needed=MB_VOL_RATIO)
                continue

            # The very next 5-min candle must be small AND in the same direction
            confirm = breakout_bar + 1
            if confirm >= n_bars:
                _dbg_fail(_debug, "no_confirm_bar_yet", symbol)
                continue
            confirm_close = closes[confirm]
            confirm_open  = opens[confirm]
            if confirm_close <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            confirm_range_pct = (highs[confirm] - lows[confirm]) / confirm_close * 100
            if confirm_range_pct >= MB_CONFIRM_MAX_RANGE_PCT:
                _dbg_fail(_debug, "confirm_candle_too_wide", symbol, confirm_range_pct=round(confirm_range_pct,2), needed_max=MB_CONFIRM_MAX_RANGE_PCT)
                continue
            if bullish and confirm_close <= confirm_open:
                _dbg_fail(_debug, "confirm_wrong_direction", symbol)
                continue
            if not bullish and confirm_close >= confirm_open:
                _dbg_fail(_debug, "confirm_wrong_direction", symbol)
                continue

            # Freshness: the breakout+confirm pair must be recent
            if n_bars - 1 - confirm > MB_FRESHNESS_BARS:
                _dbg_fail(_debug, "breakout_too_stale", symbol, bars_ago=n_bars-1-confirm, allowed=MB_FRESHNESS_BARS)
                continue

            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else closes[-1]
            else:
                current_price = closes[-1]

            if current_price < 100:
                _dbg_fail(_debug, "price_below_100", symbol, price=round(current_price,2))
                continue

            day_high = max(highs)
            day_low  = min(lows)

            # No-reversal gate: no round-trip back through the breakout level
            if bullish:
                if current_price < day_high * (1 - MB_NO_REVERSAL_PCT / 100):
                    _dbg_fail(_debug, "round_tripped", symbol, price=round(current_price,2), day_high=round(day_high,2))
                    continue
                if nifty_chg < -1.0:
                    _dbg_fail(_debug, "nifty_alignment", symbol, nifty_chg=round(nifty_chg,2))
                    continue
            else:
                if current_price > day_low * (1 + MB_NO_REVERSAL_PCT / 100):
                    _dbg_fail(_debug, "round_tripped", symbol, price=round(current_price,2), day_low=round(day_low,2))
                    continue
                if nifty_chg > 1.0:
                    _dbg_fail(_debug, "nifty_alignment", symbol, nifty_chg=round(nifty_chg,2))
                    continue

            prev_close = float(daily["Close"].iloc[-2])
            gap_pct    = round((opens[0] - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

            sl   = round(pdh * 0.997, 2) if bullish else round(pdl * 1.003, 2)
            risk = abs(current_price - sl)
            if risk <= 0:
                _dbg_fail(_debug, "invalid_risk", symbol)
                continue
            sign_ = 1 if bullish else -1
            t1    = round(current_price + sign_ * risk * 1.5, 2)
            t2    = round(current_price + sign_ * risk * 3.0, 2)

            # Confidence 0-100: breakout volume (40) + proximity to day extreme
            # (35) + how tight the confirmation candle was (25, tighter = more
            # "calmly absorbed" = higher score)
            vol_s     = min((vol_ratio - MB_VOL_RATIO) / 1.5, 1.0) * 40
            prox_r    = (current_price / day_high) if bullish else (day_low / current_price)
            prox_s    = max(prox_r - (1 - MB_NO_REVERSAL_PCT / 100), 0.0) / (MB_NO_REVERSAL_PCT / 100) * 35
            confirm_s = max(MB_CONFIRM_MAX_RANGE_PCT - confirm_range_pct, 0.0) / MB_CONFIRM_MAX_RANGE_PCT * 25
            conf_score = round(vol_s + prox_s + confirm_s)
            conf_label = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            try:
                ts = today_bars5.index[breakout_bar]
                ts = ts.astimezone(ist) if ts.tzinfo else pytz.utc.localize(ts).astimezone(ist)
                bos_time = ts.strftime("%H:%M")
            except Exception:
                bos_time = datetime.now(ist).strftime("%H:%M")

            _dbg_pass(_debug, symbol, confidence_score=conf_score)
            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          gap_pct,
                "day_high":         round(day_high, 2),
                "day_low":          round(day_low, 2),
                "pdh":              round(pdh, 2),
                "pdl":              round(pdl, 2),
                "key_level":        round(pdh if bullish else pdl, 2),
                "key_label":        "PDH" if bullish else "PDL",
                "volume_ratio":     round(vol_ratio, 2),
                "setup":            "Momentum Breakout Bullish" if bullish else "Momentum Breakdown Bearish",
                "entry":            round(current_price, 2),
                "sl":               sl,
                "sl_pct":           round(risk / current_price * 100, 2),
                "sl_label":         "Below PDH (breakout level)" if bullish else "Above PDL (breakdown level)",
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      round(pdl, 2),
                "supply_zone":      round(pdh, 2),
                "bos_time":         bos_time,
            })

        except Exception:
            _dbg_fail(_debug, "exception", symbol)
            continue

    results.sort(key=lambda x: x["confidence_score"], reverse=True)
    return [r for r in results if r["confidence_score"] >= 40][:5]


# ── Trap Reversal — failed breakdown/breakout, caught at the reversal ──────
#
# Bear Trap (bullish signal): a stock opens within (or just below) yesterday's
# range, closes below PDL — a breakdown that would normally trigger fresh
# shorts — but sellers fail to extend it: a later candle closes back above
# PDL on strong volume, trapping those shorts as buyers take control. Bull
# Trap (bearish signal) is the mirror: a close above PDH fails to hold, price
# closes back below PDH on volume, trapping breakout buyers. This fires right
# at the reversal candle — earlier and lighter than Setup 1, which waits for
# a full BOS to the opposite side of the range.
TRAP_NEAR_LEVEL_PCT   = 1.0   # open can be up to 1% beyond PDL/PDH and still qualify ("near" the level)
TRAP_VOL_RATIO        = 1.9   # reversal candle's volume vs avg volume of bars so far today
TRAP_FRESHNESS_BARS   = 6     # the reversal must be within the last 6 5-min bars (30 min)
TRAP_RECLAIM_MAX_BARS = 12    # the reclaim must happen within this many bars of the trap bar (60 min)


def _screen_trap(direction: str, _debug: dict = None) -> list:
    """Bear Trap (bullish) / Bull Trap (bearish) reversal screener — see module comment above."""
    universe    = _load_nifty500() or _get_fno_universe()
    batch_5m    = _get_5m_batch()
    batch_daily = _get_daily_batch()
    ist         = pytz.timezone("Asia/Kolkata")
    today_date  = datetime.now(ist).date()
    bullish     = direction == "bullish"   # bullish => Bear Trap; bearish => Bull Trap

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
            intra5 = _get_ticker_df(batch_5m, symbol)
            daily  = _get_ticker_df(batch_daily, symbol)
            if intra5 is None or daily is None or len(daily) < 2:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            try:
                if intra5.index.tz is not None:
                    today_mask5 = intra5.index.tz_convert(ist).date == today_date
                else:
                    today_mask5 = [ts.date() == today_date for ts in intra5.index]
                today_bars5 = intra5[today_mask5]
            except Exception:
                today_bars5 = intra5.iloc[:0]

            if len(today_bars5) < 3:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            pdh = float(daily["High"].iloc[-2])
            pdl = float(daily["Low"].iloc[-2])
            if pdh <= 0 or pdl <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue

            opens  = today_bars5["Open"].tolist()
            highs  = today_bars5["High"].tolist()
            lows   = today_bars5["Low"].tolist()
            closes = today_bars5["Close"].tolist()
            vols   = today_bars5["Volume"].tolist()
            n_bars = len(closes)

            # Opened within range, or near the level the trap forms around
            if bullish:
                if opens[0] < pdl * (1 - TRAP_NEAR_LEVEL_PCT / 100) or opens[0] > pdh:
                    _dbg_fail(_debug, "open_not_near_level", symbol, open=round(opens[0],2), pdl=round(pdl,2))
                    continue
            else:
                if opens[0] > pdh * (1 + TRAP_NEAR_LEVEL_PCT / 100) or opens[0] < pdl:
                    _dbg_fail(_debug, "open_not_near_level", symbol, open=round(opens[0],2), pdh=round(pdh,2))
                    continue

            # Find the trap bar: first close beyond the level (the breakdown/breakout)
            trap_bar = -1
            for i in range(0, n_bars - 1):
                broke = (closes[i] < pdl) if bullish else (closes[i] > pdh)
                if broke:
                    trap_bar = i
                    break
            if trap_bar < 0:
                _dbg_fail(_debug, "no_breakdown_or_breakout_yet", symbol)
                continue

            # Find the reversal bar: first LATER close back on the right side
            # of the level, within TRAP_RECLAIM_MAX_BARS of the trap bar
            reversal_bar = -1
            search_end = min(n_bars, trap_bar + 1 + TRAP_RECLAIM_MAX_BARS)
            for j in range(trap_bar + 1, search_end):
                reclaimed = (closes[j] > pdl) if bullish else (closes[j] < pdh)
                if reclaimed:
                    reversal_bar = j
                    break
            if reversal_bar < 0:
                _dbg_fail(_debug, "no_reversal_yet", symbol)
                continue

            # Reversal candle must be in the right direction and on volume —
            # confirms buyers/sellers actually stepped in, not a fluke close
            rev_open  = opens[reversal_bar]
            rev_close = closes[reversal_bar]
            if bullish and rev_close <= rev_open:
                _dbg_fail(_debug, "reversal_wrong_direction", symbol)
                continue
            if not bullish and rev_close >= rev_open:
                _dbg_fail(_debug, "reversal_wrong_direction", symbol)
                continue

            avg_vol_before = sum(vols[:reversal_bar]) / reversal_bar
            if avg_vol_before <= 0:
                _dbg_fail(_debug, "no_data", symbol)
                continue
            vol_ratio = vols[reversal_bar] / avg_vol_before
            if vol_ratio < TRAP_VOL_RATIO:
                _dbg_fail(_debug, "reversal_volume", symbol, vol_ratio=round(vol_ratio,2), needed=TRAP_VOL_RATIO)
                continue

            # Freshness: the reversal must be recent
            bars_since_reversal = n_bars - 1 - reversal_bar
            if bars_since_reversal > TRAP_FRESHNESS_BARS:
                _dbg_fail(_debug, "reversal_too_stale", symbol, bars_ago=bars_since_reversal, allowed=TRAP_FRESHNESS_BARS)
                continue

            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else closes[-1]
            else:
                current_price = closes[-1]

            if current_price < 100:
                _dbg_fail(_debug, "price_below_100", symbol, price=round(current_price,2))
                continue

            # Validity: current price must still be on the reclaimed side of
            # the level — if it's broken back through, the trap failed again
            if bullish:
                if current_price < pdl:
                    _dbg_fail(_debug, "trap_failed_again", symbol, price=round(current_price,2), pdl=round(pdl,2))
                    continue
                if nifty_chg < -1.0:
                    _dbg_fail(_debug, "nifty_alignment", symbol, nifty_chg=round(nifty_chg,2))
                    continue
            else:
                if current_price > pdh:
                    _dbg_fail(_debug, "trap_failed_again", symbol, price=round(current_price,2), pdh=round(pdh,2))
                    continue
                if nifty_chg > 1.0:
                    _dbg_fail(_debug, "nifty_alignment", symbol, nifty_chg=round(nifty_chg,2))
                    continue

            day_high   = max(highs)
            day_low    = min(lows)
            prev_close = float(daily["Close"].iloc[-2])
            gap_pct    = round((opens[0] - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

            sl   = round(day_low * 0.997, 2) if bullish else round(day_high * 1.003, 2)
            risk = abs(current_price - sl)
            if risk <= 0:
                _dbg_fail(_debug, "invalid_risk", symbol)
                continue
            sign_ = 1 if bullish else -1
            t1    = round(current_price + sign_ * risk * 1.5, 2)
            t2    = round(current_price + sign_ * risk * 3.0, 2)

            # Confidence 0-100: reversal volume (40) + how decisively price is
            # back on the right side of the level (35) + freshness — more
            # recent reversals score higher (25)
            vol_s       = min((vol_ratio - TRAP_VOL_RATIO) / 1.5, 1.0) * 40
            reclaim_pct = (abs(current_price - pdl) / pdl * 100) if bullish else (abs(pdh - current_price) / pdh * 100)
            reclaim_s   = min(reclaim_pct / 1.0, 1.0) * 35
            fresh_s     = max(0.0, (TRAP_FRESHNESS_BARS - bars_since_reversal) / TRAP_FRESHNESS_BARS) * 25
            conf_score  = round(vol_s + reclaim_s + fresh_s)
            conf_label  = "STRONG" if conf_score >= 65 else "GOOD" if conf_score >= 40 else "WATCH"

            try:
                ts = today_bars5.index[reversal_bar]
                ts = ts.astimezone(ist) if ts.tzinfo else pytz.utc.localize(ts).astimezone(ist)
                bos_time = ts.strftime("%H:%M")
            except Exception:
                bos_time = datetime.now(ist).strftime("%H:%M")

            _dbg_pass(_debug, symbol, confidence_score=conf_score)
            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          gap_pct,
                "day_high":         round(day_high, 2),
                "day_low":          round(day_low, 2),
                "pdh":              round(pdh, 2),
                "pdl":              round(pdl, 2),
                "key_level":        round(pdl if bullish else pdh, 2),
                "key_label":        "PDL" if bullish else "PDH",
                "volume_ratio":     round(vol_ratio, 2),
                "setup":            "Bear Trap Reversal" if bullish else "Bull Trap Reversal",
                "entry":            round(current_price, 2),
                "sl":               sl,
                "sl_pct":           round(risk / current_price * 100, 2),
                "sl_label":         "Below breakdown low" if bullish else "Above breakout high",
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
                "confidence_score": conf_score,
                "confidence_label": conf_label,
                "demand_zone":      round(pdl, 2),
                "supply_zone":      round(pdh, 2),
                "bos_time":         bos_time,
            })

        except Exception:
            _dbg_fail(_debug, "exception", symbol)
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


# ── Global indices (overnight / pre-market cues) ───────────────────
# Delayed Yahoo Finance data — Upstox doesn't cover foreign exchanges, so
# unlike NIFTY/SENSEX/BANK NIFTY there's no live source for these at all.
# GIFT Nifty is intentionally NOT included: there's no reliable Yahoo
# ticker for it (it would have to come from a different source later).
_GLOBAL_INDEX_YF = {
    "Dow Futures":        "YM=F",
    "Nikkei 225":         "^N225",
    "Hang Seng":          "^HSI",
    "Shanghai Composite": "000001.SS",
}

def _fetch_global_indices():
    result = {}
    for name, sym in _GLOBAL_INDEX_YF.items():
        try:
            d = yf.Ticker(sym).history(interval="1d", period="5d")
            if len(d) >= 2:
                prev = float(d["Close"].iloc[-2])
                curr = float(d["Close"].iloc[-1])
                # Yahoo occasionally returns a NaN close for a missing/partial
                # day — math.isnan() check prevents a literal NaN token from
                # going out in the JSON response, which isn't valid per the
                # JSON spec and breaks the browser's res.json() parse
                if math.isnan(prev) or math.isnan(curr) or prev <= 0:
                    continue
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
    for k in ("exh_short", "pdh_breakout_bull", "orb_bull", "orb_bear",
              "oi_opt_bull", "oi_opt_bear", "zone_demand", "zone_supply", "mb_bull", "mb_bear",
              "trap_bull", "trap_bear", "live_quotes", "15m_batch", "5m_batch", "ticker",
              "oi_buildup", "market"):
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
    for k in ("exh_short", "pdh_breakout_bull", "orb_bull", "orb_bear",
              "oi_opt_bull", "oi_opt_bear", "zone_demand", "zone_supply", "mb_bull", "mb_bear",
              "trap_bull", "trap_bear", "live_quotes", "15m_batch", "5m_batch", "ticker",
              "oi_buildup", "market"):
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

def _orb_ready() -> bool:
    """ORB only needs the opening 5-min candle plus one more completed 5-min
    bar to detect a breakout, i.e. ~9:25 AM IST — unlike the 15-min-bar
    screeners, it must NOT share _smc_ready()'s 9:45 gate, or a breakout that
    fires on the very first post-open candle sits undetected for ~20 min."""
    return datetime.now(pytz.timezone("Asia/Kolkata")).time() >= _dtime(9, 25)

def _momentum_ready() -> bool:
    """Momentum Breakout needs the open bar + a breakout bar + a confirm bar,
    i.e. 3 completed 5-min bars — ready by ~9:30 AM IST."""
    return datetime.now(pytz.timezone("Asia/Kolkata")).time() >= _dtime(9, 30)

def _trap_ready() -> bool:
    """Trap Reversal needs the open bar + a trap bar + a reversal bar,
    i.e. 3 completed 5-min bars — ready by ~9:30 AM IST."""
    return datetime.now(pytz.timezone("Asia/Kolkata")).time() >= _dtime(9, 30)

@app.route("/api/admin/revive-history", methods=["POST"])
def admin_revive_history():
    """One-off recovery tool: re-insert signals into _smc_history that were lost
    to a cold-start/restart wipe before Redis persistence caught them, using
    data the user salvaged from a downloaded CSV. Inserted as is_active=False
    (they're historical for today, not re-validated against the live screener)
    so they reappear as greyed-out 'Signal gone' rows instead of vanishing.
    POST body: {"secret": "...", "records": [{"setup":1,"direction":"bullish","symbol":"ATGL","detected_at":"16:30","signal":{...fields...}}, ...]}"""
    body = flask_req.get_json(force=True, silent=True) or {}
    if not SET_TOKEN_SECRET or body.get("secret") != SET_TOKEN_SECRET:
        return jsonify({"error": "Unauthorized"}), 403
    ist = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    inserted = []
    for rec in body.get("records", []):
        setup     = int(rec["setup"])
        direction = rec["direction"]
        symbol    = rec["symbol"]
        key = (setup, direction, today_str)
        if key not in _smc_history:
            _smc_history[key] = {}
        _smc_history[key][symbol] = {
            **rec["signal"],
            "symbol":        symbol,
            "detected_at":   rec["detected_at"],
            "first_shown_at": rec.get("first_shown_at", rec["detected_at"]),
            "is_active":     False,
        }
        inserted.append(symbol)
    _persist_history(today_str)
    return jsonify({"inserted": inserted, "date": today_str})


@app.route("/api/exhaustion/short")
def api_exhaustion_short():
    if not _smc_ready():
        return jsonify([])
    active = _cached("exh_short", _screen_exhaustion_short, ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 2, "bearish"))

@app.route("/api/pdh-breakout/bullish")
def api_pdh_breakout_bull():
    if not _smc_ready():
        return jsonify([])
    active = _cached("pdh_breakout_bull", _screen_pdh_trend, ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 3, "bullish"))

@app.route("/api/orb/bullish")
def api_orb_bull():
    if not _orb_ready():
        return jsonify([])
    active = _cached("orb_bull", _screen_orb, "bullish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 4, "bullish"))

@app.route("/api/orb/bearish")
def api_orb_bear():
    if not _orb_ready():
        return jsonify([])
    active = _cached("orb_bear", _screen_orb, "bearish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 4, "bearish"))

@app.route("/api/momentum-breakout/bullish")
def api_momentum_breakout_bull():
    if not _momentum_ready():
        return jsonify([])
    active = _cached("mb_bull", _screen_momentum_breakout, "bullish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 7, "bullish"))

@app.route("/api/momentum-breakout/bearish")
def api_momentum_breakout_bear():
    if not _momentum_ready():
        return jsonify([])
    active = _cached("mb_bear", _screen_momentum_breakout, "bearish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 7, "bearish"))

@app.route("/api/trap/bullish")
def api_trap_bull():
    """Bear Trap reversal — bullish signal."""
    if not _trap_ready():
        return jsonify([])
    active = _cached("trap_bull", _screen_trap, "bullish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 8, "bullish"))

@app.route("/api/trap/bearish")
def api_trap_bear():
    """Bull Trap reversal — bearish signal."""
    if not _trap_ready():
        return jsonify([])
    active = _cached("trap_bear", _screen_trap, "bearish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 8, "bearish"))

@app.route("/api/oi-options/bullish")
def api_oi_options_bull():
    active = _cached("oi_opt_bull", _screen_oi_options, "bullish", ttl=OI_TTL)
    return jsonify(_merge_with_history(active, 5, "bullish"))

@app.route("/api/oi-options/bearish")
def api_oi_options_bear():
    active = _cached("oi_opt_bear", _screen_oi_options, "bearish", ttl=OI_TTL)
    return jsonify(_merge_with_history(active, 5, "bearish"))

@app.route("/api/zone/demand")
def api_zone_demand():
    if not _smc_ready():
        return jsonify([])
    active = _cached("zone_demand", _screen_demand_supply_zone, "bullish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 6, "bullish"))

@app.route("/api/zone/supply")
def api_zone_supply():
    if not _smc_ready():
        return jsonify([])
    active = _cached("zone_supply", _screen_demand_supply_zone, "bearish", ttl=SCREEN_TTL)
    return jsonify(_merge_with_history(active, 6, "bearish"))


# ── Per-panel diagnostics ────────────────────────────────────────────
# Each panel runs the SAME screener function used to generate its live
# signals, with an extra _debug accumulator — so "why didn't X show up"
# always reflects the real, current filtering logic, not a guess.
_PANEL_DIAGNOSE = {
    "exh-short":    lambda dbg: _screen_exhaustion_short(_debug=dbg),
    "pdh-breakout": lambda dbg: _screen_pdh_trend(_debug=dbg),
    "orb-bull":     lambda dbg: _screen_orb("bullish", _debug=dbg),
    "orb-bear":     lambda dbg: _screen_orb("bearish", _debug=dbg),
    "zone-demand":  lambda dbg: _screen_demand_supply_zone("bullish", _debug=dbg),
    "zone-supply":  lambda dbg: _screen_demand_supply_zone("bearish", _debug=dbg),
    "mb-bull":      lambda dbg: _screen_momentum_breakout("bullish", _debug=dbg),
    "mb-bear":      lambda dbg: _screen_momentum_breakout("bearish", _debug=dbg),
    "trap-bull":    lambda dbg: _screen_trap("bullish", _debug=dbg),
    "trap-bear":    lambda dbg: _screen_trap("bearish", _debug=dbg),
    "oi-bull":      lambda dbg: _screen_oi_options("bullish", _debug=dbg),
    "oi-bear":      lambda dbg: _screen_oi_options("bearish", _debug=dbg),
}

@app.route("/api/debug/panel/<panel_key>")
def debug_panel(panel_key):
    fn = _PANEL_DIAGNOSE.get(panel_key)
    if not fn:
        return jsonify({"error": f"unknown panel '{panel_key}'"}), 404
    dbg = {"funnel": {}, "samples": {}, "blocked": None}
    try:
        results = fn(dbg)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    passed = dbg["funnel"].get("PASSED", 0)
    funnel_sorted = sorted(
        ({"stage": k, "count": v} for k, v in dbg["funnel"].items() if k != "PASSED"),
        key=lambda x: x["count"], reverse=True,
    )
    return jsonify({
        "panel":     panel_key,
        "blocked":   dbg["blocked"],
        "passed":    passed,
        "shown":     len(results),
        "funnel":    funnel_sorted,
        "samples":   dbg["samples"],
    })

@app.route("/api/signals/all-today.csv")
def signals_all_today_csv():
    """Every signal captured today across all 8 setups/16 panels — both
    still-active and inactive ('Signal gone') ones — sourced from the
    server-side _smc_history store (Redis-persisted, survives restarts and
    browser refreshes), not from whatever happens to be loaded in the
    requesting browser tab. This is what 'Download All Signals' should
    mean: the full day's capture, not just what's currently on screen."""
    ist       = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    headers = ["Panel","Symbol","Setup","Active","Signal Detected At (IST)","Signal Shown At (IST)",
               "Price","Gap%","PDH","PDL",
               "Key Level","Key Label","Vol Ratio","Score","Label",
               "Demand Zone","Supply Zone","Entry","SL","SL%","T1","T2","R:R"]
    rows = [
        "# Samvex LLP — All Signals Captured Today (Active + Inactive)",
        f"# Date: {today_str}  |  Generated: {datetime.now(ist).strftime('%H:%M IST')}",
        "",
        ",".join(headers),
    ]
    total = 0
    for (setup, direction), label in _PANEL_LABELS.items():
        key      = (setup, direction, today_str)
        history  = _smc_history.get(key, {})
        signals  = sorted(history.values(), key=lambda s: s.get("detected_at") or "", reverse=True)
        for s in signals:
            total += 1
            rows.append(",".join(str(x) for x in [
                f'"{label}"',
                s.get("symbol", ""), s.get("setup", ""),
                "Yes" if s.get("is_active") else "No",
                s.get("detected_at", ""),
                s.get("first_shown_at", s.get("detected_at", "")),
                s.get("price", ""), s.get("gap_pct", ""),
                s.get("pdh", ""), s.get("pdl", ""),
                s.get("key_level", ""), s.get("key_label", ""),
                s.get("volume_ratio", ""),
                s.get("confidence_score", ""), s.get("confidence_label", ""),
                s.get("demand_zone", ""), s.get("supply_zone", ""),
                s.get("entry", ""), s.get("sl", ""), s.get("sl_pct", ""),
                s.get("t1", ""), s.get("t2", ""), s.get("risk_reward", ""),
            ]))
    if total == 0:
        rows.append("# No signals captured yet today")
    return Response(
        "\n".join(rows),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=samvex_all_signals_{today_str}.csv"},
    )


@app.route("/api/signals/all-today.json")
def signals_all_today_json():
    """JSON twin of /api/signals/all-today.csv — every signal captured today
    across all 16 panels, active and inactive, sourced from _smc_history.
    Used by the GitHub Actions daily analysis script, which previously hit
    the live per-panel endpoints (only currently-active signals, 6/16 panels)
    and badly undercounted the day's actual signal volume."""
    ist       = pytz.timezone("Asia/Kolkata")
    today_str = datetime.now(ist).strftime("%Y-%m-%d")
    out = []
    for (setup, direction), label in _PANEL_LABELS.items():
        key     = (setup, direction, today_str)
        history = _smc_history.get(key, {})
        for s in history.values():
            row = dict(s)
            row["_panel"]     = label
            row["_direction"] = direction
            row["_setup_num"] = setup
            out.append(row)
    out.sort(key=lambda s: s.get("detected_at") or "", reverse=True)
    return jsonify({"date": today_str, "total": len(out), "signals": out})


@app.route("/api/signals/today")
def signals_today_json():
    """Today's signals, read from _signal_store. Superseded by
    /api/signals/all-today.json for full-day, all-panel coverage; kept for
    backward compatibility."""
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
        "note":   "Signals from signal store.",
    })


@app.route("/api/signals/today.csv")
def signals_today_csv():
    """Download today's signal-store signals as CSV. Superseded by
    /api/signals/all-today.csv for full-day, all-panel coverage; kept for
    backward compatibility."""
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

@app.route("/api/global-indices")
def global_indices():
    return jsonify(_cached("global_indices", _fetch_global_indices, ttl=CACHE_TTL))

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
    expires_in_min = max(0, int((_upstox_token["expires_at"] - time.time()) / 60)) if _is_live() else 0
    return jsonify({
        "time":           now.strftime("%H:%M:%S IST"),
        "market_open":    is_open,
        "date":           now.strftime("%d-%b-%Y"),
        "is_live":        _is_live(),
        "data_source":    "upstox_live" if _is_live() else "yahoo_delayed",
        "expires_in_min": expires_in_min,
    })

@app.route("/api/chart/<symbol>")
def chart_candles(symbol):
    """Live intraday 5-min candles from Upstox for the chart modal.
    Upstox v2 intraday endpoint only supports 1minute and 30minute intervals
    natively — there's no 5minute option — so we fetch 1-minute candles and
    resample them into 5-min OHLCV bars ourselves.
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
        return jsonify({"symbol": base, "interval": "5m", "candles": cached["candles"]})

    try:
        encoded_key = ikey.replace("|", "%7C")
        headers     = _upstox_headers()

        r = _http.get(
            f"{UPSTOX_BASE}/historical-candle/intraday/{encoded_key}/1minute",
            headers=headers, timeout=15,
        )

        if r.status_code == 400:
            # Market closed — return stale cache (any date) if available, else empty
            if cached.get("candles"):
                return jsonify({"symbol": base, "interval": "5m", "candles": cached["candles"]})
            return jsonify({"symbol": base, "interval": "5m", "candles": []})

        if r.status_code != 200:
            return jsonify({
                "error":   "upstox_api_error",
                "message": f"Upstox returned HTTP {r.status_code}.",
            }), 502

        raw  = r.json().get("data", {}).get("candles", [])
        rows = []
        for c in raw:
            try:
                rows.append({
                    "ts": pd.Timestamp(c[0]), "open": float(c[1]), "high": float(c[2]),
                    "low": float(c[3]), "close": float(c[4]), "volume": int(c[5]),
                })
            except Exception:
                continue

        candles = []
        if rows:
            df  = pd.DataFrame(rows).set_index("ts").sort_index()
            agg = df.resample("5min").agg({
                "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
            }).dropna()
            candles = [
                {
                    "time":   int(idx.timestamp()),
                    "open":   round(float(row["open"]), 2),
                    "high":   round(float(row["high"]), 2),
                    "low":    round(float(row["low"]), 2),
                    "close":  round(float(row["close"]), 2),
                    "volume": int(row["volume"]),
                }
                for idx, row in agg.iterrows()
            ]

        # Cache so after-hours requests are served without hitting Upstox
        _candle_cache[ikey] = {"date": today_str, "candles": candles}
        return jsonify({"symbol": base, "interval": "5m", "candles": candles})

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


@app.route("/api/journal", methods=["GET"])
def journal_list():
    entries = _load_journal()
    entries.sort(key=lambda e: (e.get("date", ""), e.get("created_at", "")), reverse=True)
    return jsonify(entries)


@app.route("/api/journal", methods=["POST"])
def journal_create():
    data  = flask_req.get_json(force=True, silent=True) or {}
    if not (data.get("symbol") or "").strip():
        return jsonify({"error": "symbol is required"}), 400

    ist   = pytz.timezone("Asia/Kolkata")
    now   = datetime.now(ist).isoformat()
    entry = _sanitize_journal_entry(data)
    entry["id"]         = str(uuid.uuid4())
    entry["created_at"] = now
    entry["updated_at"] = now

    entries = _load_journal()
    entries.append(entry)
    _save_journal(entries)
    return jsonify(entry), 201


@app.route("/api/journal/<entry_id>", methods=["PUT"])
def journal_update(entry_id):
    data    = flask_req.get_json(force=True, silent=True) or {}
    entries = _load_journal()
    idx     = next((i for i, e in enumerate(entries) if e.get("id") == entry_id), -1)
    if idx < 0:
        return jsonify({"error": "not found"}), 404

    updated = _sanitize_journal_entry(data)
    updated["id"]         = entry_id
    updated["created_at"] = entries[idx].get("created_at", "")
    updated["updated_at"] = datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()

    entries[idx] = updated
    _save_journal(entries)
    return jsonify(updated)


@app.route("/api/journal/<entry_id>", methods=["DELETE"])
def journal_delete(entry_id):
    entries     = _load_journal()
    new_entries = [e for e in entries if e.get("id") != entry_id]
    if len(new_entries) == len(entries):
        return jsonify({"error": "not found"}), 404
    _save_journal(new_entries)
    return jsonify({"deleted": entry_id})


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
            (2, "bearish"): "Exhaustion Short — huge previous-day rally pulling back from day high on an impulsive red 5-min candle",
            (3, "bullish"): "PDH Breakout — close above PDH, trending above 200 EMA(15m), daily RSI >= 60, strong volume",
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
    setup_key = flask_req.args.get("setup", "exh_short")
    setup_map = {
        "exh_short": (2, "bearish"),
        "pdh_breakout_bull": (3, "bullish"),
        "orb_bull": (4, "bullish"), "orb_bear": (4, "bearish"),
        "oi_opt_bull": (5, "bullish"), "oi_opt_bear": (5, "bearish"),
        "zone_demand": (6, "bullish"), "zone_supply": (6, "bearish"),
        "mb_bull": (7, "bullish"), "mb_bear": (7, "bearish"),
        "trap_bull": (8, "bullish"), "trap_bear": (8, "bearish"),
    }
    if setup_key not in setup_map:
        return jsonify({"error": "invalid setup"}), 400
    setup_num, direction = setup_map[setup_key]
    cached  = _cache.get(setup_key)
    if cached:
        results = cached[0]
    elif setup_key == "exh_short":
        results = _screen_exhaustion_short()
    elif setup_key == "pdh_breakout_bull":
        results = _screen_pdh_trend()
    elif setup_key in ("orb_bull", "orb_bear"):
        results = _screen_orb(direction)
    elif setup_key in ("oi_opt_bull", "oi_opt_bear"):
        results = _screen_oi_options(direction)
    elif setup_key in ("zone_demand", "zone_supply"):
        results = _screen_demand_supply_zone(direction)
    elif setup_key in ("mb_bull", "mb_bear"):
        results = _screen_momentum_breakout(direction)
    elif setup_key in ("trap_bull", "trap_bear"):
        results = _screen_trap(direction)
    else:
        return jsonify({"error": "invalid setup"}), 400
    if not results:
        return jsonify({"explanations": [], "enabled": True})
    cache_key    = f"ai_explain_{setup_key}"
    explanations = _cached(cache_key, _generate_setup_explanations, setup_num, direction, results,
                           ttl=INSIGHTS_TTL)
    return jsonify({"explanations": explanations, "enabled": True})


if __name__ == "__main__":
    print("Starting Samvex Dashboard API on http://localhost:5050")
    app.run(debug=True, port=5050, use_reloader=False)
