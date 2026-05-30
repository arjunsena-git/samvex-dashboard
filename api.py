from flask import Flask, jsonify, redirect, request as flask_req
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from datetime import datetime
import pytz
import time
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import io
import gzip
import requests as _http

app = Flask(__name__)
CORS(app)

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
    "LAURUSLABS.NS", "PFIZER.NS", "SANOFI.NS",
    # IT
    "TECHM.NS", "MPHASIS.NS", "COFORGE.NS", "PERSISTENT.NS", "LTTS.NS",
    "KPITTECH.NS", "CYIENT.NS", "OFSS.NS", "WIPRO.NS", "SUNPHARMA.NS",
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
# Fetched fresh each trading day from niftyindices.com.
# Falls back to FNO_STOCKS if the fetch fails.
NIFTY500_CSV_URL = "https://www.niftyindices.com/IndexConstituents/ind_nifty500list.csv"
_nifty500_cache: dict = {"symbols": [], "date": ""}


def _load_nifty500() -> list:
    """Return current Nifty 500 symbol list with .NS suffix. Cached for the day."""
    ist   = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).strftime("%Y-%m-%d")
    if _nifty500_cache["symbols"] and _nifty500_cache["date"] == today:
        return _nifty500_cache["symbols"]
    try:
        r = _http.get(
            NIFTY500_CSV_URL, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                                   "Chrome/120.0.0.0 Safari/537.36"},
        )
        df  = pd.read_csv(io.StringIO(r.text))
        col = next((c for c in df.columns if "symbol" in c.lower()), None)
        if not col:
            raise ValueError(f"No symbol column. Cols: {list(df.columns)}")
        syms = [
            f"{str(s).strip().upper()}.NS"
            for s in df[col].dropna()
            if str(s).strip() and str(s).strip().upper() not in ("SYMBOL", "NAN", "")
        ]
        if len(syms) < 100:
            raise ValueError(f"Only {len(syms)} symbols parsed")
        _nifty500_cache["symbols"] = syms
        _nifty500_cache["date"]    = today
        print(f"[Nifty500] {len(syms)} symbols loaded for {today}")
    except Exception as e:
        print(f"[Nifty500] Load failed: {e} — using FNO fallback ({len(FNO_STOCKS)} stocks)")
        if not _nifty500_cache["symbols"]:
            _nifty500_cache["symbols"] = FNO_STOCKS
    return _nifty500_cache["symbols"]

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


# ── Startup: always pre-warm everything auth-independent ──────────
# Load instrument maps, Nifty 500 list, and daily Yahoo batch in background
# so the first screener request doesn't have to wait 30-60s.
threading.Thread(target=lambda: _load_instrument_map(), daemon=True).start()
threading.Thread(target=lambda: _load_futures_map(),    daemon=True).start()
threading.Thread(target=lambda: _load_nifty500(),       daemon=True).start()
threading.Thread(target=lambda: _get_daily_batch(),     daemon=True).start()

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




# ── Yahoo Finance batch (daily — Nifty 500 universe) ──────────────
def _fetch_chunked(interval: str, period: str, chunk_size: int = 100) -> dict:
    """Download universe in parallel chunks; return {symbol: df}.

    Each chunk is 100 stocks → ~20 MB peak per worker.
    2 parallel workers = ~40 MB peak, well within 512 MB Starter limit.
    Sequential was 5 × 15 s = 75 s; parallel is ceil(5/2) × 15 s ≈ 30–35 s.
    """
    universe = _load_nifty500() or FNO_STOCKS
    chunks   = [universe[i:i + chunk_size] for i in range(0, len(universe), chunk_size)]

    def _one_chunk(idx_chunk):
        idx, chunk = idx_chunk
        chunk_results = {}
        try:
            raw = yf.download(
                tickers=" ".join(chunk),
                interval=interval, period=period,
                group_by="ticker", auto_adjust=False,
                threads=True, progress=False,
            )
            if raw is not None and not raw.empty:
                for sym in chunk:
                    try:
                        if isinstance(raw.columns, pd.MultiIndex):
                            df = raw[sym].dropna(how="all")
                        else:
                            df = raw.dropna(how="all")
                        if len(df) >= 1:
                            chunk_results[sym] = df
                    except Exception:
                        pass
            del raw
        except Exception as e:
            print(f"[Batch] Chunk {idx + 1}/{len(chunks)} error ({interval}): {e}")
        return chunk_results

    results: dict = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = {ex.submit(_one_chunk, (i, c)): i for i, c in enumerate(chunks)}
        for fut in as_completed(futures):
            try:
                results.update(fut.result())
            except Exception:
                pass
    print(f"[Batch] {interval}/{period} — {len(results)}/{len(universe)} symbols loaded")
    return results


def _fetch_daily_only() -> dict:
    return _fetch_chunked("1d", "15d")


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


# ── Yahoo Finance 15-min intraday batch (for first-candle screener) ─
SCREEN_TTL = 300   # 5-min screener result cache

def _fetch_intraday_15m() -> dict:
    """Batch 15-min bars for Nifty 500 universe, chunked to stay within 512 MB."""
    return _fetch_chunked("15m", "1d")


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


# ── Screener: Setup 1 (Directional Trend) + Setup 2 (Reversal/Trap) ─
#
# Both setups are based on the FIRST 15-MINUTE CANDLE (9:15–9:30 AM IST).
# Signals fire once the candle completes and remain valid all day.
#
# Common filters (all setups):
#   • First 15-min candle full range (High–Low) ≤ 1% of open   [answer 2B]
#   • Volume paced to full day ≥ 1.5× previous day             [answer 1A]
#   • Current price > ₹100
#   • Stock in Nifty 500 universe                               [answer 5C]
#
# Setup 1 — Directional Trend (Kumar sir):
#   Bullish : first 15-min candle closes ABOVE Previous Day High (PDH)
#   Bearish : first 15-min candle closes BELOW Previous Day Low  (PDL)
#
# Setup 2 — Reversal / Trap (Aravind):
#   Bullish (Bear Trap)  : opens near/below PDL (within 1% above PDL [answer 3B]),
#                          gap down ≤ 2%, first candle closes ABOVE PDL as green candle
#   Bearish (Bull Trap)  : opens near/above PDH (within 1% below PDH),
#                          gap up ≤ 2%, first candle closes BELOW PDH as red candle


def _screen_new(setup: int, direction: str) -> list:
    """
    Unified screener for Setup 1 (Directional Trend) and Setup 2 (Reversal/Trap).
    Uses the first 15-min candle (9:15–9:30 AM). Results valid all day once fired.
    """
    universe     = _load_nifty500() or FNO_STOCKS
    batch_15m    = _get_15m_batch()
    batch_daily  = _get_daily_batch()
    bullish      = direction == "bullish"

    # Live prices from Upstox (for entry price in trade plan)
    live_quotes = None
    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)

    results = []

    for symbol in universe:
        try:
            # ── Data fetch ────────────────────────────────────────
            intra = _get_ticker_df(batch_15m,   symbol)
            daily = _get_ticker_df(batch_daily, symbol)

            # Need at least 1 intraday 15m bar and 2 daily bars (yesterday + today)
            if intra is None or len(intra) < 1 or daily is None or len(daily) < 2:
                continue

            # ── Previous-day reference levels ─────────────────────
            pdh        = float(daily["High"].iloc[-2])
            pdl        = float(daily["Low"].iloc[-2])
            prev_close = float(daily["Close"].iloc[-2])
            prev_vol   = float(daily["Volume"].iloc[-2])

            if pdh <= 0 or pdl <= 0 or prev_close <= 0 or prev_vol <= 0:
                continue

            # ── First 15-min candle (9:15–9:30 AM) ───────────────
            c_open  = float(intra["Open"].iloc[0])
            c_high  = float(intra["High"].iloc[0])
            c_low   = float(intra["Low"].iloc[0])
            c_close = float(intra["Close"].iloc[0])
            c_vol   = float(intra["Volume"].iloc[0])

            if c_open <= 0 or c_vol <= 0:
                continue

            # ── Current price (Upstox live if available) ──────────
            if live_quotes:
                q  = live_quotes.get(symbol)
                lp = float(q.get("last_price", 0) or 0) if q else 0
                current_price = lp if lp > 0 else float(intra["Close"].iloc[-1])
            else:
                current_price = float(intra["Close"].iloc[-1])

            # ── Price gate: > ₹100 ────────────────────────────────
            if current_price < 100:
                continue

            # ── Candle full range (High–Low) ≤ 1% of open ─────────
            candle_range_pct = (c_high - c_low) / c_open * 100
            if candle_range_pct > 1.0:
                continue

            # ── Volume: 15-min paced to full day ≥ 1.5× prev day ─
            paced_vol = (c_vol / 15.0) * 375.0
            vol_ratio = paced_vol / prev_vol
            if vol_ratio < 1.5:
                continue

            gap_pct = (c_open - prev_close) / prev_close * 100

            # ── Setup-specific filters ────────────────────────────
            if setup == 1:
                # Directional Trend: stock opens INSIDE previous day range,
                # then breaks PDH (bullish) or PDL (bearish) in first 15 min
                if not (pdl < c_open < pdh):
                    continue
                if bullish:
                    if c_close <= pdh:
                        continue
                    sl_level  = pdh
                    sl_label  = "Below PDH"
                    setup_tag = "Trend Breakout"
                else:
                    if c_close >= pdl:
                        continue
                    sl_level  = pdl
                    sl_label  = "Above PDL"
                    setup_tag = "Trend Breakdown"

            else:  # setup == 2
                if bullish:
                    # Bear Trap: opens near/below PDL, gap down ≤ 2%,
                    # first candle recovers above PDL as a green (bullish) candle
                    if not (c_open <= pdl * 1.01        # within 1% above PDL
                            and -2.0 <= gap_pct < 0     # gap down, max 2%
                            and c_close > pdl            # closes above PDL
                            and c_close > c_open):       # green candle
                        continue
                    sl_level  = pdl
                    sl_label  = "Below PDL"
                    setup_tag = "Bear Trap"
                else:
                    # Bull Trap: opens near/above PDH, gap up ≤ 2%,
                    # first candle rejects below PDH as a red (bearish) candle
                    if not (c_open >= pdh * 0.99        # within 1% below PDH
                            and 0 < gap_pct <= 2.0      # gap up, max 2%
                            and c_close < pdh            # closes below PDH
                            and c_close < c_open):       # red candle
                        continue
                    sl_level  = pdh
                    sl_label  = "Above PDH"
                    setup_tag = "Bull Trap"

            # ── Trade plan: structural SL + 1.5R / 3R targets ─────
            # SL is 0.3% beyond the key level (small buffer to avoid noise)
            if bullish:
                sl   = round(sl_level * 0.997, 2)
                risk = current_price - sl
            else:
                sl   = round(sl_level * 1.003, 2)
                risk = sl - current_price

            if risk <= 0:
                continue

            sl_pct = round(risk / current_price * 100, 2)
            sign_  = 1 if bullish else -1
            t1     = round(current_price + sign_ * risk * 1.5, 2)
            t2     = round(current_price + sign_ * risk * 3.0, 2)

            results.append({
                "symbol":           symbol.replace(".NS", ""),
                "price":            round(current_price, 2),
                "gap_pct":          round(gap_pct, 2),
                "candle_range_pct": round(candle_range_pct, 2),
                "volume_ratio":     round(vol_ratio, 2),
                "pdh":              round(pdh, 2),
                "pdl":              round(pdl, 2),
                "c_close":          round(c_close, 2),
                "setup":            setup_tag,
                "entry":            round(current_price, 2),
                "sl":               sl,
                "sl_pct":           sl_pct,
                "sl_label":         sl_label,
                "t1":               t1,
                "t2":               t2,
                "risk_reward":      1.5,
            })

        except Exception:
            continue

    results.sort(key=lambda x: x["volume_ratio"], reverse=True)
    return results[:10]


def _screen_result(symbol, current_price, gap_pct, intraday_move, day_move,
                   paced_vol_ratio, traded_value_cr, adr_pct, setup_type,
                   today_high, today_low, today_open, direction):
    bullish    = direction == "bullish"
    remaining  = max(adr_pct - abs(day_move), 0)
    vol_boost  = 1.0 + min(max(paced_vol_ratio - 1.5, 0) * 0.12, 0.35)
    projected  = round(remaining * vol_boost, 2)
    confidence = "HIGH" if projected >= 2.5 else "MED" if projected >= 1.2 else "LOW"

    # ── Smart liquidity-aware stop loss ────────────────────────────
    # Reference anchors per setup — avoids the obvious retail stop-hunt zones:
    #   Gap Drive : anchor = today_open  (below open = gap-fill confirmed, thesis dead)
    #   Momentum  : anchor = today_high  (30% ADR drop from high = momentum failure)
    #   Trend     : anchor = today_high  (38.2% Fib retrace from high = structure broken)
    # Factor is % of ADR as the buffer from the anchor — ADR-proportional so it
    # scales with the stock's actual volatility, not a fixed rupee or % amount.
    entry         = current_price
    adr_daily_pts = entry * (adr_pct / 100)   # ADR in rupee terms

    if setup_type == "Gap Drive":
        sl_ref    = today_open if today_open > 0 else entry
        sl_factor = 0.20   # 20% of ADR below the open — sweep of open, then reversal
        sl_label  = "Below Open"
    elif setup_type == "Momentum":
        sl_ref    = today_high if today_high > 0 else entry
        sl_factor = 0.30   # 30% of ADR from day high — momentum failure zone
        sl_label  = "30% ADR Below High"
    else:                  # Trend
        sl_ref    = today_high if today_high > 0 else entry
        sl_factor = 0.382  # Fibonacci 38.2% — institutional algo level from the high
        sl_label  = "38.2% Fib Below High"

    if bullish:
        raw_sl  = sl_ref - adr_daily_pts * sl_factor
        # Guard: SL must be ≥0.25% and ≤1.8% below entry
        sl_dist = max(0.0025, min(0.018, (entry - raw_sl) / entry))
        sl      = round(entry * (1 - sl_dist), 2)
        t1      = round(entry * (1 + projected * 0.50 / 100), 2)
        t2      = round(entry * (1 + projected        / 100), 2)
    else:
        raw_sl  = sl_ref + adr_daily_pts * sl_factor
        sl_dist = max(0.0025, min(0.018, (raw_sl - entry) / entry))
        sl      = round(entry * (1 + sl_dist), 2)
        t1      = round(entry * (1 - projected * 0.50 / 100), 2)
        t2      = round(entry * (1 - projected        / 100), 2)

    sl_pct     = round(sl_dist * 100, 2)
    risk_pts   = abs(entry - sl)
    reward_pts = abs(t1    - entry)
    rr         = round(reward_pts / risk_pts, 1) if risk_pts > 0 else 0

    return {
        "symbol":              symbol.replace(".NS", ""),
        "price":               round(current_price, 2),
        "gap_pct":             round(gap_pct, 2),
        "intraday_move":       round(intraday_move, 2),
        "day_move":            round(day_move, 2),
        "volume_ratio":        round(paced_vol_ratio, 2),
        "traded_value_crores": round(traded_value_cr, 2),
        "projected_pct":       projected,
        "adr_pct":             round(adr_pct, 2),
        "confidence":          confidence,
        "aligned":             True,
        "setup_type":          setup_type,
        "entry":               round(entry, 2),
        "sl":                  sl,
        "sl_pct":              sl_pct,
        "sl_label":            sl_label,
        "t1":                  t1,
        "t2":                  t2,
        "risk_reward":         rr,
    }


def _analyze_smart(symbol, quote, daily_batch, direction, elapsed_min):
    """Live Upstox path — 3-phase time-adaptive screener."""
    daily = _get_ticker_df(daily_batch, symbol)
    if daily is None or len(daily) < 2:
        return None
    try:
        ohlc          = quote.get("ohlc") or {}
        today_open    = float(ohlc.get("open",  0) or 0)
        today_high    = float(ohlc.get("high",  0) or 0)
        today_low     = float(ohlc.get("low",   0) or 0)
        current_price = float(quote.get("last_price",       0) or 0)
        today_volume  = float(quote.get("volume",           0) or 0)
        prev_close    = float(quote.get("prev_close_price", 0) or 0)
        prev_day_vol  = float(daily["Volume"].iloc[-2])

        if today_open <= 0 or current_price <= 0 or prev_close <= 0 or prev_day_vol <= 0:
            return None

        gap_pct         = (today_open    - prev_close)    / prev_close    * 100
        intraday_move   = (current_price - today_open)    / today_open    * 100
        day_move        = (current_price - prev_close)    / prev_close    * 100
        traded_value_cr = (today_volume  * current_price) / 1e7
        vol_ratio_abs   = today_volume / prev_day_vol
        paced_vol_ratio = (today_volume / elapsed_min) * 375 / prev_day_vol
        near_high       = today_high > 0 and (current_price / today_high) >= 0.990
        near_low        = today_low  > 0 and (current_price / today_low)  <= 1.010
        adr             = _adr(daily)
        bullish         = direction == "bullish"

        if elapsed_min <= 90:
            gap_ok  = (0.15 < gap_pct <  2.5) if bullish else (-2.5 < gap_pct < -0.15)
            mom_ok  = intraday_move > 0         if bullish else intraday_move < 0
            if not (gap_ok and mom_ok and paced_vol_ratio >= 1.5
                    and traded_value_cr >= 25 and max(adr - abs(day_move), 0) >= 1.0):
                return None
            setup = "Gap Drive"

        elif elapsed_min <= 225:
            move_ok = day_move >=  1.0 if bullish else day_move <= -1.0
            ext_ok  = near_high        if bullish else near_low
            if not (move_ok and ext_ok and vol_ratio_abs >= 0.25 and traded_value_cr >= 15):
                return None
            setup = "Momentum"

        else:
            move_ok = day_move >= 0.8 if bullish else day_move <= -0.8
            ext_ok  = (today_high > 0 and current_price / today_high >= 0.985) if bullish \
                      else (today_low  > 0 and current_price / today_low  <= 1.015)
            if not (move_ok and ext_ok and vol_ratio_abs >= 0.40 and traded_value_cr >= 10):
                return None
            setup = "Trend"

        return _screen_result(symbol, current_price, gap_pct, intraday_move, day_move,
                               paced_vol_ratio, traded_value_cr, adr, setup,
                               today_high, today_low, today_open, direction)
    except Exception as e:
        print(f"[Smart] Error {symbol}: {e}")
        return None


# ── Yahoo Finance analysis (delayed fallback — same 3-phase logic) ─
def _analyze(symbol, intraday_batch, daily_batch, direction, elapsed_min):
    intraday = _get_ticker_df(intraday_batch, symbol)
    daily    = _get_ticker_df(daily_batch,    symbol)
    if intraday is None or daily is None or len(daily) < 2:
        return None
    try:
        prev_close    = float(daily["Close"].iloc[-2])
        prev_day_vol  = float(daily["Volume"].iloc[-2])
        today_open    = float(intraday["Open"].iloc[0])
        current_price = float(intraday["Close"].iloc[-1])
        today_high    = float(intraday["High"].max())
        today_low     = float(intraday["Low"].min())
        today_volume  = float(intraday["Volume"].sum())

        gap_pct         = (today_open    - prev_close)    / prev_close   * 100
        intraday_move   = (current_price - today_open)    / today_open   * 100
        day_move        = (current_price - prev_close)    / prev_close   * 100
        traded_value_cr = (today_volume  * current_price) / 1e7
        vol_ratio_abs   = today_volume / prev_day_vol if prev_day_vol > 0 else 0
        paced_vol_ratio = (today_volume / elapsed_min) * 375 / prev_day_vol if prev_day_vol > 0 else 0
        near_high       = today_high > 0 and (current_price / today_high) >= 0.990
        near_low        = today_low  > 0 and (current_price / today_low)  <= 1.010
        adr             = _adr(daily)
        bullish         = direction == "bullish"

        if elapsed_min <= 90:
            gap_ok  = (0.15 < gap_pct <  2.5) if bullish else (-2.5 < gap_pct < -0.15)
            mom_ok  = intraday_move > 0         if bullish else intraday_move < 0
            if not (gap_ok and mom_ok and paced_vol_ratio >= 1.5
                    and traded_value_cr >= 25 and max(adr - abs(day_move), 0) >= 1.0):
                return None
            setup = "Gap Drive"

        elif elapsed_min <= 225:
            move_ok = day_move >=  1.0 if bullish else day_move <= -1.0
            ext_ok  = near_high        if bullish else near_low
            if not (move_ok and ext_ok and vol_ratio_abs >= 0.25 and traded_value_cr >= 15):
                return None
            setup = "Momentum"

        else:
            move_ok = day_move >= 0.8 if bullish else day_move <= -0.8
            ext_ok  = (today_high > 0 and current_price / today_high >= 0.985) if bullish \
                      else (today_low  > 0 and current_price / today_low  <= 1.015)
            if not (move_ok and ext_ok and vol_ratio_abs >= 0.40 and traded_value_cr >= 10):
                return None
            setup = "Trend"

        return _screen_result(symbol, current_price, gap_pct, intraday_move, day_move,
                               paced_vol_ratio, traded_value_cr, adr, setup,
                               today_high, today_low, today_open, direction)
    except Exception:
        return None


# ── Screener ───────────────────────────────────────────────────────
def _screen(direction):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    open_dt     = now.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed_min = max(5.0, (now - open_dt).total_seconds() / 60)
    results     = []
    ran_live    = False   # did we successfully run the Upstox path?

    if _is_live():
        live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL)
        daily_batch = _get_daily_batch()
        if live_quotes:
            # Upstox quotes available — commit to live path.
            # Do NOT fall back to Yahoo just because 0 stocks qualify the criteria;
            # that would silently show 15-MIN DELAY data on an authenticated session.
            ran_live = True
            if daily_batch is not None:
                for symbol in FNO_STOCKS:
                    q = live_quotes.get(symbol)
                    if q:
                        r = _analyze_smart(symbol, q, daily_batch, direction, elapsed_min)
                        if r:
                            results.append(r)

    if not ran_live:
        # Only use Yahoo Finance when Upstox is not authenticated or returned no quotes
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


# ── Near-miss candidates (relaxed, time-adaptive fallback) ─────────
def _screen_candidates(direction):
    """Top 5 stocks closest to qualifying when strict screener returns nothing."""
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    open_dt     = now.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed_min = max(5.0, (now - open_dt).total_seconds() / 60)
    bullish     = direction == "bullish"

    candidates  = []
    live_quotes = _cached("live_quotes", _fetch_live_quotes, ttl=LIVE_TTL) if _is_live() else None
    daily_batch = _get_daily_batch()

    for symbol in FNO_STOCKS:
        q = live_quotes.get(symbol) if live_quotes else None
        if not q:
            continue
        try:
            ohlc          = q.get("ohlc") or {}
            today_open    = float(ohlc.get("open", 0) or 0)
            today_high    = float(ohlc.get("high", 0) or 0)
            today_low     = float(ohlc.get("low",  0) or 0)
            current_price = float(q.get("last_price",       0) or 0)
            today_volume  = float(q.get("volume",           0) or 0)
            prev_close    = float(q.get("prev_close_price", 0) or 0)

            if today_open <= 0 or current_price <= 0 or prev_close <= 0:
                continue

            gap_pct         = (today_open - prev_close) / prev_close * 100
            day_move        = (current_price - prev_close) / prev_close * 100
            traded_value_cr = (today_volume * current_price) / 1e7
            near_high       = today_high > 0 and (current_price / today_high) >= 0.99
            near_low        = today_low  > 0 and (current_price / today_low)  <= 1.01

            daily    = _get_ticker_df(daily_batch, symbol) if daily_batch is not None else None
            prev_vol = float(daily["Volume"].iloc[-2]) if daily is not None and len(daily) >= 2 else 0
            vol_ratio_abs = today_volume / prev_vol if prev_vol > 0 else 0
            paced_ratio   = (today_volume / elapsed_min) * 375 / prev_vol if prev_vol > 0 else 0

            if elapsed_min <= 90:
                dir_ok   = (gap_pct > 0.05)   if bullish else (gap_pct < -0.05)
                vol_ok   = paced_ratio    >= 1.0
                val_ok   = traded_value_cr >= 15
                extra_ok = (day_move > 0)      if bullish else (day_move < 0)
            elif elapsed_min <= 225:
                dir_ok   = (day_move > 0.2)   if bullish else (day_move < -0.2)
                vol_ok   = vol_ratio_abs  >= 0.12
                val_ok   = traded_value_cr >= 10
                extra_ok = near_high if bullish else near_low
            else:
                dir_ok   = (day_move > 0.2)   if bullish else (day_move < -0.2)
                vol_ok   = vol_ratio_abs  >= 0.25
                val_ok   = traded_value_cr >= 8
                extra_ok = near_high if bullish else near_low

            if not dir_ok or traded_value_cr < 5:
                continue

            score = int(vol_ok) + int(val_ok) + int(extra_ok)
            candidates.append({
                "symbol":       symbol.replace(".NS", ""),
                "price":        round(current_price, 2),
                "gap_pct":      round(gap_pct, 2),
                "day_move":     round(day_move, 2),
                "volume_ratio": round(paced_ratio, 2),
                "value_cr":     round(traded_value_cr, 1),
                "criteria_met": score,
                "vol_ok":       vol_ok,
                "val_ok":       val_ok,
            })
        except Exception:
            continue

    candidates.sort(key=lambda x: (x["criteria_met"], x["volume_ratio"]), reverse=True)
    return candidates[:5]


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
    for k in ("s1_bull", "s1_bear", "s2_bull", "s2_bear",
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
    for k in ("s1_bull", "s1_bear", "s2_bull", "s2_bear",
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


@app.route("/api/setup1/bullish")
def api_s1_bull():
    return jsonify(_cached("s1_bull", _screen_new, 1, "bullish", ttl=SCREEN_TTL))

@app.route("/api/setup1/bearish")
def api_s1_bear():
    return jsonify(_cached("s1_bear", _screen_new, 1, "bearish", ttl=SCREEN_TTL))

@app.route("/api/setup2/bullish")
def api_s2_bull():
    return jsonify(_cached("s2_bull", _screen_new, 2, "bullish", ttl=SCREEN_TTL))

@app.route("/api/setup2/bearish")
def api_s2_bear():
    return jsonify(_cached("s2_bear", _screen_new, 2, "bearish", ttl=SCREEN_TTL))

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
