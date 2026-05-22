from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from datetime import datetime
import pytz
import time
import math
import threading

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

# ── In-memory cache (5-minute TTL) ────────────────────────────────
_cache: dict = {}
_batch_lock = threading.Lock()   # prevents duplicate downloads on simultaneous requests
CACHE_TTL = 300  # seconds


def _cached(key, fn, *args):
    now = time.time()
    if key in _cache:
        data, ts = _cache[key]
        if now - ts < CACHE_TTL:
            return data
    result = fn(*args)
    _cache[key] = (result, now)
    return result


# ── Batch data fetch (single HTTP session for all stocks) ──────────
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


def _get_ticker_df(batch, ticker):
    """Safely extract a single ticker DataFrame from a batch download."""
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
    """
    Answers: "How much more can this stock move today?"

    Logic:
      1. ADR (10-day avg daily range) = total capacity for the day
      2. Subtract what's already been used (gap + intraday move so far)
      3. Remaining capacity x volume conviction x alignment factor
      4. Gate: ADR must be >= 2% (stock must be naturally volatile enough)

    This is time-agnostic — works equally at 9:20 AM or 2:00 PM.
    """
    today_open    = float(intraday["Open"].iloc[0])
    prev_close    = float(daily["Close"].iloc[-2])
    current_price = float(intraday["Close"].iloc[-1])
    first_close   = float(intraday["Close"].iloc[0])

    gap_pct = abs((today_open - prev_close) / prev_close * 100)

    # 10-day ADR = stock's average daily move capacity
    lookback = daily.iloc[-11:-1] if len(daily) >= 11 else daily.iloc[:-1]
    adr_pct  = float(
        ((lookback["High"] - lookback["Low"]) / lookback["Close"]).mean() * 100
    ) if len(lookback) > 0 else 2.0

    # How much has the stock already moved from today's open?
    if direction == "bullish":
        aligned       = first_close > today_open
        session_high  = float(intraday["High"].max())
        used_intraday = max((session_high - today_open) / today_open * 100, 0)
    else:
        aligned       = first_close < today_open
        session_low   = float(intraday["Low"].min())
        used_intraday = max((today_open - session_low) / today_open * 100, 0)

    # Total range consumed so far (gap + intraday move)
    total_used = gap_pct + used_intraday

    # Remaining range = ADR capacity minus what's already used
    # A stock near its daily range limit has less upside left
    remaining = max(adr_pct - total_used, 0.0)

    # Volume conviction: paced ratio adjusted boost (up to +40%)
    vol_boost = 1.0 + min(max(paced_vol_ratio - 2, 0) * 0.10, 0.40)

    # Alignment: first candle in same direction as gap = conviction signal
    align_factor = 1.15 if aligned else 0.80

    # Final potential = remaining range × conviction × alignment
    potential = remaining * vol_boost * align_factor

    # ADR gate: if stock historically moves < 2%/day, scale down
    if adr_pct < 2.0:
        potential *= (adr_pct / 2.0)

    confidence = "HIGH" if potential >= 3.0 else "MED" if potential >= 2.0 else "LOW"
    return {
        "projected_pct": round(potential, 2),
        "adr_pct":       round(adr_pct, 2),
        "aligned":       aligned,
        "confidence":    confidence,
    }


# ── Per-stock analysis ─────────────────────────────────────────────
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

        # Paced volume: project current volume rate to full 375-min trading day
        # This makes the 2× condition fair whether checked at 9:20 AM or 2:00 PM
        MARKET_MINS   = 375.0
        volume_paced  = (today_volume / elapsed_min) * MARKET_MINS
        paced_vol_ratio = volume_paced / prev_day_volume if prev_day_volume > 0 else 0

        # Condition 1: gap < 1% in right direction
        gap_ok = (0 < gap_pct < 1) if direction == "bullish" else (-1 < gap_pct < 0)
        # Condition 2: volume pace >= 2× previous day (time-fair comparison)
        # Condition 3: first 5-min candle body < 1%
        # Condition 4: >= ₹100 Cr total traded value
        if not (gap_ok
                and paced_vol_ratio >= 2
                and first_candle_move < 1
                and traded_value_cr >= 100):
            return None

        # Condition 5: remaining intraday move potential >= 2%
        pot = compute_move_potential(intraday, daily, direction, paced_vol_ratio)
        if pot["projected_pct"] < 2.0:
            return None

        return {
            "symbol":              symbol.replace(".NS", ""),
            "price":               round(current_price, 2),
            "gap_pct":             round(gap_pct, 2),
            "first_candle_move":   round(first_candle_move, 2),
            "volume_ratio":        round(paced_vol_ratio, 2),   # show paced ratio
            "traded_value_crores": round(traded_value_cr, 2),
            "projected_pct":       pot["projected_pct"],
            "adr_pct":             pot["adr_pct"],
            "confidence":          pot["confidence"],
            "aligned":             pot["aligned"],
        }
    except Exception:
        return None


def _get_batch():
    """Return cached batch, ensuring only one download runs at a time."""
    now = time.time()
    if "batch" in _cache:
        data, ts = _cache["batch"]
        if now - ts < CACHE_TTL:
            return data
    with _batch_lock:
        # double-check: another thread may have populated it while we waited
        if "batch" in _cache:
            data, ts = _cache["batch"]
            if now - ts < CACHE_TTL:
                return data
        result = _fetch_batch()
        _cache["batch"] = (result, time.time())
        return result


def _screen(direction):
    intraday_batch, daily_batch = _get_batch()
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    open_dt = now.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed_min = max(5.0, (now - open_dt).total_seconds() / 60)
    results = []
    for symbol in FNO_STOCKS:
        r = _analyze(symbol, intraday_batch, daily_batch, direction, elapsed_min)
        if r:
            results.append(r)
    results.sort(key=lambda x: x["projected_pct"], reverse=True)
    return results[:5]


# ── Market index data ──────────────────────────────────────────────
def _fetch_market():
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
            result.append({
                "symbol":     sym.replace(".NS", ""),
                "price":      price,
                "change_pct": change_pct,
            })
        except Exception:
            pass
    return result


# ── Routes ─────────────────────────────────────────────────────────
@app.route("/api/bullish")
def bullish():
    return jsonify(_cached("bullish", _screen, "bullish"))

@app.route("/api/bearish")
def bearish():
    return jsonify(_cached("bearish", _screen, "bearish"))

@app.route("/api/market")
def market():
    return jsonify(_cached("market", _fetch_market))

@app.route("/api/ticker")
def ticker():
    return jsonify(_cached("ticker", _fetch_ticker))

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
    })

@app.route("/api/ping")
def ping():
    return jsonify({"status": "ok"})

@app.route("/")
def index():
    return jsonify({"service": "Samvex Trading API", "status": "running"})


if __name__ == "__main__":
    print("Starting Samvex Dashboard API on http://localhost:5050")
    app.run(debug=True, port=5050, use_reloader=False)
