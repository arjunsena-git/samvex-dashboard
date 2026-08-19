"""
Institutional-Style Order-Flow Scanner — V1A (proxy layer)
============================================================
Implements the V1A phase of the scanner spec supplied by the trading
team (institutional_intraday_stock_scanner_context.md — research pulled
from a ChatGPT thread, itself referencing GoCharting/VolumeLens/TrueData
concepts). Philosophy, straight from that doc:

    Find flow leading price — not price leading the scanner.

Rank a liquid universe by abnormal, flow-style participation while price
displacement is still small, instead of firing dozens of binary signals
after the move has already happened. Output is a short ranked list —
Top 3 LONG / Top 3 SHORT — or a valid "NO TRADE" when nothing clears the
bar. Not a directional entry signal by itself: this is the *discovery*
layer, meant to hand a trader (or a later confirmation layer) a small,
high-quality watchlist, per the doc's two-stage workflow.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
IMPORTANT — this is a PROXY layer, not true tick-level order flow.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
True Delta/CVD/footprint require tick-by-tick trade prints with a real
aggressor-side (buy/sell) classification, or exchange market-depth data.
This codebase's current data source (Upstox/yfinance OHLCV candles) has
neither. What's computed here instead:

  • "Delta" / "CVD"  — estimated per 5-min bar from a money-flow
    multiplier (where the close sits within that bar's high-low range),
    the standard fallback used when only candle data is available. This
    is NOT genuine aggressor-side trade classification.

This is scaffolding to validate whether flow-style features have ANY
predictive value at all (the doc's own V1A goal), using data already on
hand. Per the source doc's principle #3: never report these as
"institutional buying" as fact — always "flow-style" / "proxy" language,
in code, in logs, and in the UI. V2 swaps this module's internals for a
real tick/depth feed (Dhan's 20-level WebSocket market depth, confirmed
available per DhanHQ docs, or a vendor like TrueData) without changing
the scoring architecture below — every threshold here is a named
constant for exactly that reason.

Nothing in this module is calibrated. Score bands, the survival floor,
and every bucket boundary are the doc's own starting proposals or, where
the doc didn't specify a number, an explicitly-marked placeholder of
ours. Do not treat ELITE/STRONG/WATCH labels as validated until this has
been forward-tested (see the doc's section 22 — Top-N precision, MFE/MAE,
by score decile) for several weeks.
"""

from datetime import datetime, timedelta
from pathlib import Path
import json

import pytz

IST = pytz.timezone("Asia/Kolkata")
SCANNER_VERSION = "V1A"

# ── Universe / lookback config ──────────────────────────────────────────
RVOL_LOOKBACK_DAYS   = 5     # trading days used to build the time-of-day volume baseline
CVD_BARS             = 3     # bars checked for the CVD rising/falling sequence
VOL_ACCEL_BARS       = 5     # trailing bars averaged for volume acceleration
MIN_PRICE            = 100   # same liquidity floor used by the existing panels

# ── Component weights (doc section 3) — V1A subset ──────────────────────
# Profile Context (10 pts in the doc's full 100-pt rubric) needs Market/
# Volume Profile data this module doesn't build yet — deferred to V2.
# V1A therefore scores out of 90, not 100; every score carries both the
# raw /90 value and a normalized_100 value so it's never silently compared
# against the doc's 100-pt ELITE/STRONG/WATCH bands without rescaling.
W_RVOL          = 20
W_DELTA         = 25
W_CVD           = 20
W_COMPRESSION   = 15
W_VOL_ACCEL     = 10
V1A_MAX_SCORE   = W_RVOL + W_DELTA + W_CVD + W_COMPRESSION + W_VOL_ACCEL  # 90

# ── RVOL scoring buckets (doc section 4) ─────────────────────────────────
RVOL_BUCKETS = [  # (min_rvol, score)
    (3.0, 20), (2.2, 15), (1.7, 10), (1.3, 5), (0.0, 0),
]

# ── Normalized Delta scoring buckets (doc section 5) ─────────────────────
DELTA_BUCKETS = [  # (min_abs_pct, score)
    (45.0, 25), (35.0, 17), (25.0, 10), (15.0, 5), (0.0, 0),
]

# ── Price compression scoring buckets (doc section 7) ────────────────────
# Keyed on absolute % price move from previous close.
COMPRESSION_BUCKETS = [  # (max_abs_move_pct, score) — first match wins, smallest displacement first
    (0.75, 15), (1.25, 10), (2.0, 5), (999.0, 0),
]

# Volume-acceleration scoring: the doc gives only a ~1.5x trigger for the
# 10-pt weight, not a bucketed table like the others — this linear ramp
# (0 pts at <=1.0x, full 10 pts at >=2.5x) is OUR interpolation, not from
# the source doc. Revisit once forward-test data exists.
VOL_ACCEL_FLOOR    = 1.0
VOL_ACCEL_FULL     = 2.5

# Survival floor + classification bands (doc section 23) — PROVISIONAL,
# uncalibrated placeholders pending forward-test per the doc's own
# caution (section 3, 23). Applied to the normalized_100 score.
SURVIVAL_FLOOR_100 = 55   # below this: not shown at all, counts toward NO TRADE
BAND_ELITE  = 90
BAND_STRONG = 80
BAND_WATCH  = 55  # == SURVIVAL_FLOOR_100 — anything shown is at least WATCH

TOP_N = 3  # top 3 LONG + top 3 SHORT, per doc section 1


def _bar_proxy_delta(o, h, l, c, v):
    """Money-flow-multiplier proxy for per-bar buy/sell imbalance.
    Returns (signed_volume, mfm) where mfm in [-1, 1]; +1 = close at the
    bar's high (all-out buying pressure by this proxy), -1 = close at low."""
    if h == l or v <= 0:
        return 0.0, 0.0
    mfm = ((c - l) - (h - c)) / (h - l)
    return v * mfm, mfm


def _bucket_score(value, buckets, reverse_ge=True):
    """buckets: list of (threshold, score) sorted descending by threshold.
    reverse_ge=True: first bucket whose threshold value <= is used (>=  match)."""
    for threshold, score in buckets:
        if reverse_ge:
            if value >= threshold:
                return score
        else:
            if value <= threshold:
                return score
    return 0


def _time_of_day_rvol(intra5_today, intra5_history_by_date, today_date):
    """Cumulative today-so-far volume vs the average of the same number of
    opening bars on the last RVOL_LOOKBACK_DAYS sessions — compares like
    time-of-day to like time-of-day rather than today's open against a
    flat multi-day average, since NSE volume concentrates at the open/close
    (doc section 4)."""
    n_bars_today = len(intra5_today)
    if n_bars_today == 0:
        return None
    today_vol = float(intra5_today["Volume"].sum())

    hist_dates = sorted(intra5_history_by_date.keys(), reverse=True)[:RVOL_LOOKBACK_DAYS]
    if not hist_dates:
        return None
    hist_vols = []
    for d in hist_dates:
        day_bars = intra5_history_by_date[d]
        if len(day_bars) >= n_bars_today:
            hist_vols.append(float(day_bars["Volume"].iloc[:n_bars_today].sum()))
    if not hist_vols:
        return None
    avg_hist = sum(hist_vols) / len(hist_vols)
    if avg_hist <= 0:
        return None
    return today_vol / avg_hist


def compute_symbol_features(symbol, intra5, today_date):
    """Build the raw (unscored) V1A feature set for one symbol from its
    multi-day 5-min bar history. Returns None if there isn't enough data
    to compute a reliable read. `intra5` is the full available 5-min
    history for this symbol (today + prior sessions), as already used by
    the existing OHLCV-based panels."""
    if intra5 is None or len(intra5) < VOL_ACCEL_BARS + 1:
        return None

    try:
        idx = intra5.index.tz_convert(IST) if intra5.index.tz is not None else intra5.index
    except Exception:
        idx = intra5.index

    try:
        dates_arr = idx.date
    except AttributeError:
        dates_arr = [t.date() for t in idx]

    today_mask = [d == today_date for d in dates_arr]
    today_bars = intra5[today_mask]
    if len(today_bars) < VOL_ACCEL_BARS:
        return None

    history_by_date = {}
    for row_date in set(d for d in dates_arr if d != today_date):
        mask = [d == row_date for d in dates_arr]
        history_by_date[row_date] = intra5[mask]

    opens  = today_bars["Open"].astype(float).tolist()
    highs  = today_bars["High"].astype(float).tolist()
    lows   = today_bars["Low"].astype(float).tolist()
    closes = today_bars["Close"].astype(float).tolist()
    vols   = today_bars["Volume"].astype(float).tolist()
    n = len(closes)

    current_price = closes[-1]
    if current_price < MIN_PRICE:
        return None

    prev_close = None
    if history_by_date:
        last_hist_date = max(history_by_date.keys())
        last_hist_bars = history_by_date[last_hist_date]
        if len(last_hist_bars) > 0:
            prev_close = float(last_hist_bars["Close"].iloc[-1])
    if not prev_close or prev_close <= 0:
        return None

    # RVOL (time-of-day)
    rvol = _time_of_day_rvol(today_bars, history_by_date, today_date)
    if rvol is None:
        return None

    # Proxy Delta / CVD — bar-by-bar signed volume via money-flow multiplier
    signed_vols = [
        _bar_proxy_delta(opens[i], highs[i], lows[i], closes[i], vols[i])[0]
        for i in range(n)
    ]
    cvd_series = []
    running = 0.0
    for sv in signed_vols:
        running += sv
        cvd_series.append(running)

    total_buy  = sum(v for v in signed_vols if v > 0)
    total_sell = -sum(v for v in signed_vols if v < 0)
    total_flow = total_buy + total_sell
    normalized_delta_pct = (
        (total_buy - total_sell) / total_flow * 100 if total_flow > 0 else 0.0
    )

    cvd_rising  = (len(cvd_series) >= CVD_BARS and
                   all(cvd_series[-i] > cvd_series[-i-1] for i in range(1, CVD_BARS)))
    cvd_falling = (len(cvd_series) >= CVD_BARS and
                   all(cvd_series[-i] < cvd_series[-i-1] for i in range(1, CVD_BARS)))
    cvd_recent_accel = None
    if len(cvd_series) >= CVD_BARS + 1:
        last_step = abs(cvd_series[-1] - cvd_series[-2])
        prior_steps = [abs(cvd_series[-i] - cvd_series[-i-1]) for i in range(2, CVD_BARS + 1)]
        avg_prior = sum(prior_steps) / len(prior_steps) if prior_steps else 0
        cvd_recent_accel = last_step > avg_prior if avg_prior > 0 else False

    # Volume acceleration
    recent_avg_vol = sum(vols[-(VOL_ACCEL_BARS + 1):-1]) / VOL_ACCEL_BARS
    vol_accel = (vols[-1] / recent_avg_vol) if recent_avg_vol > 0 else 0.0

    # Price displacement from previous close (compression)
    price_move_pct = (current_price - prev_close) / prev_close * 100

    return {
        "symbol":               symbol.replace(".NS", ""),
        "price":                round(current_price, 2),
        "prev_close":           round(prev_close, 2),
        "price_move_pct":       round(price_move_pct, 2),
        "rvol_time_of_day":     round(rvol, 2),
        "normalized_delta_pct": round(normalized_delta_pct, 2),
        "cvd_rising":           cvd_rising,
        "cvd_falling":          cvd_falling,
        "cvd_accelerating":     bool(cvd_recent_accel),
        "cvd_last":             round(cvd_series[-1], 0) if cvd_series else 0,
        "volume_acceleration":  round(vol_accel, 2),
        "bars_seen_today":      n,
    }


def score_features(feat):
    """Turn a raw feature dict into per-component + total scores, for both
    a LONG read and a SHORT read (a symbol can score on either side; only
    the higher-scoring, correctly-signed side is kept during ranking)."""
    rvol_score = _bucket_score(feat["rvol_time_of_day"], RVOL_BUCKETS)

    abs_delta = abs(feat["normalized_delta_pct"])
    delta_score = _bucket_score(abs_delta, DELTA_BUCKETS)
    delta_is_long = feat["normalized_delta_pct"] > 0
    delta_is_short = feat["normalized_delta_pct"] < 0

    cvd_long_score = 0
    if feat["cvd_rising"]:
        cvd_long_score = W_CVD if feat["cvd_accelerating"] else W_CVD * 0.5
    cvd_short_score = 0
    if feat["cvd_falling"]:
        cvd_short_score = W_CVD if feat["cvd_accelerating"] else W_CVD * 0.5

    compression_score = _bucket_score(abs(feat["price_move_pct"]), COMPRESSION_BUCKETS, reverse_ge=False)

    va = feat["volume_acceleration"]
    if va <= VOL_ACCEL_FLOOR:
        vol_accel_score = 0
    elif va >= VOL_ACCEL_FULL:
        vol_accel_score = W_VOL_ACCEL
    else:
        vol_accel_score = W_VOL_ACCEL * (va - VOL_ACCEL_FLOOR) / (VOL_ACCEL_FULL - VOL_ACCEL_FLOOR)

    long_total = (
        rvol_score
        + (delta_score if delta_is_long else 0)
        + cvd_long_score
        + compression_score
        + vol_accel_score
    )
    short_total = (
        rvol_score
        + (delta_score if delta_is_short else 0)
        + cvd_short_score
        + compression_score
        + vol_accel_score
    )

    return {
        "rvol_score":         round(rvol_score, 1),
        "delta_score":        round(delta_score, 1),
        "cvd_long_score":     round(cvd_long_score, 1),
        "cvd_short_score":    round(cvd_short_score, 1),
        "compression_score":  round(compression_score, 1),
        "vol_accel_score":    round(vol_accel_score, 1),
        "long_score_90":      round(long_total, 1),
        "short_score_90":     round(short_total, 1),
        "long_score_100":     round(long_total / V1A_MAX_SCORE * 100, 1),
        "short_score_100":    round(short_total / V1A_MAX_SCORE * 100, 1),
    }


def _classify(score_100):
    if score_100 >= BAND_ELITE:
        return "ELITE"
    if score_100 >= BAND_STRONG:
        return "STRONG"
    if score_100 >= BAND_WATCH:
        return "WATCH"
    return "IGNORE"


def rank_candidates(universe, batch_5m, get_ticker_df_fn, debug=None):
    """Main entry point. `get_ticker_df_fn(batch, symbol)` extracts one
    symbol's multi-day 5-min DataFrame from the already-fetched batch
    (same helper the existing candle-based panels use), so this module
    stays decoupled from api.py's HTTP/caching layer.

    Returns every scored candidate (for storage/back-testing — the doc's
    own principle: preserve raw features and rejected candidates, not
    just the survivors) plus the Top-3 LONG / Top-3 SHORT / NO TRADE view."""
    ist = IST
    today_date = datetime.now(ist).date()

    all_scored = []
    for symbol in universe:
        try:
            intra5 = get_ticker_df_fn(batch_5m, symbol)
            feat = compute_symbol_features(symbol, intra5, today_date)
            if feat is None:
                if debug is not None:
                    debug.setdefault("rejected", []).append({"symbol": symbol, "reason": "insufficient_data"})
                continue
            scores = score_features(feat)
            row = {**feat, **scores}
            row["long_band"]  = _classify(scores["long_score_100"])
            row["short_band"] = _classify(scores["short_score_100"])
            all_scored.append(row)
        except Exception as e:
            if debug is not None:
                debug.setdefault("errors", []).append({"symbol": symbol, "error": str(e)})
            continue

    longs = sorted(
        [r for r in all_scored if r["long_score_100"] >= SURVIVAL_FLOOR_100],
        key=lambda r: r["long_score_100"], reverse=True,
    )[:TOP_N]
    shorts = sorted(
        [r for r in all_scored if r["short_score_100"] >= SURVIVAL_FLOOR_100],
        key=lambda r: r["short_score_100"], reverse=True,
    )[:TOP_N]

    return {
        "scanner_version":  SCANNER_VERSION,
        "generated_at":     datetime.now(ist).isoformat(),
        "is_proxy_data":    True,
        "proxy_disclaimer": (
            "V1A scores are estimated from OHLCV candles (money-flow-multiplier "
            "proxy for Delta/CVD), NOT true tick-level order flow. Treat as "
            "flow-style participation ranking, not confirmed institutional activity."
        ),
        "universe_size":    len(universe),
        "scored_count":     len(all_scored),
        "survival_floor_100": SURVIVAL_FLOOR_100,
        "top_long":   longs,
        "top_short":  shorts,
        "no_trade_long":  len(longs) == 0,
        "no_trade_short": len(shorts) == 0,
        "all_scored": all_scored,
    }


def persist_scan(result, signals_dir, retention_days=30):
    """Append this scan's full raw-feature output (not just the Top-3) to
    a rolling JSONL-style store so back-testing (doc section 22 — Top-N
    hit rate, MFE/MAE by score decile) has the underlying data, not just
    the day's headline survivors."""
    path = Path(signals_dir) / "flow_scans_v1a.json"
    try:
        existing = json.loads(path.read_text()) if path.exists() else []
    except Exception:
        existing = []

    cutoff = (datetime.now(IST) - timedelta(days=retention_days)).strftime("%Y-%m-%d")
    existing = [r for r in existing if r.get("generated_at", "")[:10] >= cutoff]
    existing.append(result)

    try:
        path.write_text(json.dumps(existing, indent=2))
    except Exception as e:
        print(f"[OrderFlowV1A] persist_scan failed: {e}")
