"""
Samvex LLP — Daily Signal Performance Analyser (GitHub Actions edition)
=======================================================================
Runs at 4:30 PM IST weekdays via GitHub Actions.
Covers all signal panels:
  Exhaustion Short, PDH Breakout, ORB Bull/Bear, OI Options, Demand/Supply
  Zone, Momentum Breakout/Breakdown, Trap Reversal.

Env vars required:
  NOTION_API_KEY   Notion personal access token
  GITHUB_TOKEN     Provided automatically by GitHub Actions (for git push)
"""

import os, json, re, sys, subprocess
from datetime import datetime
from pathlib import Path

import pytz, requests, yfinance as yf, pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────
IST              = pytz.timezone("Asia/Kolkata")
NOW              = datetime.now(IST)
TODAY_STR        = NOW.strftime("%Y-%m-%d")
TODAY_DISPLAY    = NOW.strftime("%d-%b-%y")
API_BASE         = "https://samvex-api.onrender.com"
NOTION_KEY       = os.environ.get("NOTION_API_KEY", "")
NOTION_DB_ID     = "1696c985-fc7e-4409-967a-5ceb650bfc5f"
NOTION_IMPR_PAGE = "381c1120a5e5812d9f36c10005d13644"
LOG_DIR          = Path("/tmp/samvex-logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Fetch all signals ───────────────────────────────────────────────────────
def fetch_all_signals():
    """Pulls the full day's capture across all 16 panels (active + inactive)
    from the server-side _smc_history store — NOT the live per-panel endpoints,
    which only ever return whatever's currently active on the dashboard."""
    try:
        r = requests.get(API_BASE + "/api/signals/all-today.json", timeout=30)
        r.raise_for_status()
        data = r.json()
        sigs = data.get("signals", [])
        print(f"[Fetch] all-today.json: {len(sigs)} signals across all panels")
        return sigs
    except Exception as e:
        print(f"[Fetch] /api/signals/all-today.json failed: {e}")
        return []

# ── Fetch 1-min bars ────────────────────────────────────────────────────────
_bars_cache = {}

def get_bars(symbol):
    if symbol in _bars_cache:
        return _bars_cache[symbol]
    sym_ns = symbol + ".NS" if not symbol.endswith(".NS") else symbol
    try:
        df = yf.Ticker(sym_ns).history(interval="1m", period="1d")
        if df is None or df.empty:
            _bars_cache[symbol] = None
            return None
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
        else:
            df.index = df.index.tz_convert(IST)
        today = NOW.date()
        df = df[df.index.date == today]
        _bars_cache[symbol] = df if not df.empty else None
    except Exception as e:
        print(f"  [Bars] {symbol}: {e}")
        _bars_cache[symbol] = None
    return _bars_cache[symbol]

# ── Evaluate signal outcome ─────────────────────────────────────────────────
def evaluate(sig, bars):
    entry   = float(sig.get("entry", 0) or 0)
    sl      = float(sig.get("sl", 0) or 0)
    t1      = float(sig.get("t1", 0) or 0)
    t2      = float(sig.get("t2", 0) or 0)
    bos_str = sig.get("detected_at", "")
    bullish = sig["_direction"] == "bullish"

    if entry <= 0 or sl <= 0 or t1 <= 0:
        return {"outcome": "invalid"}
    risk = abs(entry - sl)
    if risk <= 0:
        return {"outcome": "invalid"}

    start_bars = bars
    if bos_str:
        try:
            h, m = map(int, bos_str.split(":"))
            bos_dt = NOW.replace(hour=h, minute=m, second=0, microsecond=0)
            start_bars = bars[bars.index >= bos_dt]
        except Exception:
            pass
    if start_bars is None or start_bars.empty:
        return {"outcome": "no_data"}

    actual_entry = float(start_bars.iloc[0]["Close"])
    outcome      = "expired"
    exit_price   = float(bars.iloc[-1]["Close"])
    bars_to_out  = len(start_bars)
    max_fav      = 0.0

    for i, (_, row) in enumerate(start_bars.iterrows()):
        lo, hi = float(row["Low"]), float(row["High"])
        if bullish:
            max_fav = max(max_fav, (hi - actual_entry) / actual_entry * 100)
            if lo <= sl:
                outcome = "SL_hit";  exit_price = sl;  bars_to_out = i + 1; break
            if t2 > 0 and hi >= t2:
                outcome = "T2_hit";  exit_price = t2;  bars_to_out = i + 1; break
            if hi >= t1:
                outcome = "T1_hit";  exit_price = t1;  bars_to_out = i + 1
        else:
            max_fav = max(max_fav, (actual_entry - lo) / actual_entry * 100)
            if hi >= sl:
                outcome = "SL_hit";  exit_price = sl;  bars_to_out = i + 1; break
            if t2 > 0 and lo <= t2:
                outcome = "T2_hit";  exit_price = t2;  bars_to_out = i + 1; break
            if lo <= t1:
                outcome = "T1_hit";  exit_price = t1;  bars_to_out = i + 1

    r_map = {"T1_hit": 1.5, "T2_hit": 3.0, "SL_hit": -1.0}
    r_achieved = r_map.get(outcome) or round(
        ((exit_price - actual_entry) if bullish else (actual_entry - exit_price)) / risk, 2
    )
    return {
        "outcome":         outcome,
        "entry_price":     round(actual_entry, 2),
        "exit_price":      round(exit_price, 2),
        "r_achieved":      r_achieved,
        "bars_to_outcome": bars_to_out,
        "max_fav_pct":     round(max_fav, 2),
    }

# ── Build report ────────────────────────────────────────────────────────────
def build_report(signals, results):
    detail = []
    for sig, res in zip(signals, results):
        detail.append({
            "symbol":      sig.get("symbol", ""),
            "panel":       sig.get("_panel", ""),
            "setup_num":   sig.get("_setup_num"),
            "direction":   sig["_direction"],
            "is_active":   sig.get("is_active", False),
            "detected_at": sig.get("detected_at", ""),
            "score":       sig.get("confidence_score", ""),
            "vol_ratio":   sig.get("volume_ratio", ""),
            "entry":       sig.get("entry", ""),
            "sl":          sig.get("sl", ""),
            "t1":          sig.get("t1", ""),
            "t2":          sig.get("t2", ""),
            **res,
        })

    valid   = [r for r in results if r["outcome"] not in ("invalid", "no_data")]
    wins    = [r for r in valid   if r["outcome"] in ("T1_hit", "T2_hit")]
    losses  = [r for r in valid   if r["outcome"] == "SL_hit"]
    expired = [r for r in valid   if r["outcome"] == "expired"]
    win_rate     = round(len(wins) / len(valid) * 100, 1) if valid else 0
    avg_r        = round(sum(r["r_achieved"] for r in valid) / len(valid), 2) if valid else 0
    expired_rate = round(len(expired) / len(valid) * 100, 1) if valid else 0

    bos_delays = []
    for sig in signals:
        bos = sig.get("detected_at", "")
        if bos:
            try:
                h, m = map(int, bos.split(":"))
                bos_delays.append((h - 9) * 60 + m - 15)
            except Exception:
                pass
    avg_bos = round(sum(bos_delays) / len(bos_delays)) if bos_delays else 0

    return {
        "date":              TODAY_STR,
        "total_signals":     len(results),
        "valid_signals":     len(valid),
        "wins":              len(wins),
        "losses":            len(losses),
        "expired":           len(expired),
        "win_rate_pct":      win_rate,
        "avg_r_achieved":    avg_r,
        "avg_bos_delay_min": avg_bos,
        "expired_rate_pct":  expired_rate,
        "detail":            detail,
    }

# ── Post to Notion ──────────────────────────────────────────────────────────
def _notion(method, path, body=None):
    hdrs = {
        "Authorization":  f"Bearer {NOTION_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type":   "application/json",
    }
    url = f"https://api.notion.com/v1{path}"
    return getattr(requests, method)(url, headers=hdrs, json=body, timeout=15)

def _rt(text):
    return [{"type": "text", "text": {"content": str(text)}}]

def post_to_notion(report, code_change_note=""):
    if not NOTION_KEY:
        print("[Notion] No API key — skipping")
        return None

    perf_label = "No Signals"
    if report["total_signals"] > 0:
        w = report["win_rate_pct"]
        perf_label = "Strong 💪" if w >= 60 else "Moderate 🤔" if w >= 40 else "Weak ❌"

    summary = (
        f"{report['wins']}W / {report['losses']}L / {report['expired']}E | "
        f"Win rate {report['win_rate_pct']}% | Avg R {report['avg_r_achieved']}R | "
        f"BOS delay {report['avg_bos_delay_min']}min"
    )
    if code_change_note:
        summary += f"\n\n{code_change_note}"
    short_notes = summary

    body = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "Name":          {"title":    [{"text": {"content": f"{TODAY_STR} — Signal Performance"}}]},
            "Date":          {"date":     {"start": TODAY_STR}},
            "Total Signals": {"number":   report["total_signals"]},
            "Wins":          {"number":   report["wins"]},
            "Losses":        {"number":   report["losses"]},
            "Expired":       {"number":   report["expired"]},
            "Win Rate %":    {"number":   report["win_rate_pct"] / 100},
            "Avg R":         {"number":   report["avg_r_achieved"]},
            "Performance":   {"select":   {"name": perf_label}},
            "Notes":         {"rich_text": [{"text": {"content": short_notes}}]},
        },
    }
    r = _notion("post", "/pages", body)
    if r.status_code != 200:
        print(f"[Notion] Page create failed {r.status_code}: {r.text[:200]}")
        return None
    page_id = r.json()["id"]
    print(f"[Notion] Created page {page_id}")

    # Page body: summary line + signal table
    outcome_icon = {
        "T1_hit": "✅ T1 Hit", "T2_hit": "✅ T2 Hit 🔥",
        "SL_hit": "❌ SL Hit", "expired": "⏳ Expired",
    }
    dir_icon = {"bullish": "🟢 Bull", "bearish": "🔴 Bear"}

    header = [_rt("#"), _rt("Date"), _rt("Time"), _rt("Stock"), _rt("Panel"),
              _rt("Dir"), _rt("Entry ₹"), _rt("SL ₹"), _rt("T1 ₹"), _rt("Outcome"), _rt("R")]
    table_rows = [{"type": "table_row", "table_row": {"cells": header}}]
    for i, d in enumerate(report["detail"], 1):
        table_rows.append({"type": "table_row", "table_row": {"cells": [
            _rt(i),
            _rt(TODAY_DISPLAY),
            _rt(d.get("detected_at", "")),
            _rt(d["symbol"]),
            _rt(d["panel"]),
            _rt(dir_icon.get(d["direction"], d["direction"])),
            _rt(d.get("entry", "")),
            _rt(d.get("sl", "")),
            _rt(d.get("t1", "")),
            _rt(outcome_icon.get(d.get("outcome",""), d.get("outcome",""))),
            _rt(d.get("r_achieved", "")),
        ]}})

    summary_line = (
        f"{report['win_rate_pct']}% win rate · "
        f"+{report['avg_r_achieved']}R avg · "
        f"{report['wins']}W / {report['losses']}L / {report['expired']}E · "
        f"{report['total_signals']} signals"
    )
    blocks = [
        {"type": "paragraph", "paragraph": {"rich_text": _rt(summary_line)}},
        {
            "type": "table",
            "table": {
                "table_width": 11,
                "has_column_header": True,
                "has_row_header":    False,
                "children":          table_rows,
            },
        },
    ]
    rb = _notion("patch", f"/blocks/{page_id}/children", {"children": blocks})
    if rb.status_code != 200:
        print(f"[Notion] Block append failed {rb.status_code}: {rb.text[:200]}")
    else:
        print(f"[Notion] Page body written ({len(report['detail'])} signal rows)")
    return page_id

# ── Per-panel auto code improvement ─────────────────────────────────────────
# Each of the 8 setups owns its own named volume/freshness constant in api.py.
# Tuning win-rate off a single shared knob (the old behaviour) meant every
# improvement always touched Setup 1's filter regardless of which panel was
# actually underperforming, and saturated permanently once that one knob hit
# its ceiling. This tunes the SPECIFIC panel that is actually weak/strong,
# and escalates to a second knob (freshness) instead of going silent once a
# panel's primary knob is maxed out.
PANEL_TUNERS = {
    2: {"name": "Exhaustion Short",
        "ratio_const": "EXH_VOL_RATIO", "ratio_bounds": (0.9, 3.0), "ratio_step": 0.1},
    3: {"name": "PDH Breakout",
        "ratio_const": "PDH_VOL_MIN",   "ratio_bounds": (500_000, 3_000_000), "ratio_step": 100_000, "ratio_int": True,
        "fresh_const": "_PDH_FRESH",    "fresh_bounds": (2, 8),               "fresh_step": 1},
    4: {"name": "ORB",
        "ratio_const": "ORB_VOL_RATIO",      "ratio_bounds": (0.9, 3.0), "ratio_step": 0.1,
        "fresh_const": "ORB_FRESHNESS_BARS", "fresh_bounds": (3, 12),    "fresh_step": 1},
    5: {"name": "OI Options",
        "ratio_const": "OI_OPT_REL_VOL_MIN", "ratio_bounds": (1.0, 4.0), "ratio_step": 0.2},
    6: {"name": "Demand/Supply Zone",
        "ratio_const": "DZ_IMPULSE_VOL_RATIO", "ratio_bounds": (0.9, 3.0), "ratio_step": 0.1},
    7: {"name": "Momentum Breakout/Breakdown",
        "ratio_const": "MB_VOL_RATIO",         "ratio_bounds": (0.9, 3.0), "ratio_step": 0.1,
        "fresh_const": "MB_FRESHNESS_BARS",    "fresh_bounds": (3, 12),    "fresh_step": 1},
    8: {"name": "Trap Reversal",
        "ratio_const": "TRAP_VOL_RATIO",       "ratio_bounds": (0.9, 3.0), "ratio_step": 0.1,
        "fresh_const": "TRAP_FRESHNESS_BARS",  "fresh_bounds": (3, 12),    "fresh_step": 1},
}

WIN_RATE_LOW_THRESHOLD  = 25
WIN_RATE_HIGH_THRESHOLD = 65
MIN_VALID_FOR_TUNING    = 3

def panel_stats(detail):
    groups = {}
    for d in detail:
        groups.setdefault(d.get("setup_num"), []).append(d)
    stats = {}
    for setup_num, items in groups.items():
        valid   = [d for d in items if d["outcome"] not in ("invalid", "no_data")]
        wins    = [d for d in valid if d["outcome"] in ("T1_hit", "T2_hit")]
        losses  = [d for d in valid if d["outcome"] == "SL_hit"]
        expired = [d for d in valid if d["outcome"] == "expired"]
        stats[setup_num] = {
            "valid":        len(valid),
            "wins":         len(wins),
            "losses":       len(losses),
            "expired":      len(expired),
            "win_rate":     round(len(wins) / len(valid) * 100, 1) if valid else 0,
            "expired_rate": round(len(expired) / len(valid) * 100, 1) if valid else 0,
        }
    return stats

def _read_const(content, name):
    m = re.search(rf'\b{name}\s*=\s*([\d_]+(?:\.\d+)?)', content)
    return (m, m.group(1).replace("_", "")) if m else (None, None)

def _write_const(content, m, new_value, as_int_underscore=False):
    if as_int_underscore:
        new_str = f"{int(new_value):,}".replace(",", "_")
    else:
        new_str = str(new_value)
    return content[:m.start(1)] + new_str + content[m.end(1):]

def apply_code_improvement(report):
    tot = report["total_signals"]
    api_path = Path("api.py")
    if not api_path.exists():
        print("[AutoImpr] api.py not found in working directory")
        return None, "api.py not found"
    if tot == 0:
        return None, "No signals today — no code change basis"

    content = api_path.read_text()
    stats   = panel_stats(report["detail"])
    changes = []   # list of narrative lines
    msgs    = []   # list of one-liner commit summary fragments

    for setup_num, tuner in PANEL_TUNERS.items():
        st = stats.get(setup_num)
        if not st or st["valid"] < MIN_VALID_FOR_TUNING:
            continue
        w = st["win_rate"]

        if w < WIN_RATE_LOW_THRESHOLD:
            direction, verb = 1, "Tightened"
        elif w >= WIN_RATE_HIGH_THRESHOLD:
            direction, verb = -1, "Loosened"
        else:
            continue

        lo, hi = tuner["ratio_bounds"]
        step   = tuner["ratio_step"] * direction
        m, raw_old = _read_const(content, tuner["ratio_const"])
        if not m:
            changes.append(f"{tuner['name']}: ratio constant {tuner['ratio_const']} not found — skipped")
            continue
        old = float(raw_old)
        new = round(old + step, 2)
        new = max(lo, min(hi, new))

        if new == old and "fresh_const" in tuner and direction == 1:
            # Primary knob already saturated and panel is still underperforming —
            # escalate to the freshness knob instead of doing nothing.
            flo, fhi = tuner["fresh_bounds"]
            fm, fraw_old = _read_const(content, tuner["fresh_const"])
            if fm:
                fold = int(float(fraw_old))
                fnew = max(flo, fold - tuner["fresh_step"])
                if fnew != fold:
                    content = _write_const(content, fm, fnew)
                    changes.append(
                        f"{tuner['name']}: win rate {w}% over {st['valid']} signals, but "
                        f"{tuner['ratio_const']} was already at its ceiling ({old}). "
                        f"Tightened {tuner['fresh_const']} {fold}→{fnew} instead — only the most "
                        f"recently-formed setups will qualify, cutting late/stale low-conviction entries."
                    )
                    msgs.append(f"{tuner['fresh_const']} {fold}→{fnew}")
            continue

        if new == old:
            continue  # already at floor and loosening further isn't possible

        content = _write_const(content, m, new, as_int_underscore=tuner.get("ratio_int", False))
        as_pct = f"{int(new):,}" if tuner.get("ratio_int") else new
        as_pct_old = f"{int(old):,}" if tuner.get("ratio_int") else old
        changes.append(
            f"{tuner['name']}: win rate {w}% over {st['valid']} valid signals "
            f"({st['wins']}W/{st['losses']}L/{st['expired']}E). {verb} {tuner['ratio_const']} "
            f"{as_pct_old}→{as_pct}."
        )
        msgs.append(f"{tuner['ratio_const']} {as_pct_old}→{as_pct}")

    if not changes:
        overall = report["win_rate_pct"]
        return None, (f"No panel had ≥{MIN_VALID_FOR_TUNING} valid signals with a win rate outside "
                       f"{WIN_RATE_LOW_THRESHOLD}-{WIN_RATE_HIGH_THRESHOLD}% (overall win_rate={overall}%)")

    api_path.write_text(content)
    msg = f"perf: per-panel tune — {'; '.join(msgs)} [auto-{TODAY_STR}]"
    narrative = (
        "🤖 Auto-improvement applied — per-panel tuning\n"
        + "\n".join(f"• {c}" for c in changes)
        + "\nGoal: each panel's volume/freshness gate moves independently based on its OWN "
          "win rate, not the aggregate across all 16 panels, so a strong panel isn't loosened "
          "because a weak one dragged the average down (and vice versa).\n"
        + "Commit: {commit_placeholder}"
    )
    return msg, narrative

def git_commit_push(commit_msg):
    try:
        subprocess.run(["git", "config", "user.email", "ai@samvex.in"], check=True)
        subprocess.run(["git", "config", "user.name", "Samvex AI"], check=True)
        subprocess.run(["git", "add", "api.py"], check=True)
        result = subprocess.run(["git", "commit", "-m", commit_msg],
                                capture_output=True, text=True)
        if result.returncode != 0:
            print(f"[Git] Nothing to commit: {result.stderr.strip()}")
            return None
        push = subprocess.run(["git", "push"], capture_output=True, text=True)
        if push.returncode != 0:
            print(f"[Git] Push failed: {push.stderr}")
            return None
        log = subprocess.run(["git", "log", "--oneline", "-1"],
                              capture_output=True, text=True)
        commit_hash = log.stdout.strip().split()[0]
        print(f"[Git] Pushed: {commit_hash} — {commit_msg}")
        return commit_hash
    except Exception as e:
        print(f"[Git] Error: {e}")
        return None

def update_improvement_page(note):
    if not NOTION_KEY or not note:
        return
    body = {"children": [{"type": "paragraph",
                           "paragraph": {"rich_text": _rt(f"{TODAY_STR}: {note}")}}]}
    _notion("patch", f"/blocks/{NOTION_IMPR_PAGE}/children", body)
    print(f"[Notion] Improvement log updated: {note}")

# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"Samvex Daily Analysis — {NOW.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"{'='*60}")

    if NOW.weekday() >= 5:
        print("[Guard] Weekend — skipping.")
        return

    signals = fetch_all_signals()
    print(f"\nTotal signals fetched: {len(signals)}")

    results = []
    for sig in signals:
        sym = sig.get("symbol", "")
        print(f"[Eval] {sym} | {sig.get('_panel','')} | "
              f"@{sig.get('detected_at','?')} | active={sig.get('is_active')}")
        bars = get_bars(sym)
        if bars is None or bars.empty:
            results.append({"outcome": "no_data"})
            continue
        res = evaluate(sig, bars)
        results.append(res)
        print(f"       → {res['outcome']} | R={res.get('r_achieved','?')}")

    report = build_report(signals, results)

    rpt_path = LOG_DIR / f"report_{TODAY_STR}.json"
    rpt_path.write_text(json.dumps(report, indent=2))
    print(f"\n[Report] {rpt_path}")
    print(f"  Signals: {report['total_signals']} | Wins: {report['wins']} | "
          f"Losses: {report['losses']} | Expired: {report['expired']}")
    print(f"  Win rate: {report['win_rate_pct']}% | Avg R: {report['avg_r_achieved']} | "
          f"BOS delay: {report['avg_bos_delay_min']}min")

    # Auto-improvement
    commit_msg, change_note = apply_code_improvement(report)
    commit_hash = None
    if commit_msg:
        commit_hash = git_commit_push(commit_msg)
        if commit_hash:
            change_note = change_note.replace("{commit_placeholder}", commit_hash)
        else:
            change_note = change_note.replace("{commit_placeholder}", "(push failed)")

    # Post to Notion
    post_to_notion(report, change_note)
    update_improvement_page(change_note)

    print(f"\n[Done] {TODAY_STR} analysis complete.")

if __name__ == "__main__":
    main()
