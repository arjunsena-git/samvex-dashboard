"""
Samvex LLP — Daily Signal Performance Analyser (GitHub Actions edition)
=======================================================================
Runs at 4:30 PM IST weekdays via GitHub Actions.
Covers all signal panels:
  Setup1 Bull/Bear, Exhaustion Short, PDH Breakout, ORB Bull/Bear.

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

# All 6 signal panels: (endpoint, direction, display label)
PANELS = [
    ("/api/setup1/bullish",      "bullish", "Liq. Sweep BOS Bullish"),
    ("/api/setup1/bearish",      "bearish", "Liq. Sweep BOS Bearish"),
    ("/api/exhaustion/short",    "bearish", "Exhaustion Short"),
    ("/api/pdh-breakout/bullish","bullish", "PDH Breakout"),
    ("/api/orb/bullish",         "bullish", "ORB Bullish"),
    ("/api/orb/bearish",         "bearish", "ORB Bearish"),
]

# ── Fetch all signals ───────────────────────────────────────────────────────
def fetch_all_signals():
    all_sigs = []
    for endpoint, direction, label in PANELS:
        try:
            r = requests.get(API_BASE + endpoint, timeout=30)
            r.raise_for_status()
            sigs = r.json() if isinstance(r.json(), list) else []
            for s in sigs:
                s["_direction"] = direction
                s["_panel"]     = label
            all_sigs.extend(sigs)
            print(f"[Fetch] {label}: {len(sigs)} signals")
        except Exception as e:
            print(f"[Fetch] {endpoint} failed: {e}")
    return all_sigs

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

# ── Auto code improvement ───────────────────────────────────────────────────
def apply_code_improvement(report):
    w   = report["win_rate_pct"]
    exp = report["expired_rate_pct"]
    bos = report["avg_bos_delay_min"]
    val = report["valid_signals"]
    tot = report["total_signals"]

    api_path = Path("api.py")
    if not api_path.exists():
        print("[AutoImpr] api.py not found in working directory")
        return None, "api.py not found"

    content = api_path.read_text()

    if tot == 0:
        return None, "No signals today — no code change basis"

    # RULE B: too many expired + late BOS → shrink freshness window
    if exp > 65 and bos > 60:
        m = re.search(r'_FRESH\s*=\s*(\d+)', content)
        if not m:
            return None, "RULE B: _FRESH not found"
        old = int(m.group(1)); new = max(3, old - 1)
        content = re.sub(r'(_FRESH\s*=\s*)\d+', f'\\g<1>{new}', content, count=1)
        msg = f"perf: decrease _FRESH {old}→{new} — expired_rate={exp}%, avg_delay={bos}min [auto-{TODAY_STR}]"
        api_path.write_text(content)
        narrative = (
            f"🤖 Auto-improvement applied — RULE B (High Expired Rate + Late BOS)\n"
            f"Trigger: {exp}% of signals expired and avg BOS delay was {bos} min after open.\n"
            f"Problem: Signals are firing too late in the day — BOS events are being detected "
            f"well after the optimal entry window, leaving signals with little time to play out.\n"
            f"Change: Decreased _FRESH {old}→{new} in api.py. This tightens the BOS detection "
            f"window, only accepting BOS events closer to the initial sweep — earlier, cleaner entries.\n"
            f"Commit: {{commit_placeholder}}"
        )
        return msg, narrative

    # RULE C: poor win rate → raise vol_ratio filter (stricter signals)
    if w < 25 and val >= 3:
        m = re.search(r'(if vol_ratio < )(\d+\.\d+)', content)
        if not m:
            return None, "RULE C: vol_ratio threshold not found"
        old = float(m.group(2)); new = round(min(2.0, old + 0.1), 1)
        content = content[:m.start(2)] + str(new) + content[m.end(2):]
        msg = f"perf: raise vol_ratio {old}→{new} — win_rate={w}% [auto-{TODAY_STR}]"
        api_path.write_text(content)
        narrative = (
            f"🤖 Auto-improvement applied — RULE C (Poor Win Rate)\n"
            f"Trigger: Win rate {w}% is below the 25% threshold across {val} valid signals.\n"
            f"Problem: Too many low-quality signals are being generated. With {report['losses']} SL hits "
            f"and only {report['wins']} wins, setups lack sufficient volume confirmation — "
            f"the moves aren't backed by real institutional interest.\n"
            f"Change: Raised vol_ratio threshold {old}→{new} in api.py (_screen_smc). "
            f"From tomorrow, a signal only fires if volume is at least {new}× the 20-day average "
            f"(was {old}×). Tighter filter = fewer but higher-conviction setups.\n"
            f"Commit: {{commit_placeholder}}"
        )
        return msg, narrative

    # RULE D: great win rate → lower vol_ratio (allow more signals)
    if w >= 65 and val >= 3:
        m = re.search(r'(if vol_ratio < )(\d+\.\d+)', content)
        if not m:
            return None, "RULE D: vol_ratio threshold not found"
        old = float(m.group(2)); new = round(max(0.9, old - 0.1), 1)
        content = content[:m.start(2)] + str(new) + content[m.end(2):]
        msg = f"perf: lower vol_ratio {old}→{new} — strong day win_rate={w}% [auto-{TODAY_STR}]"
        api_path.write_text(content)
        narrative = (
            f"🤖 Auto-improvement applied — RULE D (Strong Win Rate — Loosen Filter)\n"
            f"Trigger: Win rate {w}% exceeds 65% across {val} valid signals — strong day.\n"
            f"Problem (positive): Current vol_ratio filter of {old}× is working well but may be "
            f"too restrictive, filtering out valid setups that could add more wins.\n"
            f"Change: Lowered vol_ratio threshold {old}→{new} in api.py (_screen_smc). "
            f"Slightly more signals will be admitted tomorrow — capturing more opportunities "
            f"while the strategy is clearly working.\n"
            f"Commit: {{commit_placeholder}}"
        )
        return msg, narrative

    # RULE E: very late BOS → expand freshness window
    if bos > 90:
        m = re.search(r'_FRESH\s*=\s*(\d+)', content)
        if not m:
            return None, "RULE E: _FRESH not found"
        old = int(m.group(1)); new = min(10, old + 1)
        content = re.sub(r'(_FRESH\s*=\s*)\d+', f'\\g<1>{new}', content, count=1)
        msg = f"perf: increase _FRESH {old}→{new} — avg BOS delay {bos}min [auto-{TODAY_STR}]"
        api_path.write_text(content)
        narrative = (
            f"🤖 Auto-improvement applied — RULE E (Very Late BOS Detection)\n"
            f"Trigger: Avg BOS delay was {bos} min after open — signals firing very late.\n"
            f"Problem: BOS events are consistently happening long after the 9:15 open, "
            f"meaning liquidity sweeps are taking longer to resolve into directional breaks. "
            f"A tighter _FRESH window would miss these valid but delayed setups.\n"
            f"Change: Increased _FRESH {old}→{new} in api.py. This expands how far back "
            f"the system looks for a qualifying BOS event, accommodating slower-developing setups.\n"
            f"Commit: {{commit_placeholder}}"
        )
        return msg, narrative

    return None, f"No rule triggered (win_rate={w}%, expired={exp}%, bos={bos}min)"

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
