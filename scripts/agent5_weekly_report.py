"""
Samvex Sandbox — Agent 5: Weekly Report Generator
==================================================
Runs Saturday 8:00 AM IST (GitHub Actions).

Reads the last 7 days from:
  data/signal_outcomes.json
  data/missed_signals.json
  data/proposals.json

Prints a plain-English weekly report to stdout (captured as GitHub Actions log).
The report is also visible in the dashboard's Sandbox Monitor tab, which reads
the same data/ files directly — no Notion posting needed.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytz

IST       = pytz.timezone("Asia/Kolkata")
NOW       = datetime.now(IST)
WEEK_AGO  = (NOW - timedelta(days=7)).strftime("%Y-%m-%d")
TODAY_STR = NOW.strftime("%Y-%m-%d")

OUTCOMES_FILE  = Path("data/signal_outcomes.json")
MISSED_FILE    = Path("data/missed_signals.json")
PROPOSALS_FILE = Path("data/proposals.json")


def load(path):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return []


def main():
    outcomes  = [r for r in load(OUTCOMES_FILE)  if r.get("date", "") >= WEEK_AGO]
    missed    = [r for r in load(MISSED_FILE)     if r.get("date", "") >= WEEK_AGO]
    proposals = load(PROPOSALS_FILE)

    valid   = [r for r in outcomes if r.get("outcome") not in ("invalid", "no_data")]
    wins    = [r for r in valid if r.get("outcome") in ("T1_hit", "T2_hit")]
    losses  = [r for r in valid if r.get("outcome") == "SL_hit"]
    expired = [r for r in valid if r.get("outcome") == "expired"]
    win_rate = round(len(wins) / len(valid) * 100, 1) if valid else 0

    best  = max(valid, key=lambda r: r.get("r_achieved", 0)) if valid else None
    worst = min(valid, key=lambda r: r.get("r_achieved", 0)) if valid else None

    panels = {}
    for r in valid:
        p = r.get("panel", "unknown")
        panels.setdefault(p, {"wins": 0, "losses": 0, "expired": 0})
        if r.get("outcome") in ("T1_hit", "T2_hit"):
            panels[p]["wins"] += 1
        elif r.get("outcome") == "SL_hit":
            panels[p]["losses"] += 1
        else:
            panels[p]["expired"] += 1

    gate_counts = {}
    for r in missed:
        gate = r.get("blocking_gate", "unknown")
        gate_counts[gate] = gate_counts.get(gate, 0) + 1
    top_gate   = max(gate_counts, key=gate_counts.get) if gate_counts else "none"
    top_missed = sorted(missed, key=lambda r: r.get("actual_fall_pct", 0), reverse=True)[:3]

    lines = [
        f"",
        f"{'='*65}",
        f"  SAMVEX SANDBOX — WEEKLY REPORT  ({WEEK_AGO} → {TODAY_STR})",
        f"{'='*65}",
        f"",
        f"SIGNAL PERFORMANCE",
        f"  Total signals:  {len(valid)}",
        f"  Win / Loss / Expired:  {len(wins)}W / {len(losses)}L / {len(expired)}E",
        f"  Win rate:  {win_rate}%",
        f"",
    ]

    if best:
        lines.append(f"  Best signal:  {best.get('symbol')} ({best.get('panel')}) — "
                     f"{best.get('outcome')} @ +{best.get('r_achieved')}R")
    if worst and worst != best:
        lines.append(f"  Worst signal: {worst.get('symbol')} ({worst.get('panel')}) — "
                     f"{worst.get('outcome')} @ {worst.get('r_achieved')}R")

    lines += ["", "PER-PANEL BREAKDOWN"]
    for panel, stats in sorted(panels.items()):
        tot = stats["wins"] + stats["losses"] + stats["expired"]
        wr  = round(stats["wins"] / tot * 100, 1) if tot else 0
        lines.append(f"  {panel:<30} {stats['wins']}W/{stats['losses']}L/{stats['expired']}E  ({wr}% win rate)")

    lines += ["", f"MISSED SIGNALS — EXHAUSTION SHORT  ({len(missed)} this week)"]
    if gate_counts:
        lines.append(f"  Top blocking gate:  {top_gate} ({gate_counts[top_gate]} times)")
        for gate, cnt in sorted(gate_counts.items(), key=lambda x: -x[1]):
            lines.append(f"    {gate:<30} blocked {cnt} stock(s)")
    if top_missed:
        lines.append(f"")
        lines.append(f"  Biggest misses:")
        for r in top_missed:
            lines.append(f"    {r['symbol']:<12} fell {r['actual_fall_pct']}%  |  "
                         f"blocked by: {r['blocking_gate']}  "
                         f"(actual={r.get('actual_value')}, threshold={r.get('threshold')})")

    props = proposals.get("proposals", []) if isinstance(proposals, dict) else []
    lines += ["", f"PENDING PROPOSALS  ({len(props)} items — visible in dashboard Sandbox Monitor tab)"]
    if props:
        for p in props:
            lines.append(f"  [{p.get('confidence','?')}] {p['parameter']}: "
                         f"{p['current_value']} → {p['proposed_value']}")
            lines.append(f"        {p['reasoning'][:100]}...")
    else:
        lines.append("  None yet — Agent 3 runs Sunday night.")

    lines += ["", f"{'='*65}", ""]
    print("\n".join(lines))


if __name__ == "__main__":
    main()
