"""
Samvex Sandbox — Agent 5: Weekly Report Generator
==================================================
Runs Saturday 8:00 AM IST (GitHub Actions).

Reads the last 7 days from:
  data/signal_outcomes.json
  data/missed_signals.json
  data/proposals.json
  data/auto_tunes.json

Writes data/weekly_report.json (read by Sandbox Monitor tab).
Also prints a plain-English summary to stdout (GitHub Actions log).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytz

IST       = pytz.timezone("Asia/Kolkata")
NOW       = datetime.now(IST)
WEEK_AGO  = (NOW - timedelta(days=7)).strftime("%Y-%m-%d")
TODAY_STR = NOW.strftime("%Y-%m-%d")

OUTCOMES_FILE   = Path("data/signal_outcomes.json")
MISSED_FILE     = Path("data/missed_signals.json")
PROPOSALS_FILE  = Path("data/proposals.json")
AUTO_TUNES_FILE = Path("data/auto_tunes.json")
WEEKLY_FILE     = Path("data/weekly_report.json")


def load(path):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return []


def _build_narrative(win_rate, total, wins, losses, expired,
                      tunes, applied_props, pending_props,
                      missed_count, top_gate, gate_counts):
    parts = []

    # Signal performance
    if total == 0:
        parts.append("No signals were evaluated this week.")
    else:
        quality = "strong" if win_rate >= 55 else ("acceptable" if win_rate >= 40 else "below target")
        parts.append(
            f"Win rate this week was {win_rate}% ({wins}W / {losses}L / {expired}E "
            f"across {total} signals) — {quality}."
        )

    # Agent 4 auto-tunes
    if tunes:
        tune_desc = "; ".join(
            f"{t['panel_name']}: {t['parameter']} {t['old_value']}→{t['new_value']} "
            f"({t['direction']}, wr was {t['win_rate']}%)"
            for t in tunes
        )
        word = "change" if len(tunes) == 1 else "changes"
        parts.append(f"Agent 4 made {len(tunes)} automatic {word}: {tune_desc}.")
    else:
        parts.append("Agent 4 made no automatic adjustments — all panels within healthy bounds.")

    # Agent 3 applied proposals
    if applied_props:
        prop_desc = "; ".join(
            f"{p['parameter']} {p['current_value']}→{p['proposed_value']} [{p['confidence']}]"
            for p in applied_props
        )
        word = "change" if len(applied_props) == 1 else "changes"
        parts.append(f"Agent 3 applied {len(applied_props)} parameter {word} on Sunday: {prop_desc}.")
    if pending_props:
        word = "proposal" if len(pending_props) == 1 else "proposals"
        parts.append(f"{len(pending_props)} LOW confidence {word} noted but not auto-applied.")

    # Agent 2 missed signals
    if top_gate and missed_count > 0:
        count = gate_counts.get(top_gate, 0)
        pct   = round(count / missed_count * 100)
        parts.append(
            f"Agent 2 found {missed_count} missed setup{'s' if missed_count != 1 else ''} — "
            f"{top_gate} was the top blocker ({count} stock{'s' if count != 1 else ''}, {pct}% of misses)."
        )
    elif missed_count == 0:
        parts.append("Agent 2 found no missed setups — all qualifying movers were caught.")

    return " ".join(parts)


def main():
    outcomes  = [r for r in load(OUTCOMES_FILE)  if r.get("date", "") >= WEEK_AGO]
    missed    = [r for r in load(MISSED_FILE)     if r.get("date", "") >= WEEK_AGO]
    tunes     = [r for r in load(AUTO_TUNES_FILE) if r.get("date", "") >= WEEK_AGO]
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
    for p, s in panels.items():
        tot = s["wins"] + s["losses"] + s["expired"]
        s["win_rate"] = round(s["wins"] / tot * 100, 1) if tot else 0

    gate_counts = {}
    for r in missed:
        gate = r.get("blocking_gate", "unknown")
        gate_counts[gate] = gate_counts.get(gate, 0) + 1
    top_gate   = max(gate_counts, key=gate_counts.get) if gate_counts else None
    top_missed = sorted(missed, key=lambda r: r.get("actual_fall_pct", 0), reverse=True)[:5]

    all_props = proposals.get("proposals", []) if isinstance(proposals, dict) else []
    gen_at    = proposals.get("generated_at", "") if isinstance(proposals, dict) else ""
    applied_this_week = [p for p in all_props if p.get("status") == "auto_applied" and gen_at >= WEEK_AGO]
    pending_props     = [p for p in all_props if p.get("status") == "not_applied"]

    narrative = _build_narrative(
        win_rate, len(valid), len(wins), len(losses), len(expired),
        tunes, applied_this_week, pending_props,
        len(missed), top_gate, gate_counts,
    )

    report = {
        "generated_at":  TODAY_STR,
        "week_start":    WEEK_AGO,
        "week_end":      TODAY_STR,
        "total_signals": len(valid),
        "wins":          len(wins),
        "losses":        len(losses),
        "expired":       len(expired),
        "win_rate":      win_rate,
        "best_signal":   best,
        "worst_signal":  worst,
        "panels":        panels,
        "missed_count":  len(missed),
        "top_gate":      top_gate,
        "gate_counts":   dict(sorted(gate_counts.items(), key=lambda x: -x[1])),
        "top_missed":    top_missed,
        "auto_tunes_this_week":    tunes,
        "proposals_applied_this_week": applied_this_week,
        "pending_proposals": len(pending_props),
        "narrative":     narrative,
    }

    WEEKLY_FILE.write_text(json.dumps(report, indent=2))
    print(f"[Agent5] Weekly report written → {WEEKLY_FILE}")

    # Human-readable stdout summary for Actions log
    lines = [
        f"",
        f"{'='*65}",
        f"  SAMVEX SANDBOX — WEEKLY REPORT  ({WEEK_AGO} → {TODAY_STR})",
        f"{'='*65}",
        f"",
        f"SIGNAL PERFORMANCE",
        f"  Total: {len(valid)}  |  {len(wins)}W / {len(losses)}L / {len(expired)}E  |  Win rate: {win_rate}%",
    ]
    if best:
        lines.append(f"  Best:  {best.get('symbol')} ({best.get('panel')}) — {best.get('outcome')} @ +{best.get('r_achieved')}R")
    if worst and worst != best:
        lines.append(f"  Worst: {worst.get('symbol')} ({worst.get('panel')}) — {worst.get('outcome')} @ {worst.get('r_achieved')}R")

    lines += ["", "PER-PANEL"]
    for panel, s in sorted(panels.items()):
        lines.append(f"  {panel:<32} {s['wins']}W/{s['losses']}L/{s['expired']}E  ({s['win_rate']}% wr)")

    lines += ["", f"MISSED SIGNALS ({len(missed)})"]
    if top_gate:
        lines.append(f"  Top blocker: {top_gate} ({gate_counts[top_gate]}x)")
    for r in top_missed[:3]:
        lines.append(f"  {r['symbol']:<12} -{r['actual_fall_pct']}%  blocked: {r['blocking_gate']}")

    lines += ["", f"AGENT 4 AUTO-TUNES THIS WEEK ({len(tunes)})"]
    for t in tunes:
        lines.append(f"  {t['date']}  {t['panel_name']}: {t['parameter']} {t['old_value']}→{t['new_value']}  ({t['direction']}, wr={t['win_rate']}%)")
    if not tunes:
        lines.append("  None — all panels within healthy range")

    lines += ["", f"CLAUDE PROPOSALS PENDING: {len(props)}", "", f"{'='*65}", ""]
    print("\n".join(lines))


if __name__ == "__main__":
    main()
