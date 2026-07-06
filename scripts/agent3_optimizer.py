"""
Samvex Sandbox — Agent 3: Parameter Optimizer (Claude API)
===========================================================
Runs Sunday 9:00 PM IST (GitHub Actions).

Reads:
  data/signal_outcomes.json  — last 30 days of signal outcomes (Agent 1)
  data/missed_signals.json   — last 30 days of missed signals (Agent 2)
  api.py                     — current parameter constants

Calls Claude API with performance data.
AUTO-APPLIES HIGH and MEDIUM confidence proposals to api.py.
LOW confidence proposals are noted but not applied.

Output: writes data/proposals.json with applied/skipped status.
Commits api.py + proposals.json to sandbox branch.

Requires env var: ANTHROPIC_API_KEY
"""

import json, os, re, subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytz
import requests

IST        = pytz.timezone("Asia/Kolkata")
NOW        = datetime.now(IST)
TODAY_STR  = NOW.strftime("%Y-%m-%d")
CUTOFF     = (NOW - timedelta(days=30)).strftime("%Y-%m-%d")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OUTCOMES_FILE     = Path("data/signal_outcomes.json")
MISSED_FILE       = Path("data/missed_signals.json")
PROPOSALS_FILE    = Path("data/proposals.json")
API_PY            = Path("api.py")

TUNABLE_PARAMS = [
    "EXH_PREV_DAY_RALLY_PCT",
    "EXH_CUMUL_RALLY_PCT",
    "EXH_VOL_RATIO",
    "EXH_IMPULSE_MOVE_PCT",
    "EXH_IMPULSE_TURNOVER_PCT",
    "EXH_BB_PERIOD",
    "EXH_BB_STD_MULT",
    "PDH_VOL_MIN",
    "ORB_VOL_RATIO",
    "MB_VOL_RATIO",
    "TRAP_VOL_RATIO",
    "DZ_IMPULSE_VOL_RATIO",
]


def load_json(path):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return []


def read_current_params():
    if not API_PY.exists():
        return {}
    content = API_PY.read_text()
    params = {}
    for name in TUNABLE_PARAMS:
        m = re.search(rf'\b{name}\s*=\s*([\d_]+(?:\.\d+)?)', content)
        if m:
            params[name] = m.group(1).replace("_", "")
    return params


def summarise_outcomes(outcomes):
    recent = [r for r in outcomes if r.get("date", "") >= CUTOFF]
    panels = {}
    for r in recent:
        panel = r.get("panel", "unknown")
        outcome = r.get("outcome", "")
        if outcome in ("invalid", "no_data"):
            continue
        p = panels.setdefault(panel, {"wins": 0, "losses": 0, "expired": 0, "total": 0})
        p["total"] += 1
        if outcome in ("T1_hit", "T2_hit"):
            p["wins"] += 1
        elif outcome == "SL_hit":
            p["losses"] += 1
        else:
            p["expired"] += 1
    for p in panels.values():
        t = p["total"]
        p["win_rate_pct"] = round(p["wins"] / t * 100, 1) if t > 0 else 0
    return panels


def summarise_missed(missed):
    recent = [r for r in missed if r.get("date", "") >= CUTOFF]
    gate_counts = {}
    for r in recent:
        gate = r.get("blocking_gate", "unknown")
        gate_counts[gate] = gate_counts.get(gate, 0) + 1
    total = len(recent)
    avg_fall = (
        sum(r.get("actual_fall_pct", 0) for r in recent) / total if total else 0
    )
    return {
        "total_missed_30d": total,
        "avg_fall_pct_of_missed": round(avg_fall, 2),
        "gate_blocking_counts": dict(sorted(gate_counts.items(), key=lambda x: -x[1])),
        "sample_missed": recent[-5:],
    }


def build_prompt(params, outcome_summary, missed_summary):
    return f"""You are an expert algorithmic trading screener engineer working on the Samvex dashboard — a Flask+Python intraday screener for Indian equities (NSE, Nifty 500 universe).

Your job: analyse the last 30 days of signal performance data and propose specific parameter changes to improve screener quality. The goal is fewer but higher-quality signals — each one should have genuine profit-booking potential.

## Current parameter values (api.py)
{json.dumps(params, indent=2)}

## Signal outcome statistics (last 30 days, per panel)
{json.dumps(outcome_summary, indent=2)}

## Missed signals analysis — Exhaustion Short panel (last 30 days)
These are stocks that fell ≥5% from their intraday high but were NOT caught by our scanner.
{json.dumps(missed_summary, indent=2)}

## Your task
Propose up to 5 parameter changes. For each proposal:
1. Identify which parameter to change and to what value
2. Explain WHY using the actual data above (cite win rates, gate block counts, specific numbers)
3. Predict the expected impact (more signals / fewer signals / better quality)
4. Rate your confidence: HIGH / MEDIUM / LOW

Rules:
- Only propose changes to parameters listed in the current values above
- Do NOT propose removing gates entirely — only adjust thresholds
- If a panel's win rate is 0% or >80%, explain it could be small sample size
- Prioritise the Exhaustion Short panel since it has the richest missed-signals data
- Be conservative — prefer small incremental changes over large jumps
- Return ONLY valid JSON in the format below, no markdown, no explanation outside the JSON

Return this exact JSON structure:
{{
  "generated_at": "{TODAY_STR}",
  "days_analysed": 30,
  "summary": "2-3 sentence executive summary of findings",
  "proposals": [
    {{
      "parameter": "PARAM_NAME",
      "current_value": "current value as string",
      "proposed_value": "new value as string",
      "reasoning": "specific reasoning citing actual data numbers",
      "expected_impact": "what changes in signal count/quality",
      "confidence": "HIGH | MEDIUM | LOW"
    }}
  ]
}}"""


def call_claude(prompt):
    if not ANTHROPIC_API_KEY:
        print("[Claude] No ANTHROPIC_API_KEY — skipping optimizer")
        return None

    headers = {
        "x-api-key":         ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }
    body = {
        "model":      "claude-sonnet-4-6",
        "max_tokens": 1500,
        "messages":   [{"role": "user", "content": prompt}],
    }
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=body,
            timeout=60,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        print(f"[Claude] API call failed: {e}")
        return None


def _parse_numeric(val_str):
    """Parse a numeric string (handles commas, underscores, floats)."""
    cleaned = str(val_str).replace(",", "").replace("_", "").strip()
    if "." in cleaned:
        return float(cleaned), True   # (value, is_float)
    return int(cleaned), False


def _format_for_api(num, is_float, original_raw):
    """Format a number to match api.py style (underscore integers, plain floats)."""
    if is_float:
        return str(float(num))
    # Use underscores if original had them or value is large
    if "_" in original_raw or num >= 100_000:
        s = f"{int(num):_}"
        return s
    return str(int(num))


def auto_apply_proposals(proposals_data):
    """
    Apply HIGH and MEDIUM confidence proposals directly to api.py.
    Returns (applied_list, skipped_list).
    """
    if not API_PY.exists():
        print("[AutoApply] api.py not found — skipping all proposals")
        return [], list(proposals_data.get("proposals", []))

    content = API_PY.read_text()
    applied = []
    skipped = []

    for prop in proposals_data.get("proposals", []):
        confidence  = prop.get("confidence", "LOW").upper()
        param       = prop.get("parameter", "")
        new_val_str = str(prop.get("proposed_value", ""))

        if confidence == "LOW":
            skipped.append({**prop, "status": "not_applied", "skip_reason": "LOW confidence — noted but not auto-applied"})
            continue

        if param not in TUNABLE_PARAMS:
            skipped.append({**prop, "status": "not_applied", "skip_reason": f"{param} not in allowed parameter list"})
            continue

        m = re.search(rf'\b({re.escape(param)})\s*=\s*([\d_]+(?:\.\d+)?)', content)
        if not m:
            skipped.append({**prop, "status": "not_applied", "skip_reason": f"{param} not found in api.py"})
            continue

        original_raw = m.group(2)   # e.g. "1_600_000" or "1.3"
        try:
            new_num, is_float = _parse_numeric(new_val_str)
            formatted = _format_for_api(new_num, is_float, original_raw)
        except (ValueError, TypeError) as e:
            skipped.append({**prop, "status": "not_applied", "skip_reason": f"Could not parse '{new_val_str}': {e}"})
            continue

        # Patch in place — replace only the value portion
        old_span_start = m.start(2)
        old_span_end   = m.end(2)
        content = content[:old_span_start] + formatted + content[old_span_end:]

        applied.append({**prop, "status": "auto_applied", "applied_at": TODAY_STR, "formatted_value": formatted})
        print(f"[AutoApply] {param}: {original_raw} → {formatted} [{confidence}]")

    if applied:
        API_PY.write_text(content)
        print(f"[AutoApply] api.py updated with {len(applied)} change(s)")

    return applied, skipped


def git_commit_changes(n_applied):
    """Commit api.py and proposals.json back to sandbox branch."""
    try:
        subprocess.run(["git", "add", "api.py", "data/proposals.json"], check=False)
        result = subprocess.run(
            ["git", "commit", "-m", f"agent3: auto-applied {n_applied} proposals {TODAY_STR} [auto]"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            subprocess.run(["git", "pull", "--rebase", "origin", "sandbox"], check=False)
            push = subprocess.run(["git", "push"], capture_output=True, text=True)
            if push.returncode != 0:
                print(f"[AutoApply] Push failed: {push.stderr.strip()}")
            else:
                print(f"[AutoApply] Committed and pushed.")
        else:
            print(f"[AutoApply] Nothing to commit: {result.stdout.strip()}")
    except Exception as e:
        print(f"[AutoApply] Git error: {e}")


def main():
    print(f"\n{'='*60}")
    print(f"Agent 3 — Parameter Optimizer | {NOW.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"{'='*60}")

    outcomes = load_json(OUTCOMES_FILE)
    missed   = load_json(MISSED_FILE)
    params   = read_current_params()

    print(f"Outcomes loaded: {len(outcomes)} records")
    print(f"Missed signals loaded: {len(missed)} records")
    print(f"Current params: {len(params)} tunable constants found")

    outcome_summary = summarise_outcomes(outcomes)
    missed_summary  = summarise_missed(missed)

    print(f"\nOutcome summary:")
    for panel, stats in outcome_summary.items():
        print(f"  {panel}: {stats['total']} signals | {stats['win_rate_pct']}% win rate")

    print(f"\nMissed signals summary:")
    print(f"  Total missed (30d): {missed_summary['total_missed_30d']}")
    print(f"  Top blocking gate: {next(iter(missed_summary['gate_blocking_counts']), 'none')}")

    if not outcomes and not missed:
        print("\n[Guard] No data yet — need at least a few days of signal history. Skipping.")
        return

    prompt    = build_prompt(params, outcome_summary, missed_summary)
    proposals = call_claude(prompt)

    if proposals is None:
        proposals = {
            "generated_at": TODAY_STR,
            "days_analysed": 30,
            "summary": "Optimizer ran but Claude API was unavailable.",
            "proposals": [],
        }

    # Auto-apply HIGH and MEDIUM confidence proposals
    applied, skipped = auto_apply_proposals(proposals)

    proposals["proposals"] = applied + skipped
    proposals["applied_count"]  = len(applied)
    proposals["skipped_count"]  = len(skipped)
    proposals["status"] = "auto_applied" if applied else "no_changes"

    PROPOSALS_FILE.write_text(json.dumps(proposals, indent=2))

    n_applied = len(applied)
    n_skipped = len(skipped)
    print(f"\n[Done] {n_applied} proposals applied, {n_skipped} noted (LOW confidence)")
    for p in applied:
        print(f"  ✓ {p['parameter']}: {p['current_value']} → {p['proposed_value']} [{p['confidence']}]")
        print(f"    {p['reasoning'][:120]}...")
    for p in skipped:
        print(f"  ○ {p['parameter']}: {p.get('skip_reason','not applied')}")

    git_commit_changes(n_applied)


if __name__ == "__main__":
    main()
