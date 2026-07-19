"""
Samvex Sandbox — Agent 3: Parameter Optimizer (Claude API)
===========================================================
Runs Sunday 9:00 PM IST (GitHub Actions).

Reads:
  data/signal_outcomes.json  — last 30 days of signal outcomes (Agent 1)
  data/missed_signals.json   — last 30 days of missed signals (Agent 2)
  api.py                     — each panel's actual gate logic, read fresh

Calls Claude API with performance data AND each panel's real source code —
not a fixed list of numbers to nudge. Claude is free to propose either a
tune to an existing constant, or a genuinely new parameter/gate/logic
change the code doesn't have yet. There is no hardcoded allowlist of
"the only parameters that may change."

Two different things happen to a proposal depending on its type:
  - parameter_tune  (adjusts a constant that already exists in api.py):
    HIGH/MEDIUM confidence auto-applies via a safe regex value-swap,
    identical in risk to what Agent 4 already does daily. LOW confidence
    is recorded but not applied.
  - logic_change (a new parameter, a new gate, or any structural change
    the code doesn't already express): NEVER auto-applied — writing new
    trading logic isn't a value swap, and an unsupervised weekly script
    committing untested new gate code is a real way to quietly break a
    panel. These are surfaced in proposals.json and the weekly report as
    a "proposed_for_review" recommendation for Arjun (or a Claude Code
    session) to actually implement with real review.

Output: writes data/proposals.json with applied/skipped/proposed status.
Commits api.py (if any parameter_tune applied) + proposals.json.

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

# Known numeric knobs Agent 4 already nudges daily — given to Claude as
# context on what's already tunable via a plain value-swap, NOT as a limit
# on what it's allowed to propose. Anything it finds in the actual function
# source below is fair game for a parameter_tune; anything that doesn't
# exist yet is a logic_change.
KNOWN_TUNABLE_PARAMS = [
    "EXH_PREV_DAY_RALLY_PCT", "EXH_CUMUL_RALLY_PCT", "EXH_VOL_RATIO",
    "EXH_IMPULSE_MOVE_PCT", "EXH_IMPULSE_TURNOVER_PCT", "EXH_BB_PERIOD", "EXH_BB_STD_MULT",
    "PDH_VOL_MIN", "ORB_VOL_RATIO", "MB_VOL_RATIO", "TRAP_VOL_RATIO", "DZ_IMPULSE_VOL_RATIO",
]

# setup_num -> (display name, screener function in api.py)
PANEL_FUNCTIONS = {
    2: ("Exhaustion Short",              "_screen_exhaustion_short"),
    3: ("PDH Breakout",                  "_screen_pdh_trend"),
    4: ("ORB",                           "_screen_orb"),
    5: ("OI Options",                    "_screen_oi_options"),
    6: ("Demand/Supply Zone",            "_screen_demand_supply_zone"),
    7: ("Momentum Breakout/Breakdown",   "_screen_momentum_breakout"),
    8: ("Trap Reversal",                 "_screen_trap"),
}


def load_json(path):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return []


def extract_function_source(content, func_name, max_chars=20000):
    """Pull a top-level function's actual source — def line through the
    next top-level def/@app.route/class — so Claude sees the real gates,
    not just a name/value pair with no context for what it does."""
    m = re.search(rf'^def {re.escape(func_name)}\(.*', content, re.MULTILINE)
    if not m:
        return None
    rest = content[m.end():]
    end_m = re.search(r'^(def |@app\.route|class )', rest, re.MULTILINE)
    end = m.end() + end_m.start() if end_m else len(content)
    src = content[m.start():end].rstrip()
    if len(src) > max_chars:
        src = src[:max_chars] + "\n# ...(truncated for prompt size)..."
    return src


def read_panel_sources():
    if not API_PY.exists():
        return {}
    content = API_PY.read_text()
    sources = {}
    for setup_num, (name, func_name) in PANEL_FUNCTIONS.items():
        src = extract_function_source(content, func_name)
        if src:
            sources[name] = src
    return sources


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


def build_prompt(panel_sources, outcome_summary, missed_summary):
    sources_block = "\n\n".join(
        f"### {name}\n```python\n{src}\n```" for name, src in panel_sources.items()
    )
    return f"""You are an expert algorithmic trading screener engineer working on the Samvex dashboard — a Flask+Python intraday screener for Indian equities (NSE, Nifty 500 universe).

Your job: analyse the last 30 days of real signal performance and propose changes that genuinely improve win rate — not just make panels stricter or looser. You have the ACTUAL current gate logic for every panel below, not just a list of numbers. Read it.

## Current gate logic, per panel (this IS the real code — read it, don't guess at what it does)
{sources_block}

## Known existing tunable constants (nudging these is a simple value swap — but this list is NOT exhaustive and NOT a restriction, just context on what's cheap to auto-apply)
{json.dumps(KNOWN_TUNABLE_PARAMS, indent=2)}

## Signal outcome statistics (last 30 days, per panel)
{json.dumps(outcome_summary, indent=2)}

## Missed signals analysis — Exhaustion Short panel (last 30 days)
Stocks that fell ≥5% from their intraday high but were NOT caught by our scanner.
{json.dumps(missed_summary, indent=2)}

## Your task
You are NOT restricted to tweaking the constants listed above. If the data suggests a panel's actual problem is something the current logic doesn't check for at all — a missing gate, a timing issue, a confirmation signal that doesn't exist yet, a completely different threshold — propose that. Adding a genuinely new parameter or gate is exactly as valid a proposal as nudging an existing one, when the evidence supports it. Do not artificially limit yourself to what's already there just because it's easier to apply automatically.

Propose up to 6 changes total, mixing both kinds freely as the data warrants:

**parameter_tune** — adjusting a constant that already exists in the code above:
{{
  "type": "parameter_tune",
  "panel": "panel name",
  "parameter": "EXACT_CONST_NAME_AS_WRITTEN_IN_CODE_ABOVE",
  "current_value": "current value as string",
  "proposed_value": "new value as string",
  "reasoning": "cite actual win rates / gate block counts / specific numbers from the data above",
  "expected_impact": "what changes in signal count/quality",
  "confidence": "HIGH | MEDIUM | LOW"
}}

**logic_change** — a new parameter, a new gate, or any change the current code doesn't express yet:
{{
  "type": "logic_change",
  "panel": "panel name",
  "title": "short title of the idea",
  "proposed_change": "precise description of the new gate/parameter/logic — include suggested constant name(s) and starting value(s), and where in the function's gate sequence it should sit relative to the existing gates",
  "reasoning": "cite actual data above — why does the current logic's blind spot explain the losses or missed signals",
  "expected_impact": "what changes in signal count/quality",
  "confidence": "HIGH | MEDIUM | LOW"
}}

Rules:
- Ground every proposal in the actual numbers above — cite specific win rates, gate block counts, or missed-signal patterns. Don't propose something generic.
- If a panel's win rate is 0% or >80%, flag that it could be small sample size rather than proposing a large swing off it.
- Prioritise whichever panel's data tells the clearest story, not necessarily Exhaustion Short by default.
- Be honest in confidence ratings — logic_change proposals are inherently less certain than a parameter_tune, since they haven't been backtested; don't rate one HIGH unless the evidence is very strong.
- Return ONLY valid JSON in the format below, no markdown, no explanation outside the JSON.

Return this exact JSON structure:
{{
  "generated_at": "{TODAY_STR}",
  "days_analysed": 30,
  "summary": "2-3 sentence executive summary of findings — call out explicitly if the best fix this week is structural rather than a threshold nudge",
  "proposals": [ ... mix of parameter_tune and logic_change objects as specified above ... ]
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
        "max_tokens": 4000,
        "messages":   [{"role": "user", "content": prompt}],
    }
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=body,
            timeout=90,
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
    if "_" in original_raw or num >= 100_000:
        return f"{int(num):_}"
    return str(int(num))


def auto_apply_proposals(proposals_data):
    """
    Auto-applies HIGH/MEDIUM confidence parameter_tune proposals whose named
    constant is actually found in api.py — the only safety gate is "does
    this exact name really exist," not a fixed allowlist. logic_change
    proposals are never auto-applied, regardless of confidence.
    Returns (applied_list, skipped_list, structural_list).
    """
    if not API_PY.exists():
        print("[AutoApply] api.py not found — skipping all proposals")
        return [], list(proposals_data.get("proposals", [])), []

    content = API_PY.read_text()
    applied    = []
    skipped    = []
    structural = []

    for prop in proposals_data.get("proposals", []):
        ptype = prop.get("type", "parameter_tune")

        if ptype == "logic_change":
            structural.append({**prop, "status": "proposed_for_review"})
            continue

        confidence  = prop.get("confidence", "LOW").upper()
        param       = prop.get("parameter", "")
        new_val_str = str(prop.get("proposed_value", ""))

        if confidence == "LOW":
            skipped.append({**prop, "status": "not_applied", "skip_reason": "LOW confidence — noted but not auto-applied"})
            continue

        m = re.search(rf'\b({re.escape(param)})\s*=\s*([\d_]+(?:\.\d+)?)', content)
        if not m:
            skipped.append({**prop, "status": "not_applied", "skip_reason": f"{param} not found in api.py — propose as logic_change if it's meant to be new"})
            continue

        original_raw = m.group(2)
        try:
            new_num, is_float = _parse_numeric(new_val_str)
            formatted = _format_for_api(new_num, is_float, original_raw)
        except (ValueError, TypeError) as e:
            skipped.append({**prop, "status": "not_applied", "skip_reason": f"Could not parse '{new_val_str}': {e}"})
            continue

        old_span_start = m.start(2)
        old_span_end   = m.end(2)
        content = content[:old_span_start] + formatted + content[old_span_end:]

        applied.append({**prop, "status": "auto_applied", "applied_at": TODAY_STR, "formatted_value": formatted})
        print(f"[AutoApply] {param}: {original_raw} → {formatted} [{confidence}]")

    if applied:
        API_PY.write_text(content)
        print(f"[AutoApply] api.py updated with {len(applied)} change(s)")

    return applied, skipped, structural


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
    panel_sources = read_panel_sources()

    print(f"Outcomes loaded: {len(outcomes)} records")
    print(f"Missed signals loaded: {len(missed)} records")
    print(f"Panel sources read: {len(panel_sources)} of {len(PANEL_FUNCTIONS)}")

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

    prompt    = build_prompt(panel_sources, outcome_summary, missed_summary)
    proposals = call_claude(prompt)

    if proposals is None:
        proposals = {
            "generated_at": TODAY_STR,
            "days_analysed": 30,
            "summary": "Optimizer ran but Claude API was unavailable.",
            "proposals": [],
        }

    applied, skipped, structural = auto_apply_proposals(proposals)

    proposals["proposals"]          = applied + skipped + structural
    proposals["applied_count"]      = len(applied)
    proposals["skipped_count"]      = len(skipped)
    proposals["structural_count"]   = len(structural)
    proposals["status"] = "auto_applied" if applied else "no_changes"

    PROPOSALS_FILE.write_text(json.dumps(proposals, indent=2))

    print(f"\n[Done] {len(applied)} tunes applied, {len(skipped)} noted, {len(structural)} structural ideas for review")
    for p in applied:
        print(f"  ✓ {p['parameter']}: {p['current_value']} → {p['proposed_value']} [{p['confidence']}]")
        print(f"    {p['reasoning'][:120]}...")
    for p in skipped:
        print(f"  ○ {p['parameter']}: {p.get('skip_reason','not applied')}")
    for p in structural:
        print(f"  ★ [{p.get('confidence','?')}] {p.get('panel','?')} — {p.get('title','')}")
        print(f"    {p.get('proposed_change','')[:160]}...")

    git_commit_changes(len(applied))


if __name__ == "__main__":
    main()
