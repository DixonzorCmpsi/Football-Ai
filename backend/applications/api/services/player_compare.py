"""Side-by-side player comparison with a reasoned verdict.

The verdict is built from the same numbers the cards show. No model writes it, so
it is instant, free, and always available, and every sentence can be traced to a
field. The assistant can go deeper on request, but a comparison never depends on it.

How sure the verdict sounds is tied to how wrong projections actually are.
model_training/backtest_projection_formula.py puts a typical miss around six
points per player, so a one-point edge is a coin flip and is called one.
"""

from __future__ import annotations

import asyncio

from .prediction import get_player_card

MAX_PLAYERS = 4

# Margins in projected points. See the module docstring for why they are this wide.
TOSS_UP_MARGIN = 1.5
CLEAR_MARGIN = 4.0

# Stat props that describe the same kind of volume, by position.
MAIN_PROP = {"QB": "passing yards", "RB": "rushing yards", "WR": "receiving yards", "TE": "receiving yards"}
BAD_STATUSES = ("out", "ir", "doubtful", "suspended", "pup")


def _num(value) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _status(card: dict) -> str:
    return str(card.get("injury_status") or "Active")


def _injured(card: dict) -> bool:
    s = _status(card).lower()
    return s not in ("active", "") and s != "act"


def _metrics(card: dict) -> dict:
    breakdown = card.get("projection_breakdown") or {}
    pos = (card.get("position") or "").upper()
    return {
        "projection": _num(card.get("prediction")),
        "floor": _num(card.get("floor_prediction")),
        "season_avg": _num(breakdown.get("season_avg")),
        "recent_form": _num(breakdown.get("recent_form")),
        "model_adjustment": _num(breakdown.get("model_adjustment")),
        "implied_total": _num(card.get("implied_total")),
        "spread": _num(card.get("spread")),
        "prop_line": _num(card.get("prop_line")),
        "prop_label": MAIN_PROP.get(pos),
        "anytime_td_prob": _num(card.get("anytime_td_prob")),
        "snap_pct": _num(card.get("snap_percentage")),
    }


def _name(card: dict) -> str:
    return card.get("player_name") or "Unknown"


def _short(card: dict) -> str:
    parts = _name(card).split()
    return parts[-1] if len(parts) > 1 and parts[-1].lower().rstrip(".") not in ("jr", "sr", "ii", "iii", "iv") else _name(card)


def reasons_between(a: dict, b: dict) -> tuple[list[str], list[str]]:
    """(why a is ahead of b, what favors b anyway). Only differences that matter."""
    ma, mb = _metrics(a), _metrics(b)
    na, nb = _short(a), _short(b)
    pro, con = [], []

    def compare(key, threshold, better_text, higher_is_better=True, fmt="{:.1f}"):
        va, vb = ma.get(key), mb.get(key)
        if va is None or vb is None or abs(va - vb) < threshold:
            return
        a_better = (va > vb) == higher_is_better
        line = better_text(fmt.format(va), fmt.format(vb))
        (pro if a_better else con).append(line if a_better else better_text(fmt.format(vb), fmt.format(va), swap=True))

    compare("season_avg", 1.5, lambda x, y, swap=False:
            f"{nb if swap else na} has the higher season average ({x} vs {y} points a game)")
    compare("recent_form", 3.0, lambda x, y, swap=False:
            f"{nb if swap else na} has been hotter lately ({x} vs {y} over their last good games)")
    compare("model_adjustment", 1.5, lambda x, y, swap=False:
            f"the model likes {nb if swap else na}'s setup more this week ({x} vs {y} points of adjustment)",
            fmt="{:+.1f}")
    compare("implied_total", 2.5, lambda x, y, swap=False:
            f"{nb if swap else na}'s offense is expected to score more ({x} vs {y} implied points)")
    compare("snap_pct", 12.0, lambda x, y, swap=False:
            f"{nb if swap else na} plays more snaps ({x}% vs {y}%)", fmt="{:.0f}")

    # Market volume lines only compare like with like.
    if ma["prop_label"] and ma["prop_label"] == mb["prop_label"]:
        compare("prop_line", 10.0, lambda x, y, swap=False:
                f"the betting market expects more {ma['prop_label']} from {nb if swap else na} ({x} vs {y})")
    compare("anytime_td_prob", 8.0, lambda x, y, swap=False:
            f"the market gives {nb if swap else na} a better touchdown chance ({x}% vs {y}%)", fmt="{:.0f}")

    for card, bucket, other in ((b, pro, a), (a, con, b)):
        if _injured(card) and not _injured(other):
            bucket.append(f"{_short(card)} is listed {_status(card)}")
    return pro, con


def verdict(cards: list[dict]) -> dict:
    projected = [c for c in cards if _num(c.get("prediction")) is not None]
    if len(projected) < 2:
        return {"leader_id": None, "confidence": None, "headline": "Pick at least two players we project to compare.",
                "reasons": [], "counterpoints": []}

    ranked = sorted(projected, key=lambda c: _num(c.get("prediction")) or 0.0, reverse=True)
    top, second = ranked[0], ranked[1]
    margin = (_num(top.get("prediction")) or 0.0) - (_num(second.get("prediction")) or 0.0)
    injured_top = any(s in _status(top).lower() for s in BAD_STATUSES)

    if injured_top:
        confidence = "risky"
    elif margin < TOSS_UP_MARGIN:
        confidence = "toss-up"
    elif margin < CLEAR_MARGIN:
        confidence = "lean"
    else:
        confidence = "clear"

    pro, con = reasons_between(top, second)
    tp, sp = _num(top.get("prediction")) or 0.0, _num(second.get("prediction")) or 0.0
    if confidence == "toss-up":
        headline = (f"Toss-up: {_name(top)} {tp:.1f} vs {_name(second)} {sp:.1f}. "
                    f"A {margin:.1f}-point gap is well inside a typical miss, so go with the tiebreakers below.")
    elif confidence == "risky":
        headline = (f"{_name(top)} projects highest ({tp:.1f}) but is listed {_status(top)}. "
                    f"{_name(second)} ({sp:.1f}) is the safer start until that clears.")
    else:
        strength = "Clear edge" if confidence == "clear" else "Lean"
        headline = f"{strength}: {_name(top)} over {_name(second)}, {tp:.1f} to {sp:.1f} projected."

    positions = {(c.get("position") or "").upper() for c in projected}
    notes = []
    if len(positions) > 1:
        notes.append("These players play different positions. Compare them for a flex spot, not a fixed slot.")

    return {
        "leader_id": top.get("player_id"),
        "runner_up_id": second.get("player_id"),
        "margin": round(margin, 2),
        "confidence": confidence,
        "headline": headline,
        "reasons": pro,
        "counterpoints": con,
        "notes": notes,
        "ranking": [c.get("player_id") for c in ranked],
    }


async def compare(player_ids: list[str], week: int) -> dict:
    ids = list(dict.fromkeys(i for i in player_ids if i))[:MAX_PLAYERS]
    cards = await asyncio.gather(*(get_player_card(pid, week) for pid in ids))
    found = [c for c in cards if c]
    missing = [pid for pid, c in zip(ids, cards) if not c]
    for card in found:
        card["metrics"] = _metrics(card)
    return {"week": week, "players": found, "missing": missing, "verdict": verdict(found)}
