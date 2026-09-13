"""A whole Sleeper league at a glance: standings, luck, power, positions, matchups.

Everything comes from Sleeper's public read API plus this app's projections, so
it works for any league a user can see, with no login. For each roster we build
the lineup the model would start (the same `_recommend_lineup` the My Team tab
uses) and compare those lineups across the league.

How the less obvious numbers are made:

* **All-play record.** Each week, a team "plays" every other team's score. It
  measures strength without schedule luck; luck is the gap between a team's real
  win rate and its all-play win rate.
* **Power score.** Projected lineup strength early in the season, shifting toward
  results as weeks accumulate (see `power_weights`). Each part is a z-score
  across the league, so the parts are comparable.
* **Win probability.** A normal approximation on projected margin. Our projections
  miss a player by about 6.3 points on average (model_training/
  backtest_projection_formula.py), roughly an 8-point standard deviation. Nine
  starters give a team SD near 24, and a margin between two teams near 33. It
  treats players as independent, which they are not quite, so it is a rough guide.
"""

from __future__ import annotations

import asyncio
import math
import statistics
import time
from typing import Any

from ..config import CURRENT_SEASON, logger
from . import sleeper_league as sl

MARGIN_SD = 33.0
# One week of all-play is 9 coin flips; below this, "luck" is noise, so it is left blank.
LUCK_MIN_WEEKS = 3
POSITION_GROUPS = ("QB", "RB", "WR", "TE", "FLEX", "K", "DEF")
_card_cache: dict[tuple[str, int], tuple[dict | None, float]] = {}
CARD_TTL = 300.0


async def _card(gsis: str, week: int) -> dict | None:
    from .prediction import get_player_card

    key = (gsis, week)
    hit = _card_cache.get(key)
    if hit and time.time() - hit[1] < CARD_TTL:
        return hit[0]
    try:
        card = await get_player_card(gsis, week)
    except Exception as exc:
        logger.warning("league insights: card failed for %s: %s", gsis, exc)
        card = None
    _card_cache[key] = (card, time.time())
    return card


def _num(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _points(settings: dict, whole: str, decimal: str) -> float:
    return _num(settings.get(whole)) + _num(settings.get(decimal)) / 100.0


def power_weights(weeks_played: int) -> dict[str, float]:
    """Projection-heavy in week 1, results-heavy by midseason; never ignores either."""
    projection = max(0.35, 1.0 - 0.1 * weeks_played)
    rest = 1.0 - projection
    return {"projection": projection, "all_play": rest * 0.6, "points_for": rest * 0.4}


def zscores(values: list[float]) -> list[float]:
    if len(values) < 2:
        return [0.0 for _ in values]
    mean = statistics.fmean(values)
    sd = statistics.pstdev(values)
    return [0.0 if sd == 0 else (v - mean) / sd for v in values]


def ranks(values: dict[Any, float], descending: bool = True) -> dict[Any, int]:
    """Competition ranking (1, 2, 2, 4) so ties read honestly."""
    ordered = sorted(values.values(), reverse=descending)
    return {k: ordered.index(v) + 1 for k, v in values.items()}


def win_probability(projected: float, opponent: float) -> float:
    return 0.5 * (1.0 + math.erf((projected - opponent) / (MARGIN_SD * math.sqrt(2))))


def all_play(weekly: dict[int, dict[int, float]]) -> dict[int, dict]:
    """roster_id -> all-play wins/losses/ties over the weeks given."""
    out: dict[int, dict] = {}
    for scores in weekly.values():
        for rid, pts in scores.items():
            rec = out.setdefault(rid, {"wins": 0, "losses": 0, "ties": 0})
            for other, other_pts in scores.items():
                if other == rid:
                    continue
                if pts > other_pts:
                    rec["wins"] += 1
                elif pts < other_pts:
                    rec["losses"] += 1
                else:
                    rec["ties"] += 1
    for rec in out.values():
        games = rec["wins"] + rec["losses"] + rec["ties"]
        rec["pct"] = round((rec["wins"] + 0.5 * rec["ties"]) / games, 3) if games else None
    return out


def streak(results: list[str]) -> str | None:
    """'W3' style from a list of 'W'/'L'/'T', most recent last."""
    if not results:
        return None
    last, count = results[-1], 0
    for r in reversed(results):
        if r != last:
            break
        count += 1
    return f"{last}{count}"


def slot_group(slot: str) -> str:
    return "FLEX" if slot in ("FLEX", "WRRB_FLEX", "REC_FLEX", "SUPER_FLEX") else slot


async def _team_projection(roster: dict, week: int, league: dict, external: dict) -> dict:
    """The lineup the model would start for one roster, and its strength by position."""
    cards, specials = [], []
    for sleeper_id in (roster.get("players") or []):
        sleeper_id = str(sleeper_id)
        ext = external.get(sleeper_id)
        if ext is not None or not sleeper_id.isdigit():
            specials.append(sl._external_card(sleeper_id, ext, league.get("scoring_type") or "PPR", False, None))
            continue
        gsis = sl._sleeper_to_gsis(sleeper_id)
        if not gsis:
            continue
        card = await _card(gsis, week)
        if card and (card.get("position") or "").upper() in sl.PROJECTED_POSITIONS:
            cards.append({**card, "sleeper_id": sleeper_id})

    ranked = sorted(cards, key=lambda c: _num(c.get("prediction")), reverse=True)
    slots = [s for s in (league.get("roster_positions") or []) if s not in ("BN", "IR", "TAXI")]
    lineup = sl._recommend_lineup(ranked, slots)

    # Label each starter with the slot it fills, in the order _recommend_lineup filled them.
    fixed = [s for s in slots if s in sl.PROJECTED_POSITIONS]
    flex = [s for s in slots if slot_group(s) == "FLEX"]
    labelled, remaining_fixed = [], list(fixed)
    for card in lineup:
        pos = (card.get("position") or "").upper()
        if pos in remaining_fixed:
            remaining_fixed.remove(pos)
            labelled.append((pos, card))
        else:
            labelled.append(("FLEX" if flex else pos, card))

    by_group: dict[str, float] = {}
    starters = []
    for group, card in labelled:
        by_group[group] = by_group.get(group, 0.0) + _num(card.get("prediction"))
        starters.append({"slot": group, "player_id": card.get("player_id"), "name": card.get("player_name"),
                         "position": card.get("position"), "team": card.get("team"),
                         "projection": round(_num(card.get("prediction")), 2),
                         "injury_status": card.get("injury_status")})

    # Kicker and defense slots take the roster's best by Sleeper's projection.
    for group in ("K", "DEF"):
        need = sum(1 for s in slots if s == group)
        options = sorted((c for c in specials if c["position"] == group and c["prediction"] is not None),
                         key=lambda c: c["prediction"], reverse=True)[:need]
        for c in options:
            by_group[group] = by_group.get(group, 0.0) + c["prediction"]
            starters.append({"slot": group, "player_id": c.get("player_id"), "name": c["player_name"],
                             "position": group, "team": c.get("team"), "projection": c["prediction"],
                             "source": "sleeper"})

    starter_ids = {c["sleeper_id"] for c in lineup}
    bench = [c for c in ranked if c["sleeper_id"] not in starter_ids]
    return {
        "projected_total": round(sum(by_group.values()), 2),
        "by_group": {k: round(v, 2) for k, v in by_group.items()},
        "starters": starters,
        "bench_top3": round(sum(_num(c.get("prediction")) for c in bench[:3]), 2),
    }


async def league_insights(league_id: str, week: int, roster_id: int | None = None) -> dict:
    league = sl.get_league(league_id)
    if not league:
        raise sl.SleeperError(f"League {league_id} not found")
    rosters = sl.get_rosters(league_id)
    owners = {t["roster_id"]: t for t in sl.roster_owners(league_id)}
    try:
        season = int(league.get("season") or CURRENT_SEASON)
    except (TypeError, ValueError):
        season = CURRENT_SEASON
    external = sl.external_projections(season, week)

    # Completed weeks: every week before this one that has scores.
    weekly: dict[int, dict[int, float]] = {}
    results: dict[int, list[str]] = {}
    for w in range(1, max(1, week)):
        rows = sl._get(f"/league/{league_id}/matchups/{w}") or []
        scores = {int(r["roster_id"]): _num(r.get("points")) for r in rows if r.get("roster_id") is not None}
        if not scores or not any(scores.values()):
            continue
        weekly[w] = scores
        pairs: dict[Any, list[int]] = {}
        for r in rows:
            if r.get("matchup_id") is not None:
                pairs.setdefault(r["matchup_id"], []).append(int(r["roster_id"]))
        for ids in pairs.values():
            if len(ids) == 2:
                a, b = ids
                ra = "W" if scores[a] > scores[b] else "L" if scores[a] < scores[b] else "T"
                rb = {"W": "L", "L": "W", "T": "T"}[ra]
                results.setdefault(a, []).append(ra)
                results.setdefault(b, []).append(rb)
    allplay = all_play(weekly)

    projections = await asyncio.gather(*(_team_projection(r, week, league, external) for r in rosters))

    teams = []
    for roster, proj in zip(rosters, projections):
        rid = int(roster.get("roster_id"))
        s = roster.get("settings") or {}
        wins, losses, ties = int(s.get("wins") or 0), int(s.get("losses") or 0), int(s.get("ties") or 0)
        games = wins + losses + ties
        pf = _points(s, "fpts", "fpts_decimal")
        pa = _points(s, "fpts_against", "fpts_against_decimal")
        max_pf = _points(s, "ppts", "ppts_decimal")
        ap = allplay.get(rid, {"wins": 0, "losses": 0, "ties": 0, "pct": None})
        win_pct = (wins + 0.5 * ties) / games if games else None
        owner = owners.get(rid, {})
        teams.append({
            "roster_id": rid,
            "team_name": owner.get("team_name") or f"Team {rid}",
            "owner": owner.get("display_name"),
            "wins": wins, "losses": losses, "ties": ties,
            "points_for": round(pf, 2), "points_against": round(pa, 2),
            "max_points_for": round(max_pf, 2) if max_pf else None,
            "efficiency": round(pf / max_pf, 3) if max_pf else None,
            "all_play": ap,
            "luck": (round(win_pct - ap["pct"], 3)
                     if win_pct is not None and ap.get("pct") is not None and len(weekly) >= LUCK_MIN_WEEKS else None),
            "streak": streak(results.get(rid, [])),
            "weekly_points": [weekly[w].get(rid) for w in sorted(weekly)],
            **proj,
        })

    weeks_played = len(weekly)
    weights = power_weights(weeks_played)
    z_proj = zscores([t["projected_total"] for t in teams])
    z_ap = zscores([t["all_play"]["pct"] or 0.0 for t in teams]) if weeks_played else [0.0] * len(teams)
    z_pf = zscores([t["points_for"] / max(1, weeks_played) for t in teams]) if weeks_played else [0.0] * len(teams)
    for t, a, b, c in zip(teams, z_proj, z_ap, z_pf):
        t["power_score"] = round(weights["projection"] * a + weights["all_play"] * b + weights["points_for"] * c, 3)

    standing = sorted(teams, key=lambda t: (-(t["wins"] + 0.5 * t["ties"]), -t["points_for"]))
    for i, t in enumerate(standing, 1):
        t["standing_rank"] = i
    power = ranks({t["roster_id"]: t["power_score"] for t in teams})
    for t in teams:
        t["power_rank"] = power[t["roster_id"]]

    groups = [g for g in POSITION_GROUPS if any(g in t["by_group"] for t in teams)]
    position_ranks = {g: ranks({t["roster_id"]: t["by_group"].get(g, 0.0) for t in teams}) for g in groups}
    for t in teams:
        t["position_ranks"] = {g: position_ranks[g][t["roster_id"]] for g in groups}

    # This week's pairings with projected scores and a rough win probability.
    matchups = []
    current = sl._get(f"/league/{league_id}/matchups/{week}") or []
    by_id = {t["roster_id"]: t for t in teams}
    pairs: dict[Any, list[dict]] = {}
    for r in current:
        if r.get("matchup_id") is not None:
            pairs.setdefault(r["matchup_id"], []).append(r)
    for mid, rows in sorted(pairs.items(), key=lambda kv: kv[0]):
        if len(rows) != 2:
            continue
        sides = []
        for r in rows:
            t = by_id.get(int(r["roster_id"]))
            if not t:
                continue
            sides.append({"roster_id": t["roster_id"], "team_name": t["team_name"],
                          "projected": t["projected_total"], "live_points": _num(r.get("points"))})
        if len(sides) == 2:
            p = win_probability(sides[0]["projected"], sides[1]["projected"])
            sides[0]["win_probability"], sides[1]["win_probability"] = round(p, 3), round(1 - p, 3)
            matchups.append({"matchup_id": mid, "teams": sides})

    you = None
    if roster_id is not None and roster_id in by_id:
        t = by_id[roster_id]
        n = len(teams)
        comparable = {g: r for g, r in t["position_ranks"].items() if g not in ("K", "DEF")}
        you = {
            "roster_id": roster_id,
            "standing_rank": t["standing_rank"], "power_rank": t["power_rank"], "teams": n,
            "strongest": min(comparable, key=comparable.get) if comparable else None,
            "weakest": max(comparable, key=comparable.get) if comparable else None,
            "projected_rank": ranks({x["roster_id"]: x["projected_total"] for x in teams})[roster_id],
            "opponent": next((s for m in matchups for s in m["teams"]
                              if any(x["roster_id"] == roster_id for x in m["teams"]) and s["roster_id"] != roster_id), None),
        }

    teams.sort(key=lambda t: t["power_rank"])
    return {
        "league": {**league, "week": week, "weeks_played": weeks_played},
        "teams": teams,
        "position_groups": groups,
        "matchups": matchups,
        "you": you,
        "method": {
            "power_weights": weights,
            "margin_sd": MARGIN_SD,
            "notes": [
                "Projected lineups are the lineup our model would start for each roster.",
                "Kicker and defense slots use Sleeper's projection.",
                "All-play: every team's score against every other team's, each week.",
                "Win probability is a rough normal approximation from projected margin.",
            ],
        },
    }
