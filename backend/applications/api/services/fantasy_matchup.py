"""Your fantasy matchup this week, player by player, against the team you're playing.

Built for planning a week: for every starter on both sides it gathers what the
app knows about that player's NFL game and turns it into a few calls.

* **Lineups** are the ones each manager has actually set in Sleeper for the
  week (the matchup's ``starters``), falling back to the roster's saved starters.
* **Projection range.** Our projections miss by about 6.3 points on average
  (model_training/backtest_projection_formula.py). Misses grow with volume, so
  each player gets an SD of ``2.5 + 0.45 * projection`` (clamped 3..12), and the
  floor/ceiling shown are the 10th/90th percentiles of that normal. Team SD is
  the root-sum-square of its starters, treating players as independent.
* **Game script** reads the Vegas line from the player's side: spread (negative
  = favored), total, and the team's implied points.
* **Touchdown chance** is Bovada's anytime-TD price when one is posted.
  Otherwise it's a model estimate: the player's rushing + receiving touchdowns
  per game over his last 17 games, blended with a typical rate for his position
  (worth 8 games, so one hot stretch doesn't read as a sure thing), scaled by
  his team's implied points against a 22.5-point average, as
  ``1 - exp(-rate)``. Always labelled with its source.
* **Defense vs position** ranks every defense by fantasy points (PPR) allowed
  per game to the position, this season plus last, with this season's games
  counted double. Rank 1 is the softest matchup.
* **Live.** Once a player's NFL game is final, his real Sleeper points replace
  the projection in the "live" totals and win probability.
"""

from __future__ import annotations

import math
import time
from collections import defaultdict
from typing import Any

import polars as pl

from ..config import CURRENT_SEASON, logger
from . import sleeper_league as sl
from .league_insights import _card, _num, slot_group

AVERAGE_TEAM_POINTS = 22.5
STARTER_SLOTS_EXCLUDED = ("BN", "IR", "TAXI")
INJURY_RISK = {"questionable": "Q", "doubtful": "D", "out": "OUT", "ir": "IR", "pup": "PUP", "suspended": "SUS"}
MAIN_PROPS = {
    "QB": ("Passing Yards", "Passing TDs", "Passing Touchdowns", "Rushing Yards"),
    "RB": ("Rushing Yards", "Receiving Yards", "Receptions"),
    "WR": ("Receiving Yards", "Receptions"),
    "TE": ("Receiving Yards", "Receptions"),
}
# Rushing + receiving TDs per game for a typical fantasy starter at the position,
# and how many games of weight that prior gets against the player's own record.
TD_PRIOR = {"QB": 0.12, "RB": 0.45, "WR": 0.35, "TE": 0.25}
TD_PRIOR_GAMES = 8
_dvp_cache: dict[str, Any] = {"at": 0.0, "ranks": {}}
DVP_TTL = 1800.0


# --- small, testable pieces --------------------------------------------------------

def player_sd(projection: float) -> float:
    return max(3.0, min(12.0, 2.5 + 0.45 * max(0.0, projection)))


def projection_range(projection: float) -> tuple[float, float]:
    sd = player_sd(projection)
    return round(max(0.0, projection - 1.2816 * sd), 1), round(projection + 1.2816 * sd, 1)


def normal_win_probability(margin: float, sd: float) -> float:
    if sd <= 0:
        return 1.0 if margin > 0 else 0.0 if margin < 0 else 0.5
    return 0.5 * (1.0 + math.erf(margin / (sd * math.sqrt(2))))


def game_script(spread: float | None, total: float | None, implied: float | None) -> dict:
    """A short read of how the game should flow for this player's team."""
    notes: list[str] = []
    tone = "neutral"
    if spread is not None:
        if spread <= -7:
            notes.append(f"Big favorite ({spread:+.1f}): likely leading, more late rushing")
            tone = "good"
        elif spread <= -3:
            notes.append(f"Favored ({spread:+.1f})")
            tone = "good"
        elif spread >= 7:
            notes.append(f"Big underdog ({spread:+.1f}): likely trailing, more passing volume")
            tone = "bad"
        elif spread >= 3:
            notes.append(f"Underdog ({spread:+.1f})")
            tone = "bad"
        else:
            notes.append(f"Close game ({spread:+.1f})")
    if total is not None:
        if total >= 48:
            notes.append(f"shootout total {total:g}")
            tone = "good" if tone != "bad" else "neutral"
        elif total <= 40:
            notes.append(f"low total {total:g}")
            tone = "bad" if tone != "good" else "neutral"
        else:
            notes.append(f"total {total:g}")
    if implied is not None:
        notes.append(f"team implied {implied:g} pts")
    label = "Positive" if tone == "good" else "Negative" if tone == "bad" else "Neutral"
    return {"label": label, "tone": tone, "summary": "; ".join(notes) if notes else "No line posted yet"}


def td_chance(anytime_prob: float | None, tds_per_game: float | None, implied: float | None) -> dict | None:
    if anytime_prob is not None:
        prob = anytime_prob / 100.0  # Bovada implied_prob is a percentage
        return {"probability": round(prob, 3), "source": "bovada"}
    if tds_per_game is None:
        return None
    scale = (implied / AVERAGE_TEAM_POINTS) if implied else 1.0
    return {"probability": round(1.0 - math.exp(-max(0.0, tds_per_game) * scale), 3), "source": "model"}


def main_props(position: str, props: list[dict] | None) -> list[dict]:
    """The headline over/under lines for a position, one per market, nearest even money."""
    wanted = MAIN_PROPS.get((position or "").upper(), ())
    best: dict[str, dict] = {}
    for p in props or []:
        kind = p.get("prop_type")
        if kind not in wanted or (p.get("side") or "").lower() not in ("over", ""):
            continue
        closeness = abs(_num(p.get("implied_prob"), 50.0) - 52.4)
        if kind not in best or closeness < best[kind]["_c"]:
            best[kind] = {"market": kind, "line": p.get("line"), "odds": p.get("odds"), "_c": closeness}
    out = [dict((k, v) for k, v in best[k].items() if k != "_c") for k in wanted if k in best]
    return out[:3]


def injury_flag(status: str | None) -> str | None:
    s = (status or "").strip().lower()
    for key, flag in INJURY_RISK.items():
        if s.startswith(key):
            return flag
    return None


# --- data from the loaded frames ---------------------------------------------------

def _stats_frames() -> list[tuple[pl.DataFrame, bool]]:
    from .data_loader import model_data

    frames = []
    cur = model_data.get("df_player_stats")
    if isinstance(cur, pl.DataFrame) and not cur.is_empty():
        frames.append((cur, True))
    hist = model_data.get("df_player_stats_history")
    if isinstance(hist, pl.DataFrame) and not hist.is_empty():
        frames.append((hist, False))
    return frames


def _col(df: pl.DataFrame, *names: str) -> str | None:
    return next((n for n in names if n in df.columns), None)


def defense_vs_position(frames: list[tuple[pl.DataFrame, bool]] | None = None) -> dict[str, dict[str, dict]]:
    """position -> defense -> {"rank", "allowed", "teams"}; rank 1 allows the most."""
    use_cache = frames is None
    if use_cache:
        if time.time() - _dvp_cache["at"] < DVP_TTL and _dvp_cache["ranks"]:
            return _dvp_cache["ranks"]
        frames = _stats_frames()
    totals: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for df, current in frames:
        pts, opp, pos = _col(df, "y_fantasy_points_ppr", "fantasy_points_ppr"), _col(df, "opponent_team"), _col(df, "position")
        wk, season = _col(df, "week"), _col(df, "season")
        if not (pts and opp and pos and wk):
            continue
        keep = df
        if not current and season:
            last = keep.select(pl.col(season).max()).item()
            keep = keep.filter(pl.col(season) == last)
        grouped = (
            keep.filter(pl.col(pos).is_in(list(MAIN_PROPS)))
            .group_by([opp, pos] + ([season] if season else []) + [wk])
            .agg(pl.col(pts).fill_null(0).sum().alias("allowed"))
        )
        weight = 2.0 if current else 1.0
        for row in grouped.iter_rows(named=True):
            key = (str(row[pos]).upper(), str(row[opp]).upper())
            totals[key][0] += weight * _num(row["allowed"])
            totals[key][1] += weight
    by_pos: dict[str, dict[str, float]] = defaultdict(dict)
    for (position, defense), (points, games) in totals.items():
        if games:
            by_pos[position][defense] = points / games
    ranks: dict[str, dict[str, dict]] = {}
    for position, allowed in by_pos.items():
        ordered = sorted(allowed.items(), key=lambda kv: kv[1], reverse=True)
        ranks[position] = {d: {"rank": i, "allowed": round(v, 1), "teams": len(ordered)} for i, (d, v) in enumerate(ordered, 1)}
    if use_cache:
        _dvp_cache.update(at=time.time(), ranks=ranks)
    return ranks


def touchdowns_per_game(player_id: str, frames: list[tuple[pl.DataFrame, bool]] | None = None, games: int = 17,
                        position: str | None = None) -> float | None:
    frames = _stats_frames() if frames is None else frames
    rows: list[tuple[int, int, float]] = []
    for df, current in frames:
        pid = _col(df, "player_id")
        if not pid:
            continue
        mine = df.filter(pl.col(pid) == player_id)
        if mine.is_empty():
            continue
        td_cols = [c for c in ("passing_touchdown", "rush_touchdown", "receiving_touchdown", "rushing_tds", "receiving_tds") if c in mine.columns]
        if not td_cols:
            continue
        season_col = _col(mine, "season")
        for r in mine.iter_rows(named=True):
            season = int(r[season_col]) if season_col and r.get(season_col) is not None else (CURRENT_SEASON if current else 0)
            # Passing TDs don't score like a rushing or receiving touchdown; count only those two.
            tds = sum(_num(r.get(c)) for c in td_cols if not c.startswith("passing"))
            rows.append((season, int(_num(r.get("week"))), tds))
    prior = TD_PRIOR.get((position or "").upper())
    if not rows:
        return prior
    recent = sorted(rows, reverse=True)[:games]
    scored = sum(t for _s, _w, t in recent)
    if prior is None:
        return scored / len(recent)
    return (scored + prior * TD_PRIOR_GAMES) / (len(recent) + TD_PRIOR_GAMES)


def _kickoffs(week: int) -> dict[str, dict]:
    """team -> {"kickoff", "home", "opponent", "final"} for the week."""
    from .data_loader import model_data

    sched = model_data.get("df_schedule")
    out: dict[str, dict] = {}
    if not isinstance(sched, pl.DataFrame) or sched.is_empty() or "week" not in sched.columns:
        return out
    rows = sched.filter(pl.col("week") == week)
    if "season" in rows.columns:
        rows = rows.filter(pl.col("season") == rows.select(pl.col("season").max()).item())
    for r in rows.iter_rows(named=True):
        home, away = str(r.get("home_team") or "").upper(), str(r.get("away_team") or "").upper()
        kickoff = " ".join(str(x) for x in (r.get("gameday"), r.get("gametime")) if x) or None
        final = r.get("home_score") is not None and r.get("away_score") is not None
        out[home] = {"kickoff": kickoff, "home": True, "opponent": away, "final": final}
        out[away] = {"kickoff": kickoff, "home": False, "opponent": home, "final": final}
    return out


# --- the matchup ---------------------------------------------------------------------

async def _starter(sleeper_id: str, slot: str, week: int, league: dict, external: dict, points: dict,
                   dvp: dict, kickoffs: dict, frames) -> dict:
    ext = external.get(sleeper_id)
    base: dict[str, Any] = {"sleeper_id": sleeper_id, "slot": slot_group(slot)}
    if ext is not None or not sleeper_id.isdigit():
        card = sl._external_card(sleeper_id, ext, league.get("scoring_type") or "PPR", True, None)
        source = "sleeper"
    else:
        gsis = sl._sleeper_to_gsis(sleeper_id)
        card = await _card(gsis, week) if gsis else None
        source = "model"
        if not card:
            return {**base, "player_name": f"Unmatched player {sleeper_id}", "position": None, "projection": 0.0,
                    "floor": 0.0, "ceiling": 0.0, "source": None, "live_points": points.get(sleeper_id)}

    position = (card.get("position") or "").upper()
    team = (card.get("team") or "").upper() or None
    projection = _num(card.get("prediction"))
    floor, ceiling = projection_range(projection)
    game = kickoffs.get(team or "", {})
    opponent = (card.get("opponent") or game.get("opponent") or None)
    spread, total, implied = card.get("spread"), card.get("overunder"), card.get("implied_total")
    matchup = (dvp.get(position) or {}).get((opponent or "").upper())
    td = None
    if position in MAIN_PROPS:
        td = td_chance(card.get("anytime_td_prob"), touchdowns_per_game(card["player_id"], frames, position=position) if card.get("player_id") else None,
                       implied)
    return {
        **base,
        "player_id": card.get("player_id"),
        "player_name": card.get("player_name"),
        "position": position,
        "team": team,
        "opponent": opponent,
        "home": game.get("home"),
        "kickoff": game.get("kickoff"),
        "game_final": bool(game.get("final")),
        "bye": opponent in (None, "BYE"),
        "image": card.get("image"),
        "injury_status": card.get("injury_status"),
        "injury_flag": injury_flag(card.get("injury_status")),
        "projection": round(projection, 2),
        "floor": floor if source == "model" else None,
        "ceiling": ceiling if source == "model" else None,
        "source": source,
        "live_points": points.get(sleeper_id),
        "vegas": {"spread": spread, "total": total, "implied_total": implied, "moneyline": card.get("moneyline"),
                  "source": card.get("lines_source")},
        "script": game_script(spread, total, implied) if position in MAIN_PROPS else None,
        "td": td,
        "props": main_props(position, card.get("props")),
        "defense_rank": matchup,
    }


def _lineup_ids(row: dict | None, roster: dict | None) -> list[str]:
    ids = (row or {}).get("starters") or (roster or {}).get("starters") or []
    return [str(x) for x in ids]


async def _side(roster: dict, row: dict | None, owner: dict, league: dict, week: int, external: dict,
                dvp: dict, kickoffs: dict, frames) -> dict:
    slots = [s for s in (league.get("roster_positions") or []) if s not in STARTER_SLOTS_EXCLUDED]
    points = {str(k): _num(v) for k, v in ((row or {}).get("players_points") or {}).items()}
    starters = []
    for i, sid in enumerate(_lineup_ids(row, roster)):
        slot = slots[i] if i < len(slots) else "FLEX"
        if sid in ("0", "", "None"):
            starters.append({"sleeper_id": None, "slot": slot_group(slot), "player_name": "Empty slot", "position": None,
                             "projection": 0.0, "floor": 0.0, "ceiling": 0.0, "source": None, "empty": True})
            continue
        starters.append(await _starter(sid, slot, week, league, external, points, dvp, kickoffs, frames))

    projected = sum(_num(p.get("projection")) for p in starters)
    live = sum((_num(p.get("live_points")) if p.get("game_final") else _num(p.get("projection"))) for p in starters)
    variance = sum(player_sd(_num(p.get("projection"))) ** 2 for p in starters
                   if p.get("source") == "model" and not p.get("game_final"))
    by_group: dict[str, float] = defaultdict(float)
    for p in starters:
        by_group[p["slot"]] += _num(p.get("projection"))
    return {
        "roster_id": int(roster.get("roster_id")),
        "team_name": owner.get("team_name") or f"Team {roster.get('roster_id')}",
        "owner": owner.get("display_name"),
        "record": f"{(roster.get('settings') or {}).get('wins', 0)}-{(roster.get('settings') or {}).get('losses', 0)}",
        "starters": starters,
        "projected_total": round(projected, 1),
        "live_projected_total": round(live, 1),
        "points_so_far": round(_num((row or {}).get("points")), 2),
        "sd": round(math.sqrt(variance), 1),
        "by_group": {k: round(v, 1) for k, v in by_group.items()},
        "bench_ids": [str(x) for x in (roster.get("players") or []) if str(x) not in set(_lineup_ids(row, roster))],
    }


async def _bench_fixes(side: dict, league: dict, week: int) -> list[dict]:
    """For each of your starters who is out, on bye, or doubtful, the best bench player who can fill the slot."""
    trouble = [p for p in side["starters"]
               if p.get("empty") or p.get("bye") or p.get("injury_flag") in ("OUT", "IR", "D", "SUS", "PUP")]
    if not trouble:
        return []
    bench = []
    for sid in side["bench_ids"]:
        if not sid.isdigit():
            continue
        gsis = sl._sleeper_to_gsis(sid)
        card = await _card(gsis, week) if gsis else None
        if card and (card.get("position") or "").upper() in sl.PROJECTED_POSITIONS and not injury_flag(card.get("injury_status")):
            bench.append(card)
    bench.sort(key=lambda c: _num(c.get("prediction")), reverse=True)
    fixes, used = [], set()
    for p in trouble:
        eligible = sl.FLEX_POSITIONS if p["slot"] == "FLEX" else {p.get("position") or p["slot"]}
        pick = next((c for c in bench if c["player_id"] not in used and (c.get("position") or "").upper() in eligible), None)
        reason = "empty slot" if p.get("empty") else "on bye" if p.get("bye") else f"injury: {p.get('injury_status')}"
        if pick:
            used.add(pick["player_id"])
        fixes.append({"out": p.get("player_name"), "slot": p["slot"], "reason": reason,
                      "replace_with": pick and {"player_id": pick["player_id"], "player_name": pick.get("player_name"),
                                                "position": pick.get("position"), "projection": round(_num(pick.get("prediction")), 1)}})
    return fixes


def _shared_games(me: dict, them: dict) -> list[dict]:
    games: dict[str, dict] = {}
    for who, side in (("you", me), ("opponent", them)):
        for p in side["starters"]:
            if not p.get("team") or p.get("bye"):
                continue
            key = " vs ".join(sorted([p["team"], (p.get("opponent") or "").upper()]))
            g = games.setdefault(key, {"game": key, "kickoff": p.get("kickoff"), "you": [], "opponent": []})
            g[who].append(p["player_name"])
    return sorted((g for g in games.values() if g["you"] and g["opponent"]), key=lambda g: -(len(g["you"]) + len(g["opponent"])))


def _group_edges(me: dict, them: dict) -> list[dict]:
    order = ["QB", "RB", "WR", "TE", "FLEX", "K", "DEF"]
    groups = [g for g in order if g in me["by_group"] or g in them["by_group"]]
    groups += [g for g in set(me["by_group"]) | set(them["by_group"]) if g not in groups]
    return [{"group": g, "you": me["by_group"].get(g, 0.0), "opponent": them["by_group"].get(g, 0.0),
             "edge": round(me["by_group"].get(g, 0.0) - them["by_group"].get(g, 0.0), 1)} for g in groups]


def _swing_players(me: dict, them: dict) -> list[dict]:
    """Starters whose range is widest: where the week is most likely to be decided."""
    pool = []
    for who, side in (("you", me), ("opponent", them)):
        for p in side["starters"]:
            if p.get("source") == "model" and not p.get("game_final") and p.get("ceiling") is not None:
                pool.append({"side": who, "player_name": p["player_name"], "position": p["position"],
                             "projection": p["projection"], "floor": p["floor"], "ceiling": p["ceiling"],
                             "td": p.get("td")})
    return sorted(pool, key=lambda x: x["ceiling"] - x["floor"], reverse=True)[:4]


async def head_to_head(league_id: str, roster_id: int, week: int) -> dict:
    league = sl.get_league(league_id)
    if not league:
        raise sl.SleeperError(f"League {league_id} not found")
    rosters = {int(r["roster_id"]): r for r in sl.get_rosters(league_id)}
    if roster_id not in rosters:
        raise sl.SleeperError(f"Roster {roster_id} isn't in league {league_id}")
    owners = {t["roster_id"]: t for t in sl.roster_owners(league_id)}
    rows = {int(r["roster_id"]): r for r in (sl._get(f"/league/{league_id}/matchups/{week}", ttl=60) or [])
            if r.get("roster_id") is not None}

    mine = rows.get(roster_id)
    opponent_id = None
    if mine and mine.get("matchup_id") is not None:
        opponent_id = next((rid for rid, r in rows.items() if rid != roster_id and r.get("matchup_id") == mine["matchup_id"]), None)

    try:
        season = int(league.get("season") or CURRENT_SEASON)
    except (TypeError, ValueError):
        season = CURRENT_SEASON
    external = sl.external_projections(season, week)
    frames = _stats_frames()
    try:
        dvp = defense_vs_position()
    except Exception as exc:  # a missing column shouldn't take the matchup down
        logger.warning("defense vs position unavailable: %s", exc)
        dvp = {}
    kickoffs = _kickoffs(week)

    me = await _side(rosters[roster_id], mine, owners.get(roster_id, {}), league, week, external, dvp, kickoffs, frames)
    result: dict[str, Any] = {"league": {"league_id": league_id, "name": league.get("name"), "week": week,
                                         "scoring_type": league.get("scoring_type")},
                              "you": me, "opponent": None}
    result["bench_fixes"] = await _bench_fixes(me, league, week)
    if opponent_id is None or opponent_id not in rosters:
        result["message"] = f"No opponent scheduled for week {week} (a bye, or the league hasn't set matchups)."
        return result

    them = await _side(rosters[opponent_id], rows.get(opponent_id), owners.get(opponent_id, {}), league, week, external,
                       dvp, kickoffs, frames)
    margin_sd = math.sqrt(me["sd"] ** 2 + them["sd"] ** 2)
    pregame = normal_win_probability(me["projected_total"] - them["projected_total"], margin_sd)
    live = normal_win_probability(me["live_projected_total"] - them["live_projected_total"], margin_sd)
    result.update({
        "opponent": them,
        "win_probability": round(pregame, 3),
        "live_win_probability": round(live, 3),
        "margin_sd": round(margin_sd, 1),
        "group_edges": _group_edges(me, them),
        "shared_games": _shared_games(me, them),
        "swing_players": _swing_players(me, them),
        "method": [
            "Lineups are the ones set in Sleeper for this week.",
            "Floor/ceiling are 10th/90th percentiles of each projection's typical miss.",
            "Win probability treats players as independent; read it as a guide.",
            "TD chance is Bovada's anytime price when posted, otherwise a model estimate from recent TD rate and the team's implied points.",
            "Defense rank 1 allows the most fantasy points to that position (this season counted double, plus last season).",
            "Kickers and defenses use Sleeper's projection.",
        ],
    })
    return result
