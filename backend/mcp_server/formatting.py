"""Shaping backend responses into something an agent can actually reason over.

The REST API is built for a UI: a single matchup returns ~31 player cards, each
with a dozen prop rows and a headshot URL. Handing that to a model verbatim
burns context on data it cannot use and buries the few numbers that matter.

Everything here is pure so it can be tested without a running backend.
"""

from __future__ import annotations

from typing import Any


def _num(value, default=0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if out == out else default  # NaN guard


def _fmt(value, digits=1) -> str:
    n = _num(value, None)
    return "-" if n is None else f"{n:.{digits}f}"


def player_line(card: dict, include_props: bool = False) -> str:
    """One compact line for a player card."""
    if not card:
        return ""
    bits = [
        f"{card.get('player_name', '?')}",
        f"({card.get('position', '?')} {card.get('team', '?')}",
    ]
    opp = card.get("opponent")
    bits[-1] += f" vs {opp})" if opp else ")"
    bits.append(f"proj {_fmt(card.get('prediction'))}")
    floor = card.get("floor_prediction")
    if floor is not None:
        bits.append(f"floor {_fmt(floor)}")
    status = card.get("injury_status")
    if status and str(status).lower() not in ("active", "act", "none", ""):
        bits.append(f"[{status}]")
    snap = card.get("snap_percentage")
    if snap:
        # The API already returns 0-100; multiplying again produced "snaps 8400%".
        bits.append(f"snaps {_fmt(snap, 0)}%")

    line = " ".join(bits)
    if include_props:
        props = props_summary(card.get("props") or [])
        if props:
            line += f"\n    props: {props}"
    return line


def props_summary(props: list, limit: int = 8) -> str:
    """Collapse prop rows to `Market line (odds)` pairs, over-side only.

    Both sides of a total carry the same line, so printing them doubles the text
    without adding information; the odds of the over is the part that moves.
    """
    if not props:
        return ""
    seen, out = set(), []
    for p in props:
        market = p.get("prop_type")
        if not market:
            continue
        side = str(p.get("side") or "").lower()
        if side == "under":
            continue
        key = (market, p.get("line"))
        if key in seen:
            continue
        seen.add(key)
        line = p.get("line")
        odds = p.get("odds")
        prob = p.get("implied_prob")
        piece = f"{market} {_fmt(line)}"
        if odds:
            piece += f" ({odds}"
            piece += f", {_fmt(prob)}%)" if prob else ")"
        out.append(piece)
        if len(out) >= limit:
            break
    return "; ".join(out)


def summarize_matchup(data: dict, week: int, home: str, away: str,
                      top_n: int = 8) -> str:
    """Game context plus the players worth talking about, not the whole roster."""
    if not data:
        return f"No matchup data for {away} @ {home} in week {week}."

    lines = [f"Week {week}: {away} @ {home}"]

    ou = data.get("over_under") or data.get("overunder")
    spread = data.get("spread")
    if ou is not None or spread is not None:
        bits = []
        if ou is not None:
            bits.append(f"O/U {_fmt(ou)}")
        if spread is not None:
            bits.append(f"spread {_fmt(spread)}")
        lines.append("Line: " + ", ".join(bits))

    weather = data.get("weather") or {}
    if weather:
        w = []
        if weather.get("is_dome"):
            w.append("dome")
        else:
            if weather.get("temp_f") is not None:
                w.append(f"{_fmt(weather['temp_f'], 0)}F")
            if weather.get("wind_mph") is not None:
                w.append(f"wind {_fmt(weather['wind_mph'], 0)}mph")
            if weather.get("condition"):
                w.append(str(weather["condition"]))
        if w:
            lines.append("Weather: " + ", ".join(w))

    for side, team in (("away_roster", away), ("home_roster", home)):
        roster = data.get(side) or []
        if not roster:
            continue
        ranked = sorted(roster, key=lambda c: _num(c.get("prediction")), reverse=True)
        lines.append(f"\n{team} top {min(top_n, len(ranked))} by projection:")
        for card in ranked[:top_n]:
            lines.append("  " + player_line(card))

    for side, team in (("away_injuries", away), ("home_injuries", home)):
        report = data.get(side) or []
        hurt = [r for r in report
                if str(r.get("status") or "").strip().lower() not in ("active", "act", "na", "")]
        if hurt:
            lines.append(f"\n{team} injuries ({len(hurt)}):")
            for r in hurt[:12]:
                starter = "starter " if r.get("is_starter") else ""
                lines.append(f"  {r.get('name')} ({r.get('position')}) {starter}- {r.get('status')}")

    return "\n".join(lines)


def summarize_roster_analysis(data: dict) -> str:
    """Sleeper lineup analysis: the decision, then the evidence."""
    if not data:
        return "No analysis returned."
    league = data.get("league") or {}
    lines = [
        f"League: {league.get('name', '?')} ({league.get('scoring_type', '?')}) "
        f"- roster {data.get('roster_id')}, week {data.get('week')}",
        f"Projected starting total: {_fmt(data.get('projected_total'))} pts",
    ]

    start = data.get("bench_but_should_start") or []
    sit = data.get("start_but_should_sit") or []
    if start or sit:
        lines.append("\nChanges vs the saved lineup:")
        for c in start:
            lines.append(f"  START {player_line(c)}")
        for c in sit:
            lines.append(f"  SIT   {player_line(c)}")
    else:
        lines.append("\nSaved lineup already matches the projection.")

    rec = data.get("recommended_starters") or []
    if rec:
        lines.append("\nRecommended starters:")
        for c in rec:
            lines.append("  " + player_line(c))

    unmatched = data.get("unmatched_sleeper_ids") or []
    if unmatched:
        lines.append(f"\n{len(unmatched)} rostered player(s) could not be matched to the "
                     "player database (usually kickers or team defenses, which this model "
                     "does not project).")
    return "\n".join(lines)


def summarize_waivers(data: dict, limit: int = 15) -> str:
    if not data:
        return "No waiver data returned."
    players = data.get("players") or []
    if not players:
        return "No available free agents matched."
    lines = [
        f"Week {data.get('week')} free agents, ranked by projection "
        f"({data.get('rostered', 0)} players rostered leaguewide):"
    ]
    for c in players[:limit]:
        line = "  " + player_line(c)
        adds = c.get("trending_adds")
        if adds:
            line += f" (+{adds} adds/24h)"
        lines.append(line)
    return "\n".join(lines)


def summarize_search(results: list, limit: int = 15) -> str:
    if not results:
        return "No players matched."
    lines = [f"{len(results)} match(es):"]
    for r in results[:limit]:
        lines.append(
            f"  {r.get('player_name')} - {r.get('position')} {r.get('team_abbr')} "
            f"[id {r.get('player_id')}] {r.get('status') or ''}".rstrip()
        )
    if len(results) > limit:
        lines.append(f"  ... and {len(results) - limit} more")
    return "\n".join(lines)


# /player/history uses its own column names (points, passing_yds, ...) rather than
# the weekly-stats names, so reading fantasy_points_ppr off it produced a game log
# of "- pts". Accept both spellings so this survives either changing.
_HISTORY_POINTS = ("points", "fantasy_points_ppr", "fantasy_points")
_HISTORY_STATS = (
    (("passing_yds", "passing_yards"), "pass yd"),
    (("rushing_yds", "rushing_yards"), "rush yd"),
    (("receiving_yds", "receiving_yards"), "rec yd"),
    (("receptions",), "rec"),
    (("touchdowns",), "TD"),
)


def _first(row: dict, keys):
    for k in keys:
        if row.get(k) not in (None, ""):
            return row[k]
    return None


def summarize_history(data: Any, limit: int = 12) -> str:
    """Weekly game log, newest first."""
    rows = data.get("history") if isinstance(data, dict) else data
    if not rows:
        return "No history available."
    lines = ["Recent games (newest first):"]
    for r in list(rows)[:limit]:
        season, week = r.get("season"), r.get("week")
        label = f"  {season} wk{week}" if season else f"  wk{week}"
        opp = r.get("opponent")
        if opp:
            label += f" vs {opp}"
        bits = [f"{_fmt(_first(r, _HISTORY_POINTS))} pts"]
        for keys, tag in _HISTORY_STATS:
            value = _first(r, keys)
            if value:
                bits.append(f"{_fmt(value, 0)} {tag}")
        lines.append(f"{label}: " + ", ".join(bits))
    return "\n".join(lines)

def summarize_storylines(rows: list, limit: int = 5) -> str:
    if not rows:
        return "No recent storylines for this player."
    lines = []
    for s in rows[:limit]:
        published = s.get("published") or s.get("fetched_at") or ""
        lines.append(f"- {s.get('headline', '(no headline)')} [{published}]")
        desc = s.get("description")
        if desc:
            lines.append(f"    {desc}")
    return "\n".join(lines)


def summarize_schedule(games: list, week: int) -> str:
    if not games:
        return f"No games found for week {week}."
    lines = [f"Week {week} schedule ({len(games)} games):"]
    for g in games:
        away, home = g.get("away_team"), g.get("home_team")
        bit = f"  {away} @ {home}"
        if g.get("home_score") is not None and g.get("away_score") is not None:
            bit += f"  FINAL {away} {g['away_score']} - {home} {g['home_score']}"
        elif g.get("gameday"):
            bit += f"  {g['gameday']}"
        ou = g.get("over_under") or g.get("total")
        if ou:
            bit += f"  O/U {_fmt(ou)}"
        lines.append(bit)
    return "\n".join(lines)
