"""Compare Go (:8002) vs Python (:8001) pool output for exact-enough parity."""
import json
import sys
import urllib.request

POSITIONS = ["QB", "RB", "WR", "TE", "ALL"]
FLOAT_TOL = {"season_total_pts": 0.11, "season_avg_pts": 0.02, "recent_avg_pts": 0.02, "snap_pct_avg": 0.02}
INT_FIELDS = ["games_played", "boom_games", "bust_games", "total_yds", "total_tds",
              "total_receptions", "total_targets", "total_carries", "snaps_total"]
TOP_FIELDS = ["player_id", "player_name", "position", "team", "image", "injury_status",
              "is_rookie", "draft_year", "draft_number", "age", "height", "weight",
              "season", "stats_season"]


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def compare_pos(pos):
    py = fetch(f"http://localhost:8001/tier_list/pool/{pos}")
    go = fetch(f"http://localhost:8002/tier_list/pool/{pos}")
    diffs = []
    if len(py) != len(go):
        diffs.append(f"length py={len(py)} go={len(go)}")
        return diffs
    order_mismatch = 0
    for i, (p, g) in enumerate(zip(py, go)):
        if p["player_id"] != g["player_id"]:
            order_mismatch += 1
            if order_mismatch <= 5:
                diffs.append(f"[{i}] order: py={p['player_id']}({p['stats']['season_avg_pts']}) go={g['player_id']}({g['stats']['season_avg_pts']})")
            continue
        for f in TOP_FIELDS:
            if p.get(f) != g.get(f):
                diffs.append(f"[{i}] {p['player_id']} {f}: py={p.get(f)!r} go={g.get(f)!r}")
        ps, gs = p["stats"], g["stats"]
        for f in INT_FIELDS:
            if ps.get(f) != gs.get(f):
                diffs.append(f"[{i}] {p['player_id']} stats.{f}: py={ps.get(f)} go={gs.get(f)}")
        for f, tol in FLOAT_TOL.items():
            a, b = ps.get(f, 0) or 0, gs.get(f, 0) or 0
            if abs(a - b) > tol:
                diffs.append(f"[{i}] {p['player_id']} stats.{f}: py={a} go={b} (diff {abs(a-b):.3f})")
    if order_mismatch:
        diffs.append(f"TOTAL order mismatches: {order_mismatch}/{len(py)}")
    return diffs


def main():
    total = 0
    for pos in POSITIONS:
        d = compare_pos(pos)
        total += len(d)
        status = "OK" if not d else f"{len(d)} DIFFS"
        print(f"=== {pos}: {status} ===")
        for line in d[:20]:
            print("   ", line)
    print("\nRESULT:", "PARITY OK" if total == 0 else f"{total} differences")
    sys.exit(0 if total == 0 else 1)


if __name__ == "__main__":
    main()
