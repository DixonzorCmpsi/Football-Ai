"""Backtest how a projection is assembled from its parts, against real outcomes.

The position models predict a *deviation*; `services/prediction.py` turns that
into points by adding it to a baseline. How that happens decides whether
projections are honest, and it used to be chosen by feel. This script decides it
by evidence:

1. Rebuild features for past seasons with the same functions the production
   feature step (rag_data/13_generate_production_features.py) uses.
2. Run the shipped position models over every player-week.
3. Score candidate formulas on fantasy-relevant players (season average >= 8):
   average miss, bias, and how often same-week, same-position pairs are ordered
   correctly -- which is the start/sit decision.

Candidates are picked on FIT_SEASON and then reported on TEST_SEASON, so the
winner is not simply the one that memorised a season.

    cd backend && .venv/Scripts/python model_training/backtest_projection_formula.py

Needs rag_data/weekly_player_stats_<season>.csv and snap counts for the seasons
below (the ETL caches them) and nflreadpy for game scores.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BACKEND = Path(__file__).resolve().parents[1]
RAG = BACKEND / "rag_data"
APP = BACKEND / "applications"
sys.path.insert(0, str(RAG))
sys.path.insert(0, str(BACKEND))

SEASONS = [2023, 2024, 2025]
FIT_SEASON, TEST_SEASON = 2024, 2025
POSITIONS = ["QB", "RB", "WR", "TE"]
CARRYOVER_GAMES = 4
Y = "y_fantasy_points_ppr"


def build_frame() -> pd.DataFrame:
    cwd = os.getcwd()
    os.chdir(RAG)
    try:
        fh = importlib.import_module("feature_history")
        step13 = importlib.import_module("13_generate_production_features")
        profiles = pd.read_csv(RAG / "player_profiles_2026.csv", low_memory=False)
        df = step13.load_prior_seasons(SEASONS, profiles)
    finally:
        os.chdir(cwd)
    try:
        import nflreadpy as nfl
        sched = nfl.load_schedules(seasons=SEASONS).to_pandas()
        sched = sched[["season", "week", "home_team", "away_team", "home_score", "away_score"]]
    except Exception as exc:  # offline: points-based opponent features stay 0
        print(f"schedules unavailable ({exc})")
        sched = pd.DataFrame()
    team_off = fh.attach_game_points(fh.team_offense_from_players(df), sched)
    df = fh.add_opponent_features(df, team_off, fh.mirror_to_defense(team_off))
    df = fh.add_defense_vs_position(df)
    df = fh.add_player_lags(df, fh.LAG_COLUMNS)
    df = df.rename(columns={
        "rolling_avg_def_sacks_4_weeks": "rolling_avg_sack_4_weeks",
        "rolling_avg_def_interceptions_4_weeks": "rolling_avg_interception_4_weeks",
        "rolling_avg_def_qb_hits_4_weeks": "rolling_avg_qb_hit_4_weeks",
        "opp_def_def_sacks_lag_1": "opp_def_sack_lag_1",
    })
    df = df.sort_values(["player_id", "season", "week"]).reset_index(drop=True)
    df[Y] = pd.to_numeric(df[Y], errors="coerce").fillna(0.0)
    return df


def add_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """The two baseline ingredients, computed the way production computes them."""

    def per_player(g: pd.DataFrame) -> pd.DataFrame:
        pts, seasons = g[Y].tolist(), g["season"].tolist()
        recent, season_avg, has_history = [], [], []
        for i in range(len(g)):
            s = seasons[i]
            cur = [p for p, ss in zip(pts[:i], seasons[:i]) if ss == s]
            earlier = [(p, ss) for p, ss in zip(pts[:i], seasons[:i]) if ss < s]
            last_season = max((ss for _, ss in earlier), default=None)
            prev = [p for p, ss in earlier if ss == last_season]
            has_history.append(bool(cur or prev))

            # recent form: last 4 non-zero games, crossfaded with last season's
            nz = [p for p in reversed(cur) if p > 0][:CARRYOVER_GAMES]
            cur_nz = sum(nz) / len(nz) if nz else 0.0
            prev_nz = [p for p in reversed(prev) if p > 0][:CARRYOVER_GAMES]
            if len(nz) >= CARRYOVER_GAMES or not prev_nz:
                recent.append(cur_nz)
            else:
                w = len(nz) / CARRYOVER_GAMES
                recent.append(w * cur_nz + (1 - w) * sum(prev_nz) / len(prev_nz))

            # season average: every game, crossfaded with last season's average
            ca = sum(cur) / len(cur) if cur else 0.0
            if len(cur) >= CARRYOVER_GAMES or not prev:
                season_avg.append(ca)
            else:
                w = len(cur) / CARRYOVER_GAMES
                season_avg.append(w * ca + (1 - w) * sum(prev) / len(prev))
        return pd.DataFrame({"recent_form": recent, "season_avg": season_avg,
                             "has_history": has_history}, index=g.index)

    parts = df.groupby("player_id", group_keys=False).apply(per_player)
    return df.join(parts)


def add_model_deviation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["dev"] = 0.0
    for pos in POSITIONS:
        model = joblib.load(APP / f"xgboost_{pos}_sliding_window_deviation_v1.joblib")
        feats = json.load(open(APP / f"feature_names_{pos}_sliding_window_deviation_v1.json"))
        m = df["position"] == pos
        X = df.loc[m].reindex(columns=feats).apply(pd.to_numeric, errors="coerce").fillna(0.0)
        df.loc[m, "dev"] = model.predict(X.to_numpy())
    return df


def score(frame: pd.DataFrame, pred: pd.Series) -> tuple[float, float, float]:
    rel = frame["season_avg"] >= 8
    err = (pred - frame[Y])[rel]
    right = total = 0
    sub = frame[rel].assign(pred=pred[rel])
    for _, g in sub.groupby(["week", "position"]):
        p, a = g["pred"].to_numpy(), g[Y].to_numpy()
        dp, da = np.sign(p[:, None] - p[None, :]), np.sign(a[:, None] - a[None, :])
        mask = (dp != 0) & (da != 0)
        right += int((dp[mask] == da[mask]).sum())
        total += int(mask.sum())
    return float(err.abs().mean()), float(err.mean()), right / max(total, 1)


def main() -> None:
    df = add_model_deviation(add_baselines(build_frame()))
    df = df[df["season"].isin([FIT_SEASON, TEST_SEASON]) & (df["week"] <= 18)
            & df["position"].isin(POSITIONS)]
    fit, test = df[df["season"] == FIT_SEASON], df[df["season"] == TEST_SEASON]

    def old_formula(f):
        amp = np.sign(f["dev"]) * 5.0 * np.log1p(f["dev"].abs())
        return (f["recent_form"] + amp).clip(lower=0)

    center = fit.groupby("position")["dev"].mean().to_dict()
    candidates = {}
    for w in (0.0, 0.25, 0.5, 1.0):
        for k in (0.0, 0.5, 1.0):
            for centered in (False, True):
                def formula(f, w=w, k=k, centered=centered):
                    d = f["dev"] - (f["position"].map(center) if centered else 0.0)
                    return ((1 - w) * f["season_avg"] + w * f["recent_form"] + k * d).clip(lower=0)
                candidates[f"recent {w:.2f} · dev x{k:.1f}{' centered' if centered else ''}"] = formula

    ranked = sorted(candidates.items(), key=lambda kv: score(fit, kv[1](fit))[0])
    print(f"position deviation means, {FIT_SEASON}: "
          + ", ".join(f"{p} {v:+.2f}" for p, v in sorted(center.items())))
    print(f"\n{'formula':34} | {FIT_SEASON} miss  bias  pairs | {TEST_SEASON} miss  bias  pairs")
    rows = [("OLD: last-4 non-zero + 5ln(1+dev)", old_formula)] + ranked[:6]
    for name, formula in rows:
        a, b = score(fit, formula(fit)), score(test, formula(test))
        print(f"{name:34} | {a[0]:9.2f} {a[1]:+5.2f} {a[2]:6.3f} | {b[0]:9.2f} {b[1]:+5.2f} {b[2]:6.3f}")

    # What production actually ships, through its own code path.
    from applications.api.services import prediction as pr
    shipped = []
    for f in (fit, test):
        vals = [sum(pr.combine_projection(r.season_avg, r.recent_form, r.dev, r.position, r.has_history))
                for r in f.itertuples()]
        shipped.append(pd.Series(vals, index=f.index).clip(lower=0))
    a, b = score(fit, shipped[0]), score(test, shipped[1])
    print(f"{'SHIPPED (prediction.py)':34} | {a[0]:9.2f} {a[1]:+5.2f} {a[2]:6.3f} | {b[0]:9.2f} {b[1]:+5.2f} {b[2]:6.3f}")


if __name__ == "__main__":
    main()
