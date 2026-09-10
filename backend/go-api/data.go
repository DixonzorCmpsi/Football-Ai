package main

import (
	"encoding/csv"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// CurrentSeason mirrors config.get_current_season() for the benchmark window
// (date 2026-06 -> 2026). Kept as a constant so Go and the Python harness agree.
const CurrentSeason = 2026

// validPositions mirrors tier_list.VALID_POSITIONS.
var validPositions = map[string]bool{"QB": true, "RB": true, "WR": true, "TE": true}

// ---------------------------------------------------------------- CSV plumbing

// table is a parsed CSV: a header->index map plus the raw rows.
type table struct {
	colIdx map[string]int
	rows   [][]string
}

func loadCSV(path string) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // tolerate ragged rows, like polars ignore_errors
	r.ReuseRecord = false

	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	colIdx := make(map[string]int, len(header))
	for i, name := range header {
		colIdx[strings.TrimSpace(name)] = i
	}

	var rows [][]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip malformed line (parity with ignore_errors=True).
			continue
		}
		rows = append(rows, rec)
	}
	return &table{colIdx: colIdx, rows: rows}, nil
}

func (t *table) has(col string) bool {
	_, ok := t.colIdx[col]
	return ok
}

// cell returns the raw string for a column, or "" if the column/row is absent.
func cell(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return row[idx]
}

// ------------------------------------------------------------------- helpers

// numberRow mirrors tier_list._number: first parseable column wins, else 0.
func numberRow(row []string, t *table, names ...string) float64 {
	for _, name := range names {
		idx, ok := t.colIdx[name]
		if !ok {
			continue
		}
		s := strings.TrimSpace(cell(row, idx))
		if s == "" {
			continue
		}
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return 0.0
}

// num pulls a single optional numeric cell, returning 0 when blank/unparseable.
func num(row []string, t *table, name string) float64 {
	idx, ok := t.colIdx[name]
	if !ok {
		return 0.0
	}
	s := strings.TrimSpace(cell(row, idx))
	if s == "" {
		return 0.0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0
	}
	return v
}

// roundTo mimics Python's round() (round-half-to-even) at the given precision.
// FormatFloat rounds the float64 directly with round-to-nearest/ties-to-even —
// the same rule and same input bits as CPython's round(), so results match.
// (Scaling by 10^n and rounding, by contrast, perturbs the value before the
// rounding decision and can flip boundary cases.)
func roundTo(x float64, places int) float64 {
	v, _ := strconv.ParseFloat(strconv.FormatFloat(x, 'f', places, 64), 64)
	return v
}

// calculateFantasyPoints mirrors services.utils.calculate_fantasy_points.
func calculateFantasyPoints(row []string, t *table) float64 {
	if idx, ok := t.colIdx["y_fantasy_points_ppr"]; ok {
		s := strings.TrimSpace(cell(row, idx))
		if s != "" {
			if v, err := strconv.ParseFloat(s, 64); err == nil {
				return v
			}
		}
	}
	pYds := num(row, t, "passing_yards")
	pTds := num(row, t, "passing_touchdown")
	rYds := num(row, t, "rushing_yards")
	rTds := num(row, t, "rush_touchdown")
	recYds := num(row, t, "receiving_yards")
	recTds := num(row, t, "receiving_touchdown")
	receptions := num(row, t, "receptions")
	ints := num(row, t, "interceptions")
	fumbles := num(row, t, "fumbles_lost")
	return pYds*0.04 + pTds*4.0 + rYds*0.1 + rTds*6.0 + recYds*0.1 + recTds*6.0 +
		receptions*1.0 - ints*2.0 - fumbles*2.0
}

var defStatCols = []string{
	"def_tackles_solo", "def_tackle_assists", "def_tackles_for_loss", "def_sacks",
	"def_qb_hits", "def_interceptions", "def_pass_defended", "def_passes_defended",
	"pass_defended", "def_fumbles_forced", "fumble_recovery_opp",
	"def_fumble_recoveries", "def_tds", "fumble_recovery_tds", "def_safeties",
}

func hasDefensiveStats(row []string, t *table) bool {
	for _, c := range defStatCols {
		if numberRow(row, t, c) > 0 {
			return true
		}
	}
	return false
}

// calculateDefensivePoints mirrors tier_list._calculate_defensive_points.
func calculateDefensivePoints(row []string, t *table) float64 {
	solo := numberRow(row, t, "def_tackles_solo")
	assisted := numberRow(row, t, "def_tackle_assists")
	tfl := numberRow(row, t, "def_tackles_for_loss")
	sacks := numberRow(row, t, "def_sacks")
	qbHits := numberRow(row, t, "def_qb_hits")
	interceptions := numberRow(row, t, "def_interceptions")
	passesDefended := numberRow(row, t, "def_pass_defended", "def_passes_defended", "pass_defended")
	forcedFumbles := numberRow(row, t, "def_fumbles_forced")
	recoveries := numberRow(row, t, "fumble_recovery_opp", "def_fumble_recoveries")
	touchdowns := numberRow(row, t, "def_tds") + numberRow(row, t, "fumble_recovery_tds")
	safeties := numberRow(row, t, "def_safeties")
	return solo*1.5 + assisted*0.75 + tfl*2.0 + sacks*4.0 + qbHits*1.0 +
		interceptions*6.0 + passesDefended*1.5 + forcedFumbles*4.0 + recoveries*4.0 +
		touchdowns*6.0 + safeties*2.0
}

// ------------------------------------------------------------ aggregation

// PlayerAgg holds the 13 summary fields the pool endpoint surfaces.
type PlayerAgg struct {
	GamesPlayed     int     `json:"games_played"`
	SeasonTotalPts  float64 `json:"season_total_pts"`
	SeasonAvgPts    float64 `json:"season_avg_pts"`
	RecentAvgPts    float64 `json:"recent_avg_pts"`
	BoomGames       int     `json:"boom_games"`
	BustGames       int     `json:"bust_games"`
	TotalYds        int     `json:"total_yds"`
	TotalTds        int     `json:"total_tds"`
	TotalReceptions int     `json:"total_receptions"`
	TotalTargets    int     `json:"total_targets"`
	TotalCarries    int     `json:"total_carries"`
	SnapsTotal      int     `json:"snaps_total"`
	SnapPctAvg      float64 `json:"snap_pct_avg"`
}

// aggregatePlayerStats mirrors tier_list._aggregate_player_stats. The snap-count
// join is intentionally omitted: the snap CSV keys on pfr_id (no player_id
// column), so the Python path's `"player_id" in snaps_df.columns` guard is false
// and snaps are never merged. We match that exactly.
func aggregatePlayerStats(stats *table) map[string]*PlayerAgg {
	out := map[string]*PlayerAgg{}
	if stats == nil || len(stats.rows) == 0 {
		return out
	}
	pidIdx, ok := stats.colIdx["player_id"]
	if !ok {
		return out
	}
	weekIdx := stats.colIdx["week"] // may be absent (-> 0 default below)

	groups := map[string][][]string{}
	var order []string
	for _, row := range stats.rows {
		pid := strings.TrimSpace(cell(row, pidIdx))
		if pid == "" {
			continue
		}
		if _, seen := groups[pid]; !seen {
			order = append(order, pid)
		}
		groups[pid] = append(groups[pid], row)
	}

	for _, pid := range order {
		rows := groups[pid]
		gamesPlayed := len(rows)
		if gamesPlayed == 0 {
			continue
		}

		hasDef := false
		for _, r := range rows {
			if hasDefensiveStats(r, stats) {
				hasDef = true
				break
			}
		}

		pts := make([]float64, 0, len(rows))
		for _, r := range rows {
			if hasDef {
				pts = append(pts, calculateDefensivePoints(r, stats))
			} else {
				pts = append(pts, calculateFantasyPoints(r, stats))
			}
		}

		seasonTotal := 0.0
		boom, bust := 0, 0
		for _, p := range pts {
			seasonTotal += p
			if p >= 20 {
				boom++
			}
			if p > 0 && p < 5 {
				bust++
			}
		}
		seasonAvg := 0.0
		if gamesPlayed > 0 {
			seasonAvg = seasonTotal / float64(gamesPlayed)
		}

		// Recent form: last 4 non-zero outings by descending week (stable).
		sortedRows := make([][]string, len(rows))
		copy(sortedRows, rows)
		sort.SliceStable(sortedRows, func(i, j int) bool {
			return weekOf(sortedRows[i], weekIdx) > weekOf(sortedRows[j], weekIdx)
		})
		recent := make([]float64, 0, 4)
		for _, r := range sortedRows {
			var p float64
			if hasDef {
				p = calculateDefensivePoints(r, stats)
			} else {
				p = calculateFantasyPoints(r, stats)
			}
			if p > 0 {
				recent = append(recent, p)
			}
			if len(recent) >= 4 {
				break
			}
		}
		recentAvg := 0.0
		if len(recent) > 0 {
			s := 0.0
			for _, p := range recent {
				s += p
			}
			recentAvg = s / float64(len(recent))
		}

		totalYds, totalTds := 0.0, 0.0
		totalRec, totalTgt, totalCar := 0.0, 0.0, 0.0
		for _, r := range rows {
			totalYds += num(r, stats, "passing_yards") + num(r, stats, "rushing_yards") + num(r, stats, "receiving_yards")
			totalTds += num(r, stats, "passing_touchdown") + num(r, stats, "rush_touchdown") + num(r, stats, "receiving_touchdown")
			totalRec += num(r, stats, "receptions")
			totalTgt += num(r, stats, "targets")
			totalCar += num(r, stats, "rush_attempts")
		}

		out[pid] = &PlayerAgg{
			GamesPlayed:     gamesPlayed,
			SeasonTotalPts:  roundTo(seasonTotal, 1),
			SeasonAvgPts:    roundTo(seasonAvg, 2),
			RecentAvgPts:    roundTo(recentAvg, 2),
			BoomGames:       boom,
			BustGames:       bust,
			TotalYds:        int(totalYds),
			TotalTds:        int(totalTds),
			TotalReceptions: int(totalRec),
			TotalTargets:    int(totalTgt),
			TotalCarries:    int(totalCar),
			SnapsTotal:      0,
			SnapPctAvg:      0.0,
		}
	}
	return out
}

func weekOf(row []string, weekIdx int) float64 {
	if weekIdx < 0 {
		return 0
	}
	s := strings.TrimSpace(cell(row, weekIdx))
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

// ------------------------------------------------------------ profiles + store

type Profile struct {
	PlayerID    string
	PlayerName  string
	Position    string
	TeamAbbr    string
	Headshot    string
	DraftYear   *int
	DraftNumber *int
	Age         *int
	Height      *int
	Weight      *int
}

// DataStore is the in-memory equivalent of Python's model_data for this slice.
type DataStore struct {
	Profiles    []Profile
	StatsByPID  map[string]*PlayerAgg
	CurrentWeek int
	LoadedAt    string
}

func parseIntPtr(s string) *int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	// Some numeric CSV cells arrive as floats ("178.0"); truncate like int().
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		v := int(f)
		return &v
	}
	return nil
}

func loadProfiles(t *table) []Profile {
	out := make([]Profile, 0, len(t.rows))
	gi := func(name string) int {
		if idx, ok := t.colIdx[name]; ok {
			return idx
		}
		return -1
	}
	pid, pfrName := gi("player_id"), gi("player_name")
	posI, teamI, headI := gi("position"), gi("team_abbr"), gi("headshot")
	dyI, dnI, ageI, hI, wI := gi("draft_year"), gi("draft_number"), gi("age"), gi("height"), gi("weight")
	for _, row := range t.rows {
		id := strings.TrimSpace(cell(row, pid))
		if id == "" {
			continue
		}
		out = append(out, Profile{
			PlayerID:    id,
			PlayerName:  cell(row, pfrName),
			Position:    strings.TrimSpace(cell(row, posI)),
			TeamAbbr:    strings.TrimSpace(cell(row, teamI)),
			Headshot:    cell(row, headI),
			DraftYear:   parseIntPtr(cell(row, dyI)),
			DraftNumber: parseIntPtr(cell(row, dnI)),
			Age:         parseIntPtr(cell(row, ageI)),
			Height:      parseIntPtr(cell(row, hI)),
			Weight:      parseIntPtr(cell(row, wI)),
		})
	}
	return out
}

const defaultHeadshot = "https://sleepercdn.com/images/v2/icons/player_default.webp"

func headshotURL(p Profile) string {
	if strings.Contains(p.Headshot, "http") {
		return p.Headshot
	}
	return defaultHeadshot
}
