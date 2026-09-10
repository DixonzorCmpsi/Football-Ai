package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
)

// PoolPlayer is the JSON shape returned by /tier_list/pool/{position},
// mirroring routes.tier_list.get_position_pool.
type PoolPlayer struct {
	PlayerID     string     `json:"player_id"`
	PlayerName   string     `json:"player_name"`
	Position     string     `json:"position"`
	Team         string     `json:"team"`
	Image        string     `json:"image"`
	InjuryStatus string     `json:"injury_status"`
	IsRookie     bool       `json:"is_rookie"`
	DraftYear    *int       `json:"draft_year"`
	DraftNumber  *int       `json:"draft_number"`
	Age          *int       `json:"age"`
	Height       *int       `json:"height"`
	Weight       *int       `json:"weight"`
	Season       int        `json:"season"`
	StatsSeason  int        `json:"stats_season"`
	Stats        *PlayerAgg `json:"stats"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *DataStore) handlePool(w http.ResponseWriter, r *http.Request) {
	pos := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "position")))
	if pos != "ALL" && !validPositions[pos] {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"detail": "Invalid position. Must be one of QB/RB/WR/TE or ALL",
		})
		return
	}
	includeRookiesOnly := r.URL.Query().Get("include_rookies_only") == "true"

	results := make([]PoolPlayer, 0, len(s.Profiles))
	for _, p := range s.Profiles {
		if pos == "ALL" {
			if !validPositions[strings.ToUpper(p.Position)] {
				continue
			}
		} else if strings.ToUpper(p.Position) != pos {
			continue
		}

		isRookie := p.DraftYear != nil && *p.DraftYear == CurrentSeason
		if includeRookiesOnly && !isRookie {
			continue
		}

		agg := s.StatsByPID[p.PlayerID]
		if agg == nil {
			agg = &PlayerAgg{} // all-zero, matches Python's `agg.get(..., 0)`
		}

		playerPos := pos
		if pos == "ALL" {
			playerPos = strings.ToUpper(p.Position)
		}

		team := p.TeamAbbr
		if team == "" {
			team = "FA"
		}

		results = append(results, PoolPlayer{
			PlayerID:     p.PlayerID,
			PlayerName:   p.PlayerName,
			Position:     playerPos,
			Team:         team,
			Image:        headshotURL(p),
			InjuryStatus: "Active", // injury_map is empty in the CSV-only path
			IsRookie:     isRookie,
			DraftYear:    p.DraftYear,
			DraftNumber:  p.DraftNumber,
			Age:          p.Age,
			Height:       p.Height,
			Weight:       p.Weight,
			Season:       CurrentSeason,
			StatsSeason:  CurrentSeason,
			Stats:        agg,
		})
	}

	// Default sort: highest season_avg_pts first, rookies ahead of ties.
	// Stable, matching Python's reverse=True on a stable sort.
	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.Stats.SeasonAvgPts != b.Stats.SeasonAvgPts {
			return a.Stats.SeasonAvgPts > b.Stats.SeasonAvgPts
		}
		if a.IsRookie != b.IsRookie {
			return a.IsRookie && !b.IsRookie
		}
		return false
	})

	writeJSON(w, http.StatusOK, results)
}

func (s *DataStore) handleCurrentWeek(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]int{"week": s.CurrentWeek})
}

func (s *DataStore) handleHealth(w http.ResponseWriter, r *http.Request) {
	ready := len(s.Profiles) > 0
	status := "ok"
	if !ready {
		status = "starting"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        status,
		"ready":         ready,
		"current_week":  s.CurrentWeek,
		"data_loaded_at": s.LoadedAt,
		"data_counts": map[string]int{
			"df_profile":       len(s.Profiles),
			"df_player_stats":  len(s.StatsByPID),
		},
	})
}
