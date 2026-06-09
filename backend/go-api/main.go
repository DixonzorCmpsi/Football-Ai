// Command go-api is a Go port of a read-heavy slice of the FastAPI backend
// (the tier-list position pool plus current_week/health), built to benchmark
// Go vs Python on identical CSV-backed data. See README.md.
package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	ragDir := flag.String("rag-dir", "../rag_data", "directory holding the CSV data files")
	statsSeason := flag.Int("stats-season", 2025, "season whose weekly_player_stats CSV to load")
	addr := flag.String("addr", ":8002", "listen address")
	flag.Parse()

	store, err := loadStore(*ragDir, *statsSeason)
	if err != nil {
		log.Fatalf("data load failed: %v", err)
	}
	log.Printf("loaded %d profiles, %d players with stats (season %d)",
		len(store.Profiles), len(store.StatsByPID), *statsSeason)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/current_week", store.handleCurrentWeek)
	r.Get("/health", store.handleHealth)
	r.Get("/tier_list/pool/{position}", store.handlePool)

	log.Printf("go-api listening on %s", *addr)
	if err := http.ListenAndServe(*addr, r); err != nil {
		log.Fatal(err)
	}
}

func loadStore(ragDir string, statsSeason int) (*DataStore, error) {
	profilesPath := filepath.Join(ragDir, "player_profiles_2026.csv")
	statsPath := filepath.Join(ragDir, "weekly_player_stats_"+itoa(statsSeason)+".csv")

	profTbl, err := loadCSV(profilesPath)
	if err != nil {
		return nil, err
	}
	statsTbl, err := loadCSV(statsPath)
	if err != nil {
		return nil, err
	}

	return &DataStore{
		Profiles:    loadProfiles(profTbl),
		StatsByPID:  aggregatePlayerStats(statsTbl),
		CurrentWeek: 1,
		LoadedAt:    time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func itoa(n int) string {
	// small helper to avoid importing strconv in main for one call site
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
