# Bovada Betting Data - Quick Start Guide

This guide shows you how to collect, store, and use Bovada betting data for ML training.

## Overview

The system collects:
- **Game Lines**: Spreads, totals, moneylines
- **Player Props**: Passing/rushing/receiving yards, TDs, etc.
- **Line Movements**: Historical odds changes over time
- **Results**: Actual outcomes for training validation

## Quick Start (3 Steps)

### 1. Run the ETL Pipeline

The ETL automatically collects Bovada data:

```bash
# Using Docker (recommended)
docker-compose up --build db backend
docker-compose exec backend python rag_data/05_etl_to_postgres.py

# Or locally
cd backend
python rag_data/05_etl_to_postgres.py
```

**What it does:**
- Crawls Bovada for current week's odds (step 10)
- Scrapes detailed HTML data (step 11)
- Processes into structured format with AI player matching (step 12)
- Updates results for past props (step 13)

### 2. Verify Data Collection

Check the database:

```bash
# Connect to database
docker-compose exec db psql -U football_user -d football_db

# Check what was collected
SELECT COUNT(*) FROM bovada_player_props;
SELECT COUNT(*) FROM bovada_game_lines;

# Check data quality
SELECT 
    prop_type,
    COUNT(*) as total,
    COUNT(actual_result) as with_results
FROM bovada_player_props
GROUP BY prop_type;
```

Expected output:
```
      prop_type       | total | with_results 
---------------------+-------+--------------
 Passing Yards       |   128 |           94
 Rushing Yards       |   156 |          112
 Receiving Yards     |   224 |          167
 Passing Touchdowns  |    64 |           47
```

### 3. Train a Model

Run the sample training script:

```bash
cd backend
python rag_data/sample_prop_prediction_training.py
```

This will:
- ✅ Load props with results
- ✅ Engineer features from player stats
- ✅ Train a Random Forest classifier
- ✅ Evaluate performance
- ✅ Simulate betting ROI

## Data Flow Diagram

```
┌─────────────────┐
│  Bovada.lv      │
│  (Live Odds)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 10_bovada_      │   Crawls menu structure
│ crawler.py      ├──► saves to raw_bovada_menu.json
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 11_bovada_      │   Fetches HTML for each game
│ scraper.py      ├──► saves to raw_bovada_html.json
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 12_process_     │   Parses HTML → structured CSV
│ bovada.py       ├──► Matches players with AI
└────────┬────────┘   Adds scraped_at timestamps
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │
│ ┌──────────────┐│
│ │bovada_game_  ││   Game-level lines
│ │lines         ││   (spread, total, ML)
│ └──────────────┘│
│ ┌──────────────┐│
│ │bovada_player_││   Player props
│ │props         ││   (lines, odds, results)
│ └──────────────┘│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 14_update_      │   Post-game: compare props
│ bovada_results  ├──► to actual stats, mark hit/miss
└─────────────────┘
```

## File Locations

```
Football-Ai/
├── backend/
│   ├── rag_data/
│   │   ├── 10_bovada_crawler.py          ← Crawls menu
│   │   ├── 11_bovada_scraper.py          ← Fetches HTML
│   │   ├── 12_process_bovada.py          ← Parses & matches
│   │   ├── 14_update_bovada_results.py   ← Updates results
│   │   ├── sample_prop_prediction_training.py  ← Example model
│   │   └── 05_etl_to_postgres.py         ← Main orchestrator
│   └── applications/
│       └── api/services/betting_insights.py  ← Live insights
└── docs/
    ├── BOVADA_ML_TRAINING.md             ← Full documentation
    └── BOVADA_QUICKSTART.md              ← This file
```

## Common Tasks

### Manually Update Results

After games complete (recommended: Tuesday after MNF):

```bash
docker-compose exec backend python rag_data/14_update_bovada_results.py
```

### Check Line Movements

See how odds changed over time:

```sql
SELECT 
    player_name,
    prop_type,
    line,
    odds,
    implied_prob,
    scraped_at
FROM bovada_player_props
WHERE player_name = 'Patrick Mahomes'
    AND week = 10
    AND prop_type = 'Passing Yards'
ORDER BY scraped_at;
```

### Debug Prop Matching

Check if a prop was matched correctly:

```bash
# In Python
import polars as pl
from sqlalchemy import create_engine

engine = create_engine(DB_CONNECTION_STRING)

# Find props for a specific player
df = pl.read_database(
    "SELECT * FROM bovada_player_props WHERE player_name = 'Josh Allen'",
    engine
)
print(df)
```

## Scheduling (Production)

The backend automatically runs ETL daily at 06:00 (configured in [server.py](../backend/applications/server.py#L65)):

```python
# In applications/server.py
scheduler.add_job(
    run_etl_job,
    trigger="cron",
    hour=6,
    minute=0,
    id="daily_etl"
)
```

To change the schedule:
1. Edit the `hour` and `minute` parameters
2. Restart the backend container

## Troubleshooting

### No props found

**Problem:** `SELECT COUNT(*) FROM bovada_player_props` returns 0

**Solutions:**
1. Check if Bovada scraping succeeded:
   ```bash
   ls -lh backend/rag_data/raw_bovada_*.json
   ```
2. Check scraper logs:
   ```bash
   docker-compose logs backend | grep bovada
   ```
3. Manually run the scraper:
   ```bash
   docker-compose exec backend python rag_data/11_bovada_scraper.py
   ```

### Player matching issues

**Problem:** Props show "Unknown Player" or missing players

**Solutions:**
1. Check player profiles exist:
   ```bash
   grep -i "josh allen" backend/player_profiles_2025.csv
   ```
2. Review matching confidence:
   - The AI matcher logs confidence scores
   - Check logs for low-confidence matches (<0.7)
3. Add manual overrides in `12_process_bovada.py` if needed

### Results not updating

**Problem:** `actual_result` column stays NULL

**Solutions:**
1. Ensure weekly stats are loaded:
   ```sql
   SELECT COUNT(*) FROM weekly_player_stats_2025 WHERE week = 10;
   ```
2. Run result updater manually:
   ```bash
   docker-compose exec backend python rag_data/14_update_bovada_results.py
   ```
3. Check for stat column mismatches (see `PROP_STAT_MAP` in script)

## Next Steps

1. **Read Full Documentation**: [BOVADA_ML_TRAINING.md](BOVADA_ML_TRAINING.md)
2. **Experiment with Features**: Add weather, injuries, opponent defense
3. **Try Different Models**: XGBoost, LightGBM, Neural Networks
4. **Build Production Pipeline**: Version control, monitoring, alerts
5. **Backtest Strategies**: Simulate betting across historical data

## Key Concepts

### Line Movement
Odds change as betting action comes in. Tracking these movements can reveal:
- Sharp money (professional bettors)
- Injury news impact
- Public bias (casual bettors)

### Implied Probability
The probability implied by the odds:
- `-110` odds = 52.4% implied probability
- `+150` odds = 40.0% implied probability

Break-even point at `-110` is 52.4% win rate due to the vig (bookmaker's edge).

### Hit vs. Miss
- **Hit**: Prop bet wins (e.g., player goes OVER yards line)
- **Miss**: Prop bet loses (e.g., player goes UNDER yards line)

Training data needs both outcomes for classification.

### Smart Append
Database updates preserve historical data:
- Same prop scraped at different times = multiple rows
- Deduplication key includes `scraped_at` timestamp
- This enables line movement analysis

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review [BOVADA_ML_TRAINING.md](BOVADA_ML_TRAINING.md)
3. Open an issue on GitHub
4. Review ETL logs: `docker-compose logs backend`

---

**Last Updated**: January 2025
