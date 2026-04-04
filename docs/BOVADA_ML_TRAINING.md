# Bovada Betting Data for ML Training

## Overview
The system now captures and stores comprehensive betting data from Bovada for future machine learning model training. This data enables advanced prop prediction models that can learn from market expectations and actual outcomes.

## Database Schema

### Table: `bovada_player_props`
Stores individual player prop bets with outcomes.

**Columns:**
- `player_name` (TEXT): Player's full name
- `position` (TEXT): QB, RB, WR, TE
- `prop_type` (TEXT): Type of prop (e.g., "Passing Yards", "Receiving Yards", "Anytime TD")
- `line` (FLOAT): The betting line (e.g., 250.5 yards)
- `odds` (TEXT): American odds (e.g., "-110", "+150")
- `side` (TEXT): "over", "under", or "yes" (for TD props)
- `implied_prob` (FLOAT): Implied probability from odds (0.0 to 1.0)
- `week` (INT): NFL week number
- `game_id` (TEXT): Unique game identifier
- `season` (INT): NFL season year
- `scraped_at` (TIMESTAMP): When the odds were captured (allows tracking line movements)
- `actual_result` (TEXT): "hit" or "miss" (populated post-game)
- `processed_at` (TIMESTAMP): When the record was added to database

### Table: `bovada_game_lines`
Stores game-level betting lines.

**Columns:**
- `game_id` (TEXT): Unique game identifier
- `week` (INT): NFL week number
- `season` (INT): NFL season year
- `home_team` (TEXT): Home team abbreviation
- `away_team` (TEXT): Away team abbreviation
- `total_over` (FLOAT): Over/under total
- `total_over_odds` / `total_under_odds` (TEXT): O/U odds
- `home_spread` / `away_spread` (FLOAT): Point spreads
- `home_ml` / `away_ml` (TEXT): Moneyline odds
- Various `*_prob` fields with implied probabilities
- `processed_at` (TIMESTAMP): When record was added

## Data Pipeline

### 1. Scraping (Steps 10-12)
- **10_bovada_crawler.py**: Identifies game URLs from Bovada
- **11_bovada_scraper.py**: Downloads raw HTML data
- **12_process_bovada.py**: Parses and structures the data with AI player matching

### 2. Storage (smart_append mode)
- Props are deduplicated by `[player_name, week, game_id, prop_type, line, side, scraped_at]`
- This preserves historical line movements (e.g., if line moves from 250.5 to 245.5)
- Game lines are deduplicated by `[game_id, week]`

### 3. Post-Game Results (Step 14)
- **14_update_bovada_results.py**: Runs after games complete
- Compares betting lines against actual player stats
- Populates `actual_result` field with "hit" or "miss"
- Can be run manually: `python3 backend/rag_data/14_update_bovada_results.py [week]`

## Using This Data for ML Training

### Use Case 1: Prop Prediction Model
Train a model to predict whether props will hit or miss.

**Features:**
- Player's recent performance (rolling averages)
- Betting line value
- Implied probability
- Vegas game total (over/under)
- Opponent defensive rankings
- Injury status
- Snap count trends

**Target:**
- `actual_result` (binary: hit/miss)

**Sample Query:**
```sql
SELECT 
    p.player_name,
    p.position,
    p.prop_type,
    p.line,
    p.implied_prob,
    p.actual_result,
    s.fantasy_points_ppr,
    s.passing_yards,
    s.rushing_yards,
    s.receiving_yards,
    gl.total_over as game_total,
    gl.home_spread
FROM bovada_player_props p
LEFT JOIN weekly_player_stats_2025 s 
    ON p.player_name = s.player_name AND p.week = s.week
LEFT JOIN bovada_game_lines gl
    ON p.game_id = gl.game_id
WHERE p.actual_result IS NOT NULL
```

### Use Case 2: Line Movement Analysis
Track how betting lines change leading up to games.

**Analysis:**
- Group by `[player_name, week, game_id, prop_type]`
- Order by `scraped_at`
- Calculate line movement velocity
- Identify "sharp money" indicators

### Use Case 3: Market Efficiency Study
Compare model predictions against Vegas lines.

**Metrics:**
- Model win rate vs. Vegas implied probability
- ROI simulation
- Identify prop types where models outperform bookmakers

### Use Case 4: Correlation Analysis
Study prop correlations within games.

**Examples:**
- QB passing yards vs. WR receiving yards
- Game total vs. QB passing TDs
- Spread vs. RB rushing yards

## Running the Pipeline

### Full ETL (includes betting data):
```bash
docker-compose exec backend python3 rag_data/05_etl_to_postgres.py
```

### Update results after games:
```bash
# Update all weeks
docker-compose exec backend python3 rag_data/14_update_bovada_results.py

# Update specific week
docker-compose exec backend python3 rag_data/14_update_bovada_results.py 10
```

### Using the helper script:
```bash
# Run individual step
python3 backend/rag_data/run_step.py --step 14_update_bovada_results.py
```

## Data Validation

Check data quality:
```sql
-- Props with results
SELECT 
    COUNT(*) as total_props,
    SUM(CASE WHEN actual_result = 'hit' THEN 1 ELSE 0 END) as hits,
    SUM(CASE WHEN actual_result = 'miss' THEN 1 ELSE 0 END) as misses,
    SUM(CASE WHEN actual_result IS NULL THEN 1 ELSE 0 END) as pending
FROM bovada_player_props;

-- Hit rate by prop type
SELECT 
    prop_type,
    COUNT(*) as total,
    AVG(CASE WHEN actual_result = 'hit' THEN 1.0 ELSE 0.0 END) as hit_rate
FROM bovada_player_props
WHERE actual_result IS NOT NULL
GROUP BY prop_type
ORDER BY total DESC;

-- Historical line movements
SELECT 
    player_name,
    prop_type,
    line,
    odds,
    scraped_at
FROM bovada_player_props
WHERE player_name = 'Patrick Mahomes' 
    AND week = 10 
    AND prop_type = 'Passing Yards'
ORDER BY scraped_at;
```

## Future Enhancements

1. **Automated Result Updates**: Schedule `14_update_bovada_results.py` to run on Tuesdays after MNF
2. **Line Movement Alerts**: Flag significant line movements (>5% implied prob change)
3. **Prop Recommendation Engine**: Use historical hit rates to identify +EV opportunities
4. **Multi-book Aggregation**: Expand beyond Bovada to DraftKings, FanDuel, etc.
5. **Live Odds Tracking**: Capture in-game odds movements

## Important Notes

- **Legal Disclaimer**: This data is for research and model training purposes only
- **Data Freshness**: Scraping frequency affects line movement tracking accuracy
- **Deduplication**: `scraped_at` is included in deduplication to preserve line movements
- **Prop Matching**: AI-powered player matching ensures consistency with existing player profiles
- **Result Accuracy**: `actual_result` calculation assumes stat corrections are complete (run 48+ hours post-game)

## 6. Sample Training Script

A complete working example is provided in `backend/rag_data/sample_prop_prediction_training.py`:

```bash
# Run the sample training script
cd backend
python rag_data/sample_prop_prediction_training.py
```

This script demonstrates:
- Loading prop data with results from the database
- Engineering features from player stats and game lines
- Training a Random Forest classifier
- Evaluating performance with ROC AUC and classification metrics
- Simulating betting performance with confidence thresholds
- Feature importance analysis

**Expected Output:**
```
📥 Loading data from database...
   ✅ Loaded 1247 props with results
   ✅ Loaded 8934 player stat records
   ✅ Loaded 267 game line records

🔧 Engineering features...
   📊 Calculating rolling features...

📋 Preparing training data...
   Features: 10
   Samples: 1247
   Hit Rate: 52.34%

🤖 Training Random Forest model...

📊 Model Performance:
              precision    recall  f1-score   support
           0       0.56      0.62      0.59       119
           1       0.58      0.52      0.55       131

ROC AUC Score: 0.6234

📈 Top 10 Most Important Features:
   implied_prob                   0.2451
   line                           0.1834
   fantasy_points_ppr             0.1273
   ...

💰 Simulated Betting Performance:
Bets placed (>55% confidence): 87
Win rate: 56.32%
✅ Model beats the vig! (+3.9% edge)
```

## 7. Next Steps

### Recommended Development Path

1. **Start Simple**
   - Begin with a single prop type (e.g., QB passing yards)
   - Use basic features (line, implied_prob, recent_avg)
   - Establish baseline performance

2. **Iterate & Expand**
   - Add opponent-specific features (defense rankings)
   - Include game context (home/away, weather, primetime)
   - Incorporate injury data and snap counts

3. **Advanced Techniques**
   - Ensemble models combining multiple prop types
   - Meta-models that learn when to trust prop predictions
   - Real-time updating as new props are released

4. **Production Considerations**
   - Version control for models and feature pipelines
   - A/B testing framework for comparing strategies
   - Risk management (bankroll, bet sizing)
   - Performance monitoring and alerting
