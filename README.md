# Football-Ai

An advanced, AI-powered NFL fantasy football prediction engine. This system leverages machine learning (XGBoost) and historical data to predict player performance, identify trends, and provide actionable insights for fantasy football managers.

## Quick Start

### Prerequisites
*   **Docker & Docker Compose**: The entire stack is containerized for easy deployment.
*   **Git**: To clone the repository.

### Startup
1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-repo/football-ai.git
    cd football-ai
    ```

2.  **Launch the application**:
    ```bash
    docker compose up --build -d
    ```
    This command builds and starts three services:
    *   `db`: PostgreSQL database (Port 5432)
    *   `backend`: FastAPI server (Port 8000)
    *   `frontend`: React/Vite dashboard (Port 80)

3.  **Access the Dashboard**:
    Open your browser and navigate to `http://localhost`.

### Data Initialization (ETL)
The system includes a robust ETL (Extract, Transform, Load) pipeline to populate the database.

*   **Automatic**: The backend runs a daily ETL job at 06:00 AM via APScheduler. On startup, if the database appears empty, the server will run ETL synchronously before accepting traffic; otherwise it triggers ETL asynchronously in the background.
*   **Manual**:
    Run ETL inside the backend container:
    ```bash
    docker compose exec backend python3 rag_data/05_etl_to_postgres.py
    ```
*   **Local Python (alternative)**:
    Use the helper script with dependency checks:
    ```bash
    python3 backend/rag_data/run_etl.py --check-deps   # verify required packages
    python3 backend/rag_data/run_etl.py --full          # run full ETL locally
    ```

---

## Architecture

### Tech Stack
*   **Frontend**: React, Vite, Tailwind CSS, Recharts, Lucide React (located in `Dashboard/predictor-frontend/`).
*   **Backend**: Python 3.10, FastAPI, Polars (DataFrames), SQLAlchemy.
*   **Database**: PostgreSQL 15.
*   **ML Engine**: XGBoost, Scikit-Learn.

### Database Schema (PostgreSQL)
The database is designed for high-performance analytics using a star-schema-like approach centered around weekly stats.

*   **`player_profiles_{SEASON}`**: Static player info (ID, Name, Position, Team).
*   **`schedule_{SEASON}`**: Season schedule and game results.
*   **`weekly_player_stats_{SEASON}`**: The core fact table. Contains weekly performance metrics (yards, TDs, fantasy points) for every player.
*   **`weekly_snap_counts_{SEASON}`**: Snap counts and percentages.
*   **`weekly_injuries_{SEASON}`**: Injury reports and status.
*   **`weekly_feature_set_{SEASON}`**: Pre-computed ML features (rolling averages, defensive rankings).
*   **`bovada_game_lines` / `bovada_player_props`**: Betting odds for correlation analysis.

---

## Machine Learning & Retraining

The system uses a "Deviation Model" approach. Instead of predicting raw points directly, it predicts how much a player will *deviate* from their recent 4-week rolling average.

### Model Hierarchy
*   **Base Models**: Position-specific XGBoost regressors (QB, RB, WR, TE).
    *   *Input*: Rolling averages, defensive strength, Vegas implied totals, injury status.
    *   *Output*: Predicted deviation (+/- points).
*   **Meta Model**: A secondary layer that refines predictions based on ensemble outputs (optional).

### Retraining Process
To retrain the models with new data:
1.  Ensure your database is populated with the latest season data.
2.  Run the feature generator:
    ```bash
    # Inside the backend container or with dependencies installed
    python3 backend/applications/feature_generator_timeseries.py
    ```
3.  The script will:
    *   Fetch historical training data.
    *   Train new XGBoost models for each position.
    *   Save artifacts (`.joblib` models and `.json` feature lists) to `backend/applications/`.
4.  Restart the backend to load the new models.

---

## Deployment & Maintenance

### Cloudflare / Production
*   **Hosting**: The frontend is served via Nginx as a static site (see `Dashboard/predictor-frontend/Dockerfile`).
*   **Caching**: If using Cloudflare, ensure caching rules do not cache API endpoints (`/api/*`) to prevent stale data.
*   **SSL**: Cloudflare provides automatic SSL termination.

### Database Transitions
*   **Backup**: Use `pg_dump` to back up your data.
    ```bash
    docker compose exec db pg_dump -U admin football_ai > backup.sql
    ```
*   **Restore**:
    ```bash
    cat backup.sql | docker compose exec -T db psql -U admin football_ai
    ```
*   **Schema Evolution**: The ETL script (`05_etl_to_postgres.py`) has "Smart Schema Evolution". If it detects new columns in the source CSVs, it will automatically alter the database tables to match.

---

## Codebase Structure

```
Football-Ai/
├── backend/
│   ├── applications/
│   │   ├── api/                  # Modular API Logic
│   │   │   ├── routes/           # Endpoints (players, games, general, debug)
│   │   │   ├── services/         # Business logic (data_loader, etl, prediction)
│   │   │   ├── config.py         # Configuration and constants
│   │   │   ├── state.py          # Shared in-memory state (model_data)
│   │   │   └── main.py           # FastAPI app factory with lifespan
│   │   ├── server.py             # Entry point (imports app from api/main.py)
│   │   ├── feature_generator_timeseries.py  # Model training script
│   │   └── *.joblib / *.json     # Trained ML models and feature lists
│   ├── rag_data/                 # ETL Scripts & Data Ingestion
│   │   ├── 05_etl_to_postgres.py # Master ETL Orchestrator
│   │   ├── run_etl.py            # Helper for running ETL locally
│   │   └── *.py                  # Individual ETL steps (stats, injuries, odds, etc.)
│   └── Dockerfile
├── Dashboard/
│   └── predictor-frontend/       # React Application
│       ├── src/
│       │   ├── components/       # UI Components (CompareView, PlayerCard, etc.)
│       │   └── hooks/            # Custom React Hooks
│       ├── Dockerfile            # Nginx-based production build
│       └── package.json
├── docker-compose.yml            # Service Orchestration (db, backend, frontend)
└── README.md                     # This file
```

### Development Commands

**Backend (dev mode with hot reload)**:
```bash
cd backend && uvicorn applications.server:app --reload --host 0.0.0.0 --port 8000
```

**Frontend (dev mode)**:
```bash
cd Dashboard/predictor-frontend && npm install && npm run dev
```

*Note: The root `package.json` references a non-existent `frontend` folder. Use the commands above instead.*

---

*Built with precision for the modern fantasy football enthusiast.*
