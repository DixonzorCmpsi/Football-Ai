# Dev Commands: Run ETL, Start Server, Run Frontend

These commands assume you're at the repo root (`/home/football-ai/Football-Ai`).

1) Run the ETL (preferred inside backend container):

- Using docker-compose (recommended):
  docker-compose up --build db backend
  # Then inside the backend container: 
  docker exec -it <backend_container_name> python3 rag_data/05_etl_to_postgres.py

- Or use helper (attempts container):
  python3 backend/rag_data/run_etl.py --container

- Or run locally (ensure dependencies):
  pip install -r backend/requirements.txt
  python3 backend/rag_data/05_etl_to_postgres.py

2) Start the backend server (development):
  cd backend
  uvicorn applications.server:app --reload --host 0.0.0.0 --port 8000

Environment variables you can set:
- DB_CONNECTION_STRING="postgresql://user:pass@host:port/db"
- RUN_ETL_ON_STARTUP=false  # disable synchronous startup ETL

3) Start the frontend (development):
  cd Dashboard/predictor-frontend
  npm install
  npm run dev

4) Useful checks
  - Health endpoint: curl http://localhost:8000/health
  - Check ETL presence: curl http://localhost:8000/health | jq .etl_script_exists

Notes
- The server will run ETL on startup if `RUN_ETL_ON_STARTUP` is true (default). It will run synchronously only when the DB appears empty (quick probe) and otherwise run ETL in the background, and scheduled ETL runs at 06:00 are preserved.
- Team logos (used in the matchup banner) are looked up at `/assets/logos/{TEAM_NAME}.svg` — add SVGs to `Dashboard/predictor-frontend/public/assets/logos/` if desired.
