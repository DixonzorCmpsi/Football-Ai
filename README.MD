Football-Ai/
├── dataPrep/
│   ├── rag-features.py
│   └── ... (other data prep scripts)
├── model/
│   ├── xgboost_fantasy_model.json  # trained model
│   └── feature_names.json          # List of features model expects
├── rag_data/
│   ├── 01_create_static_files.py
│   ├── 02_update_weekly_stats.py
│   ├── 03_create_defense_and_offense_files.py
│   ├── player_profiles.csv
│   ├── schedule_2025.csv
│   ├── weekly_player_stats_2025.csv
│   ├── weekly_defense_stats_2025.csv
│   └── weekly_offense_stats_2025.csv
├── applications/                 
│   └── predict_fantasy_points.py 
└── ... (other project files like requirements.txt)



1.  **Built** all 5 of your live RAG data files (`player_profiles`, `schedule`, `player_stats`, `offense_stats`, `snap_counts`).
2.  **Trained** your 4 specialized, time-series models (QB, RB, WR, TE).
3.  **Created** the `feature_generator_all.py` script (the "brain").
4.  **Created** the `server.py` file (the "application").

The final step is to run the server.

### Run Prediction Server

From your `applications` directory, run this command:

```bash
python .\server.py
```

This will start the FastAPI server. You will see output like this:

```
Starting Multi-Model Time-Series Server...
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process...
INFO:     Started server process...
INFO:     Waiting for application startup.
Loading all RAG data and all 4 positional models...
Loading snap counts and player map...
All RAG data files loaded.
Loading QB model from ...
Loaded QB model and 63 features.
Loading RB model from ...
Loaded RB model and 59 features.
Loading WR model from ...
Loaded WR model and 59 features.
Loading TE model from ...
Loaded TE model and 55 features.
Server Ready. Current NFL week is: 10
INFO:     Application startup complete.
```

### Test Your Server

Once the server is running, open your web browser and go to this URL:

**`http://127.0.0.1:8000/docs`**

You will see the automatic FastAPI documentation. From there, you can use the interactive UI to test your `/predict` and `/compare` endpoints.

