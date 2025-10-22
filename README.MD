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