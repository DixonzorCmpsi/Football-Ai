## What We Have

we've successfully created a robust set of data files within the `rag_data` folder, designed to provide comprehensive, up-to-date context for the 2025 NFL season:

1.  **`player_profiles.csv`**: Contains static information about each player (ID, name, team, position, age, height, weight, draft info, injury status, headshot URL). This acts as the master "phonebook" for players.
2.  **`schedule_2025.csv`**: Holds the entire 2025 schedule, including game IDs, teams involved, week number, and scores/results for games that have already been played.
3.  **`weekly_player_stats_2025.csv`**: This is the dynamic file with detailed week-by-week offensive statistics (passing, rushing, receiving, snaps, targets, derived stats like YPC, etc.) specifically for fantasy-relevant positions (QB, RB, WR, TE). It crucially includes the `opponent_team` for each game.
4.  **`weekly_defense_stats_2025.csv`**: the dynamic file detailing each team's defensive performance per week, including key stats like sacks, interceptions, fumbles forced, points allowed, passing yards allowed, and rushing yards allowed.
5.  **`weekly_offense_stats_2025.csv`**: the dynamic file detailing each team's overall offensive production per week, including total passing yards, rushing yards, touchdowns, turnovers, first downs, and points scored.

**Scripts:** we also have three Python scripts (`01_create_static_files.py`, `02_update_weekly_stats.py`, `03_create_defense_and_offense_files.py`) that generate and maintain these files.

