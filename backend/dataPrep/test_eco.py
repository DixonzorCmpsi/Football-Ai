import pandas as pd
df_eco = pd.read_csv("team_ecosystem_dataset.csv")

# Calculate correlation matrix
corr_matrix = df_eco.corr(numeric_only=True)

# See how QB points correlate with WR and RB points
print(corr_matrix['y_fantasy_points_ppr_QB'][['y_fantasy_points_ppr_WR', 'y_fantasy_points_ppr_RB', 'y_fantasy_points_ppr_TE']])

# See how RB rush attempts correlate with QB pass attempts
print(corr_matrix['rush_attempts_RB'].corr(corr_matrix['pass_attempts_QB']))