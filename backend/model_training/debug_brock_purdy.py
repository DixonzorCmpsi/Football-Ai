import polars as pl
import numpy as np
import math

# --- 1. MOCK DATA SETUP ---
print("--- 🛠️  Setting up Mock Data for Brock Purdy ---")

# Mock Model that just returns a fixed deviation (e.g., +1.5) to test the math
class MockXGBoost:
    def predict(self, input_data):
        # We can inspect input_data here to see what average was passed!
        # Assuming input_data is a numpy array, let's grab the first feature
        # (Assuming player_season_avg_points is the first feature for this test)
        input_val = input_data[0][0] 
        print(f"   🤖 [MODEL INTERNAL] Received Input Feature: {input_val}")
        
        # Let's say the model predicts he performs 1.0 point BETTER than the baseline
        return np.array([1.0]) 

# Mock Global State
model_data = {
    "models": {
        "QB": {
            "model": MockXGBoost(),
            # Simplified feature list for debugging
            "features": ["player_season_avg_points", "opponent_rank"] 
        }
    },
    "injury_map": {"BP123": "Active"}
}

# 1. PROFILE: Brock Purdy (SF)
model_data["df_profile"] = pl.DataFrame({
    "player_id": ["BP123"],
    "player_name": ["Brock Purdy"],
    "position": ["QB"],
    "team_abbr": ["SF"]
})

# 2. FEATURES: Week 10 (Target Week)
# Note: Season Average is 18.5 here
model_data["df_features"] = pl.DataFrame({
    "player_id": ["BP123"],
    "week": [10],
    "team_abbr": ["SF"],
    "player_season_avg_points": [18.5], 
    "opponent_rank": [15],
    "opponent_team": ["SEA"],
    "offense_pct": [1.0],
    "offense_snaps": [65]
})

# 3. PLAYER STATS: History (Weeks 7, 8, 9)
# We make him HOT recently. Avg should be 24.0
model_data["df_player_stats"] = pl.DataFrame({
    "player_id": ["BP123", "BP123", "BP123"],
    "week": [7, 8, 9],
    "y_fantasy_points_ppr": [22.0, 26.0, 24.0], # Avg = 24.0
    # Dummy fillers for calculation function
    "passing_yards": [250, 300, 280],
    "passing_tds": [2, 3, 2],
    "rushing_yards": [10, 20, 15],
    "rushing_tds": [0, 0, 0],
    "receiving_yards": [0, 0, 0],
    "receiving_tds": [0, 0, 0],
    "receptions": [0, 0, 0],
    "interceptions": [0, 0, 0],
    "fumbles_lost": [0, 0, 0]
})

# --- 2. HELPER FUNCTIONS ---
def calculate_fantasy_points(row):
    # Simplified for debug, just returns the pre-set points
    return float(row.get('y_fantasy_points_ppr', 0.0))

def get_average_points_fallback(pid, week):
    return 15.0 # Dummy fallback

# --- 3. THE LOGIC TO TEST (run_base_prediction) ---
def run_base_prediction_debug(pid, pos, week):
    print(f"\n🚀 Running Prediction for {pid} (Week {week})...")
    
    # Filter features
    player_features = model_data["df_features"].filter(
        (pl.col("player_id") == str(pid)) & (pl.col("week") == int(week))
    )
    features_dict = player_features.row(0, named=True)
    
    m_info = model_data["models"][pos]
    
    # --- 1. CALCULATE 3-GAME AVG ---
    last_3_games = model_data['df_player_stats'].filter(
        (pl.col('player_id') == str(pid)) & 
        (pl.col('week') < int(week)) & 
        (pl.col('week') >= int(week) - 3)
    )
    
    avg_last_3 = 0.0
    if not last_3_games.is_empty():
        rolling_pts = [calculate_fantasy_points(row) for row in last_3_games.to_dicts()]
        avg_last_3 = sum(rolling_pts) / len(rolling_pts)
        print(f"   📊 [CALC] Last 3 Games Points: {rolling_pts}")
        print(f"   📊 [CALC] 3-Game Rolling Avg: {avg_last_3}")
    else:
        print("   ⚠️ [CALC] No recent history found.")

    # --- 2. MODEL PREDICTION ---
    feats_input = {}
    for k in m_info["features"]:
        if k == 'player_season_avg_points':
            print(f"   🔄 [INPUT] Overriding Season Avg ({features_dict.get(k)}) with Rolling Avg ({avg_last_3})")
            feats_input[k] = [float(avg_last_3)]
        else:
            feats_input[k] = [float(features_dict.get(k) or 0.0)]
    
    pred_dev = m_info["model"].predict(pl.DataFrame(feats_input).to_numpy())[0]
    print(f"   🤖 [MODEL] Predicted Deviation: {pred_dev}")

    # --- 3. LOGARITHMIC BOOST ---
    if pred_dev > 0:
        amplified_dev = 5.0 * math.log1p(pred_dev)
        print(f"   📈 [BOOST] Logarithmic Boost Applied: {pred_dev} -> {amplified_dev:.2f}")
    else:
        amplified_dev = pred_dev
        print(f"   📉 [BOOST] No Boost (Linear): {pred_dev}")

    # --- 4. BASELINE SELECTION (CRITICAL CHECK) ---
    baseline = avg_last_3 
    season_avg = float(features_dict.get('player_season_avg_points', 0.0))
    
    print(f"   📏 [BASELINE] Using 3-Week Avg ({baseline}) for math.")
    print(f"   👁️ [UI] Frontend will show Season Avg ({season_avg}).")

    # --- 5. FINAL CALC ---
    final_score = max(0.0, baseline + amplified_dev)
    
    print(f"   🏁 [FINAL] {baseline} (Base) + {amplified_dev:.2f} (Dev) = {final_score:.2f}")
    
    return final_score, False, features_dict, avg_last_3

# --- 4. EXECUTE ---
if __name__ == "__main__":
    final, boosted, feats, rolling = run_base_prediction_debug("BP123", "QB", 10)
    
    print("\n--- RESULTS SUMMARY ---")
    print(f"Name: Brock Purdy")
    print(f"Season Average (Frontend): {feats['player_season_avg_points']}")
    print(f"3-Game Average (Model Input): {rolling}")
    print(f"Final Prediction: {final}")
    
    if final == feats['player_season_avg_points'] + 1.0: # 1.0 was the dummy dev
        print("\n❌ FAILURE: Logic used Season Average as baseline.")
    elif final > rolling:
        print("\n✅ SUCCESS: Logic used 3-Game Average + Boost.")
    else:
        print("\n⚠️ CHECK: Math looks unusual.")