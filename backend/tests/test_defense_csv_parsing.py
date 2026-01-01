import polars as pl

def test_defense_two_point_column_parsing(tmp_path):
    csv = tmp_path / "weekly_team_stats_defense.csv"
    csv.write_text("team,average_defensive_two_point_attempt\nA,0.166666667\nB,1\n")

    # Without override, polars may try to infer ints for most values and complain on mixed types
    # We assert that providing schema_overrides to Float64 parses cleanly
    df = pl.read_csv(csv, schema_overrides={"average_defensive_two_point_attempt": pl.Float64}, infer_schema_length=1000)

    assert df['average_defensive_two_point_attempt'].dtype == pl.Float64
    assert abs(df[0, 'average_defensive_two_point_attempt'] - 0.166666667) < 1e-9
