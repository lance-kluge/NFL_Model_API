from fastapi import FastAPI
import pandas as pd
import datetime
import os

app = FastAPI(title="NFL Prediction API", version="1.0")

PREDICTIONS_FILE = "predictions_latest.csv"

@app.get("/")
def root():
    return {"message": "NFL Model API is running!"}


@app.get("/predictions")
def get_predictions():
    """Return this week's games with home/away names and win probabilities"""
    if not os.path.exists(PREDICTIONS_FILE):
        return {"error": "Predictions file not found."}

    df = pd.read_csv(PREDICTIONS_FILE)

    # Ensure required columns exist
    required_cols = ["team_home", "team_away", "week", "season", "win_probability"]
    for col in required_cols:
        if col not in df.columns:
            return {"error": f"Missing column in CSV: {col}"}

    # Determine the current week
    current_week = df["week"].max()
    current_season = df["season"].max()

    # Filter for current week only
    this_week = df[(df["week"] == current_week) & (df["season"] == current_season)]

    # Select key columns
    output = this_week[["team_home", "team_away", "win_probability"]].to_dict(orient="records")

    return {
        "season": int(current_season),
        "week": int(current_week),
        "games": output,
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
