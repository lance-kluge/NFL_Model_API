from fastapi import FastAPI, BackgroundTasks
import pandas as pd
import datetime
import os
import logging
from espn_scraping import scrape_full_current_season
from model_predict import main as run_model

app = FastAPI(title="NFL Prediction API", version="1.0")

PREDICTIONS_FILE = "predictions_latest.csv"



logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")

@app.get("/")
def root():
    return {"message": "NFL Model API is running!"}


def weekly_job():
    logging.info("Starting scheduled job: scrape and then predict")
    try:
        scrape_full_current_season()
        run_model()
        logging.info("Weekly job complete")
    except Exception as e:
        logging.error(f"Error in scheduled job: {e}")

@app.get("/predictions")
def get_predictions():
    """Return this week's games with home/away names and win probabilities"""
    if not os.path.exists(PREDICTIONS_FILE):
        return {"error": "Predictions file not found."}

    df = pd.read_csv(PREDICTIONS_FILE)

    # Ensure required columns exist
    required_cols = ["team_home", "team_away", "week", "season", "win_probability", "home_win"]
    for col in required_cols:
        if col not in df.columns:
            return {"error": f"Missing column in CSV: {col}"}

    # Determine the current week
    current_week = df["week"].max()
    current_season = df["season"].max()

    # Filter for current week only
    this_week = df[(df["week"] == current_week) & (df["season"] == current_season)]

    # Select key columns
    output = this_week[["team_home", "team_away", "win_probability", "home_win"]].to_dict(orient="records")

    return {
        "season": int(current_season),
        "week": int(current_week),
        "games": output,
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


@app.get("/predictions-week")
def get_predictions_week(week: int):
    """Return predictions for a specific week in the current season"""
    if not os.path.exists(PREDICTIONS_FILE):
        return {"error": "Predictions file not found."}

    df = pd.read_csv(PREDICTIONS_FILE)

    # Ensure required columns exist
    required_cols = ["team_home", "team_away", "week", "season", "win_probability", "home_win"]
    for col in required_cols:
        if col not in df.columns:
            return {"error": f"Missing column in CSV: {col}"}

    # Determine the current season
    current_season = df["season"].max()

    # Filter for given week and current season
    week_data = df[(df["week"] == week) & (df["season"] == current_season)]

    if week_data.empty:
        return {
            "error": f"No data found for week {week} in season {current_season}."
        }

    # Select key columns
    output = week_data[["team_home", "team_away", "win_probability", "home_win"]].to_dict(orient="records")

    return {
        "season": int(current_season),
        "week": int(week),
        "games": output,
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


@app.get("/prediction-record")
def get_prediction_record():
    """Calculate model's prediction accuracy (for completed games only)."""
    if not os.path.exists(PREDICTIONS_FILE):
        return {"error": "Predictions file not found."}

    df = pd.read_csv(PREDICTIONS_FILE)

    required_cols = [
        "team_home", "team_away", "week", "season",
        "win_probability", "score_home", "score_away", 'home_win'
    ]
    for col in required_cols:
        if col not in df.columns:
            return {"error": f"Missing column in CSV: {col}"}

    # --- Filter to completed games ---
    completed = df[df["home_win"] != -1].copy()

    if completed.empty:
        return {"message": "No completed games available to evaluate."}

    # --- Determine predicted winners ---
    completed["predicted_home_win"] = (completed["win_probability"] > 0.51).astype(int)

    # --- Compare predictions to results ---
    completed["correct"] = (completed["predicted_home_win"] == completed["home_win"]).astype(int)

    total_games = len(completed)
    correct_games = completed["correct"].sum()
    accuracy = correct_games / total_games if total_games > 0 else 0

    return {
        "season": int(completed["season"].max()),
        "total_games_evaluated": int(total_games),
        "correct_predictions": int(correct_games),
        "accuracy": round(accuracy, 3),
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


@app.post("/run-now")
def run_now(background_tasks: BackgroundTasks):
    background_tasks.add_task(weekly_job)
    return {
        "status": "Pipeline started in background",
        "started_at": datetime.datetime.now().isoformat()
    }