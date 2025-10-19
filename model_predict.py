import pandas as pd
import xgboost as xgb
import json
from datetime import datetime
from rolling_stats import add_rolling_features

DATA_PATH = "nfl_team_stats_2025.csv"
MODEL_PATH = "models/trained_model.json"
FEATURES_PATH = "models/feature_columns.json"
OUTPUT_PATH = "predictions_latest.csv"


def load_data(path=DATA_PATH):
    df = pd.read_csv(path)
    return df


def load_model(model_path=MODEL_PATH, features_path=FEATURES_PATH):
    model = xgb.XGBClassifier()
    model.load_model(model_path)

    with open(features_path, "r") as f:
        feature_cols = json.load(f)
    return model, feature_cols


def prepare_features(df):
    print("Applying rolling statistics...")
    df = add_rolling_features(df)
    return df


def predict(df, model, feature_cols):
    # Make sure expected columns exist
    for col in feature_cols:
        if col not in df.columns:
            df[col] = 0
            print('found col not in df.columns')

    X = df[feature_cols].fillna(0)
    preds = model.predict_proba(X)[:, 1]  # assuming binary win probability
    df["win_probability"] = preds
    df["prediction_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def save_predictions(df, path=OUTPUT_PATH):
    df.to_csv(path, index=False)
    print(f"Predictions saved → {path}")


def main():
    df = load_data()
    model, feature_cols = load_model()
    df = prepare_features(df)
    df = predict(df, model, feature_cols)
    save_predictions(df)
    return df


if __name__ == "__main__":
    main()
