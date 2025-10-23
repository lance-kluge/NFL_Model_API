import pandas as pd
import numpy as np
import os

def add_rolling_features(df):
    X = df
    X['home'] = X['home'].astype(str)
    X['away'] = X['away'].astype(str)

    X['points_allowed_home'] = X['score_away']
    X['points_allowed_away'] = X['score_home']
    X["home_win"] = (np.where((X["score_home"] == 0) & (X["score_away"] == 0), -1, np.where(X["score_home"] > X["score_away"], 1, 0)))

    X.loc[:,'week'] = pd.to_numeric(X['week'], errors='coerce')
    X = X.dropna(subset=['week'])
    X.loc[:,'game_id'] = X.index

    home_cols = [col for col in X.columns if col.endswith('_home')]
    away_cols = [col for col in X.columns if col.endswith('_away')]

    home_df = X[['season', 'week', 'game_id', 'home', 'home_win'] + home_cols].copy()
    away_df = X[['season', 'week', 'game_id', 'away', 'home_win'] + away_cols].copy()

    home_df = home_df.rename(columns=lambda x: x.replace('_home', '') if x.endswith('_home') else x)
    away_df = away_df.rename(columns=lambda x: x.replace('_away', '') if x.endswith('_away') else x)

    home_df = home_df.rename(columns={'home': 'team'})
    away_df = away_df.rename(columns={'away': 'team'})

    long_X = pd.concat([home_df, away_df], ignore_index=True)
    long_X = long_X.sort_values(by =['season', 'team', 'week'])

    stat_cols = [col for col in long_X.columns if col not in ['season', 'week', 'team', 'game_id', 'home_win']]

    #rolling average, shifted by one so all we know are the stats leading up to that game for the current season averages.
    rolling_avg = (
        long_X
        .groupby(['season', 'team'])[stat_cols]
        .transform(lambda x: x.shift(1).expanding().mean())
    )

    long_X = long_X.sort_values(by=['season', 'team', 'week'])
    rolling_X = pd.concat([long_X[['season', 'week', 'team', 'game_id', 'home_win']], rolling_avg], axis=1)

    #merge the two df back together and label to be able to tell which team was the home team
    home_stats = rolling_X.merge(X[['game_id', 'home']], left_on=['game_id', 'team'], right_on=['game_id', 'home'])
    away_stats = rolling_X.merge(X[['game_id', 'away']], left_on=['game_id', 'team'], right_on=['game_id', 'away'])

    home_stats = home_stats.add_suffix('_home')
    away_stats = away_stats.add_suffix('_away')

    #final merge of home and away stats
    final_X = pd.merge(home_stats, away_stats, left_on='game_id_home', right_on='game_id_away')
    final_X = final_X.rename(columns={'game_id_home': 'game_id'}).drop(columns=['game_id_away'])

    #drop cols that are week 1 as we do not have any existing information about their season yet.
    final_X = final_X[final_X['week_home'] > 1].copy()

    final_X['week'] = final_X['week_home']
    final_X['season'] = final_X['season_home']
    final_X['home_win'] = final_X['home_win_home']
    final_X = final_X.drop(['season_home', 'week_home', 'home_home', 'season_away', 'week_away', 'away_away', 'home_win_home', 'home_win_away'], axis =1)

    home_cols = [c for c in final_X.columns if c.endswith("_home")]
    away_cols = [c for c in final_X.columns if c.endswith("_away")]

    #drop the team cols as we don't want to find the diff of them, we only want numeric cols
    home_cols.remove('team_home')
    away_cols.remove('team_away')

    #sort to make sure both are in same order
    home_cols = sorted(home_cols)
    away_cols = sorted(away_cols)

    #we are going to look at the difference in the two teams per feature in use for upcoming models, calculate it by taking the home team stat - the away team stat
    for h, a in zip(home_cols, away_cols):
        base = h.replace("_home", "")
        diff_col = f"{base}_diff"
        final_X[diff_col] = final_X[h] - final_X[a]
    #keep only the cols that are the difference cols and identifiers like week, season and ofc our y in home_win 
    diff_cols = [c for c in final_X.columns if c.endswith("_diff")]
    X2 = final_X[diff_cols + ['week', 'season', 'team_home', 'team_away', 'home_win']]
    
    print(X2.columns)
    return X2