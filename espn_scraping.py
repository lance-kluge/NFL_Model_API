import requests
import pandas as pd
import datetime
import time

def get_boxscore(event_id):
    """Fetch detailed boxscore data for a given game ID"""
    url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={event_id}"
    resp = requests.get(url)
    if resp.status_code != 200:
        return None
    return resp.json()

def get_week_games(season, week):
    print(season, week)
    """Fetch the scoreboard for a given week"""
    url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={season}&seasontype=2&week={week}"
    resp = requests.get(url)

    if resp.status_code != 200:
        return None
    print("got data for week")
    data = resp.json()
    if not data.get("events"):
        return None
    return data["events"]

def parse_team_stats(box):
    team_stats = {}

    try:
        for team in box["boxscore"]["teams"]:
            team_name = team["team"]["displayName"]
            team_stats[team_name] = {}

            # Loop through each stat category for this team
            for cat in team.get("statistics", []):
                name = cat.get("name")
                if not name:
                    continue

                val = cat.get("value")

                # Prefer numeric 'value', unless it's "-", empty, or None
                if val in (None, "-", ""):
                    val = cat.get("displayValue")

                team_stats[team_name][name] = val
    except Exception as e:
        print(f"Error parsing team stats for {team_name}: {e}")
    return team_stats



def build_team_stat_row(event, week, season):
    """Build one row matching your historical dataset"""
    comp = event["competitions"][0]
    date = comp["date"].split("T")[0]
    time_et = comp["date"].split("T")[1].replace("Z", "")
    neutral = comp.get("neutralSite", False)

    home = comp["competitors"][0]
    away = comp["competitors"][1]
    home_team = home["team"]["displayName"]
    away_team = away["team"]["displayName"]
    score_home = int(home.get("score", 0))
    score_away = int(away.get("score", 0))

    # Fetch boxscore for detailed stats
    box = get_boxscore(event["id"])
    time.sleep(0.3)
    if not box or "boxscore" not in box:
        return None

    # Build a lookup for team stats
    team_stats = parse_team_stats(box)

    def get_stat(team, name):
        val = team_stats.get(team, {}).get(name)
        if val is None:
            return 0
        try:
            return int(val)
        except:
            return val
    
    def parse_two_val_entry(display_val):
        if not display_val or display_val in ("-", ""):
            return 0, 0
        if isinstance(display_val, (int, float)):
            return display_val, 0
        if "/" in str(display_val):
            parts = display_val.split("/")
            try:
                first = int(parts[0])
                second = int(parts[1])
                return first, second
            except Exception:
                return 0, 0
        if "-" in str(display_val):
            parts = display_val.split("-")
            try:
                first = int(parts[0])
                second = int(parts[1])
                return first, second
            except Exception:
                return 0, 0
        return 0, 0
    #['score_diff','points_allowed_diff','pen_yards_diff', 'first_downs_diff','first_downs_from_penalty_diff', 'sacks_yards_diff'

    # 'pass_att_diff']

    home_pass_obj = team_stats.get(home_team, {}).get("completionAttempts", {})
    away_pass_obj = team_stats.get(away_team, {}).get("completionAttempts", {})

    home_display = home_pass_obj.get("displayValue") if isinstance(home_pass_obj, dict) else home_pass_obj
    away_display = away_pass_obj.get("displayValue") if isinstance(away_pass_obj, dict) else away_pass_obj

    home_comp, home_att = parse_two_val_entry(home_display)
    away_comp, away_att = parse_two_val_entry(away_display)

    home_sack_obj = team_stats.get(home_team, {}).get("sacksYardsLost", {})
    away_sack_obj = team_stats.get(away_team, {}).get("sacksYardsLost", {})

    home_display = home_sack_obj.get("displayValue") if isinstance(home_sack_obj, dict) else home_sack_obj
    away_display = away_sack_obj.get("displayValue") if isinstance(away_sack_obj, dict) else away_sack_obj

    home_sacks, home_sack_yards = parse_two_val_entry(home_display)
    away_sacks, away_sack_yards = parse_two_val_entry(away_display)

    home_pen_obj = team_stats.get(home_team, {}).get("totalPenaltiesYards", {})
    away_pen_obj = team_stats.get(away_team, {}).get("totalPenaltiesYards", {})

    home_display = home_pen_obj.get("displayValue") if isinstance(home_pen_obj, dict) else home_pen_obj
    away_display = away_pen_obj.get("displayValue") if isinstance(away_pen_obj, dict) else away_pen_obj

    home_pen_num, home_pen_yards = parse_two_val_entry(home_display)
    away_pen_num, away_pen_yards = parse_two_val_entry(away_display)

    row = {
        "season": season,
        "week": week,
        "neutral": neutral,
        "away": away_team,
        "home": home_team,
        "score_away": score_away,
        "score_home": score_home,
       
        "first_downs_away": get_stat(away_team, "firstDowns"),
        "first_downs_home": get_stat(home_team, "firstDowns"),

        "pen_yards_away": home_pen_yards,
        "pen_yards_home": away_pen_yards,

        "first_downs_from_penalty_away": get_stat(away_team, "firstDownsPenalty"),
        "first_downs_from_penalty_home": get_stat(home_team, "firstDownsPenalty"),

        "sacks_yards_away": home_sack_yards,
        "sacks_yards_home": away_sack_yards,

        "pass_att_away": away_att,
        "pass_att_home": home_att,
        
    }

    return row

def get_current_season():
    url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
    resp = requests.get(url)
    data = resp.json()
    return data["season"]["year"], data["week"]["number"]

def scrape_full_current_season():
    season, current_week = get_current_season()
    print(f"Fetching stats for NFL {season} season (up to week {current_week})")

    rows = []
    for week in range(1, current_week + 1):
        print(f"Fetching week {week} ...")
        games = get_week_games(season, week)
        if not games:
            continue

        for event in games:
            row = build_team_stat_row(event, week, season)
            if row:
                rows.append(row)
        time.sleep(1)

    df = pd.DataFrame(rows)
    df.to_csv(f"nfl_team_stats_{season}.csv", index=False)
    print(f"Saved {len(df)} games to nfl_team_stats_{season}.csv")
    return df


if __name__ == "__main__":
    df = scrape_full_current_season()
    print(df.head())
