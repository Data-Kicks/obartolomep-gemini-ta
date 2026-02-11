"""
Database queries and utilities.
"""
table_queries = {
    'teams': """CREATE TABLE dim_teams (
            team_id VARCHAR NOT NULL PRIMARY KEY,
            name VARCHAR NOT NULL,
            league VARCHAR NOT NULL,
            stadium VARCHAR NOT NULL,
            city VARCHAR NOT NULL
            );""",

    'players': """CREATE TABLE dim_players (
                player_id VARCHAR NOT NULL PRIMARY KEY,
                name VARCHAR NOT NULL,
                team_id VARCHAR NOT NULL,
                position VARCHAR NOT NULL,
                date_of_birth DATE NOT NULL,
                nationality VARCHAR NOT NULL,
                market_value INTEGER,
                contract_until DATE
            );""",

    'matches': """CREATE TABLE dim_matches (
                match_id VARCHAR NOT NULL PRIMARY KEY,
                competition VARCHAR NOT NULL,
                season VARCHAR NOT NULL,
                match_date DATE NOT NULL,
                home_team_id VARCHAR NOT NULL,
                away_team_id VARCHAR NOT NULL,
                home_score INTEGER NOT NULL,
                away_score INTEGER NOT NULL,
                venue VARCHAR NOT NULL,
                attendance INTEGER NOT NULL,
                referee VARCHAR NOT NULL
            );""",

    'match_events': """CREATE TABLE fact_match_events (
                    event_id VARCHAR NOT NULL PRIMARY KEY,
                    match_id VARCHAR NOT NULL,
                    minute INTEGER NOT NULL,
                    second INTEGER NOT NULL,
                    event_type VARCHAR NOT NULL,
                    player_id VARCHAR NOT NULL,
                    team_id VARCHAR NOT NULL,
                    x_start FLOAT NOT NULL,
                    y_start FLOAT NOT NULL,
                    x_end FLOAT,
                    y_end FLOAT,
                    outcome VARCHAR NOT NULL,
                    body_part VARCHAR,
                    pass_type VARCHAR DEFAULT '',
                    recipient_id VARCHAR DEFAULT ''
                );""",

    'player_match_stats': """CREATE TABLE fact_player_match_stats (
                            player_id VARCHAR NOT NULL,
                            match_id VARCHAR NOT NULL,
                            minutes_played INTEGER NOT NULL,
                            goals INTEGER NOT NULL,
                            assists INTEGER NOT NULL,
                            shots INTEGER NOT NULL,
                            shots_on_target INTEGER NOT NULL,
                            passes_attempted INTEGER NOT NULL,
                            passes_completed INTEGER NOT NULL,
                            key_passes INTEGER NOT NULL,
                            tackles INTEGER NOT NULL,
                            interceptions INTEGER NOT NULL,
                            duels_won INTEGER NOT NULL,
                            duels_lost INTEGER NOT NULL,
                            fouls_committed INTEGER NOT NULL,
                            yellow_cards INTEGER NOT NULL,
                            red_cards INTEGER NOT NULL,
                            xg FLOAT,
                            xa FLOAT,
                            PRIMARY KEY (player_id, match_id)
                        );"""
}

def get_table_type(name: str) -> str:
    dimensions = ["teams", "players", "matches"]
    facts = ["player_match_stats", "match_events"]

    type = ""
    if name in dimensions:
        type = "dim"
    elif name in facts:
        type = "fact"

    return type  