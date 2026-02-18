"""
Database queries and utilities.
"""

table_queries = {
    "teams": """CREATE TABLE dim_teams (
            team_id VARCHAR NOT NULL PRIMARY KEY,
            name VARCHAR NOT NULL,
            league VARCHAR NOT NULL,
            stadium VARCHAR NOT NULL,
            city VARCHAR NOT NULL
            );""",
    "players": """CREATE TABLE dim_players (
                player_id VARCHAR NOT NULL PRIMARY KEY,
                name VARCHAR NOT NULL,
                team_id VARCHAR NOT NULL,
                position VARCHAR NOT NULL,
                date_of_birth DATE NOT NULL,
                nationality VARCHAR NOT NULL,
                market_value INTEGER,
                contract_until DATE,
                age INTEGER NOT NULL
            );""",
    "matches": """CREATE TABLE dim_matches (
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
    "match_events": """CREATE TABLE fact_match_events (
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
    "player_match_stats": """CREATE TABLE fact_player_match_stats (
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
                            goal_contribution INTEGER NOT NULL, 
                            g_xg FLOAT,
                            PRIMARY KEY (player_id, match_id)
                        );""",
}

agg_players_query = """CREATE OR REPLACE TABLE agg_player_stats_sql AS
                        WITH 
                            players_with_team AS (
                                SELECT
                                    p.*,
                                    t.name  AS team_name
                                FROM dim_players p
                                LEFT JOIN dim_teams t USING (team_id)
                                ),
                            players_agg AS (
                                SELECT
                                    player_id,
                                    SUM(goals) AS goals,
                                    SUM(assists) AS assists,
                                    SUM(minutes_played) AS minutes_played,
                                    SUM(shots) AS shots,
                                    SUM(shots_on_target) AS shots_on_target,
                                    SUM(passes_attempted) AS passes_attempted,
                                    SUM(passes_completed) AS passes_completed,
                                    SUM(key_passes) AS key_passes,
                                    SUM(tackles) AS tackles,
                                    SUM(interceptions) AS interceptions,
                                    SUM(duels_won) AS duels_won,
                                    SUM(duels_lost) AS duels_lost,
                                    SUM(fouls_committed) AS fouls_committed,
                                    SUM(yellow_cards) AS yellow_cards,
                                    SUM(red_cards) AS red_cards,
                                    SUM(xg) AS xg,
                                    SUM(xa) AS xa,
                                    SUM(goal_contribution) AS goal_contribution,
                                    SUM(g_xg) AS g_xg,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(goals) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS goals_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(assists) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS assists_90,
                                    ROUND(
                                        CASE WHEN SUM(shots) > 0
                                        THEN SUM(shots_on_target) / SUM(shots)
                                        ELSE NULL END
                                    , 2) AS shot_accuracy,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(passes_attempted) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS passes_attempted_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(passes_completed) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS passes_completed_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(key_passes) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS key_passes_90,
                                    ROUND(
                                        CASE WHEN SUM(passes_attempted) > 0
                                        THEN SUM(passes_completed) / SUM(passes_attempted)
                                        ELSE NULL END
                                    , 2) AS pass_accuracy,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(tackles) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS tackles_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(interceptions) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS interceptions_90,
                                    ROUND(
                                        CASE WHEN (SUM(duels_won) + SUM(duels_lost)) > 0
                                        THEN SUM(duels_won) / (SUM(duels_won) + SUM(duels_lost))
                                        ELSE NULL END
                                    , 2) AS duel_win_rate,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(fouls_committed) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS fouls_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(yellow_cards) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS yellow_cards_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(red_cards) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS red_cards_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(xg) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS xg_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(xa) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS xa_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(goal_contribution) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS goal_contribution_90,
                                    ROUND(
                                        CASE WHEN SUM(minutes_played) > 0
                                        THEN SUM(g_xg) / SUM(minutes_played) * 90
                                        ELSE NULL END
                                    , 2) AS g_xg_90
                                FROM fact_player_match_stats
                                GROUP BY player_id
                            )
                            SELECT 
                                pt.*,
                                pa.*
                            FROM players_with_team pt
                            LEFT JOIN players_agg pa USING (player_id)
                            ORDER BY pt.player_id;"""
teams_summary_query = """SELECT 
                            t.league,
                            t.name AS team_name,
                            ROUND(AVG(a.age),2) AS avg_age,
                            SUM(a.goals) AS total_goals, 
                            SUM(a.assists) AS total_assists, 
                            SUM(a.minutes_played) AS total_minutes_played, 
                            SUM(a.shots) AS total_shots, 
                            SUM(a.shots_on_target) AS total_shots_on_target, 
                            ROUND(AVG(a.shot_accuracy),2) AS avg_shot_accuracy,
                            SUM(a.passes_attempted) AS total_passes_attempted, 
                            SUM(a.passes_completed) AS total_passes_completed, 
                            ROUND(AVG(a.pass_accuracy),2) AS avg_pass_accuracy,
                            SUM(a.key_passes) AS total_key_passes, 
                            SUM(a.tackles) AS total_tackles, 
                            SUM(a.interceptions) AS total_interceptions, 
                            SUM(a.duels_won) AS total_duels_won, 
                            SUM(a.duels_lost) AS total_duels_lost, 
                            ROUND(AVG(a.duel_win_rate),2) AS avg_duel_win_rate,
                            SUM(a.fouls_committed) AS total_fouls_committed, 
                            SUM(a.yellow_cards) AS total_yellow_cards, 
                            SUM(a.red_cards) AS total_red_cards, 
                            ROUND(SUM(a.xg),2) AS total_xg, 
                            ROUND(SUM(a.xa),2) AS total_xa, 
                            ROUND(SUM(a.g_xg),2) AS total_g_xg 
                        FROM agg_player_stats_sql a
                        INNER JOIN dim_teams t 
                            ON a.team_id = t.team_id
                        GROUP BY t.team_id, t.name, t.league
                        ORDER BY t.name;"""

teams_top3_query = """WITH agg_player_stats_with_league AS (
                        SELECT 
                            a.player_id,
                            a.name,
                            a.team_id,
                            a.team_name,
                            a.position,
                            a.age,
                            a.minutes_played,
                            a.goals,
                            a.assists,
                            a.goal_contribution,
                            t.league
                        FROM agg_player_stats_sql a
                        LEFT JOIN dim_teams t 
                            ON a.team_id = t.team_id
                        ),
                        ranked_team_players AS (
                            SELECT 
                                *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY team_id 
                                    ORDER BY goal_contribution DESC
                                ) as ranking
                            FROM agg_player_stats_with_league
                        )
                        SELECT 
                            league,
                            team_name,
                            name as player_name,
                            position,
                            age,
                            minutes_played,
                            goals,
                            assists,
                            goal_contribution
                        FROM ranked_team_players
                        WHERE ranking <= 3
                        ORDER BY team_name ASC, goal_contribution DESC;"""


def get_table_type(name: str) -> str:
    dimensions = ["teams", "players", "matches"]
    facts = ["player_match_stats", "match_events"]

    type = ""
    if name in dimensions:
        type = "dim"
    elif name in facts:
        type = "fact"

    return type
