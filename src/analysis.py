"""
Script for creating aggregated views and queries for analysing.
Also includes some functionalities for filtering database tables and custom queries.
"""

import polars as pl
from pathlib import Path
from typing import Optional
from datetime import datetime
import duckdb
from logging import Logger, getLogger
from db_utils import agg_players_query, teams_summary_query, teams_top3_query


# Define main paths
project_path: Path = Path(__file__).resolve().parents[1]
db_path = project_path / "data" / "processed" / "scouting.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)
analysis_path = project_path / "outputs" / "analysis"
analysis_path.mkdir(parents=True, exist_ok=True)


# Set up logging
logger: Logger = getLogger(name="Analysis")


# Main analysis functions
def create_player_aggregated_view(query_type: str = "polars") -> None:
    """
    Create an aggregated view of player statistics across all matches, including per 90 metrics and accuracy rates.
    This function reads player match stats from the database, performs aggregations and calculations using Polars,
    and saves the results back to the database as a new table.

    Args:
        query_type: "polars" to use Polars for aggregation, "sql" to use a SQL query. Default is "polars".
    """
    conn = duckdb.connect(str(db_path))

    if query_type == "polars":
        logger.info("Creating players aggregated view using Polars")

        teams = conn.execute(
            """SELECT team_id, name AS team_name FROM dim_teams"""
        ).pl()
        players = (
            conn.execute("""SELECT * FROM dim_players""")
            .pl()
            .join(teams, on="team_id", how="left")
        )

        player_match_stats = conn.execute(
            """SELECT * FROM fact_player_match_stats"""
        ).pl()

        player_aggregated_stats = player_match_stats.group_by("player_id").agg(
            [
                pl.sum("goals").alias("goals"),
                pl.sum("assists").alias("assists"),
                pl.sum("minutes_played").alias("minutes_played"),
                pl.sum("shots").alias("shots"),
                pl.sum("shots_on_target").alias("shots_on_target"),
                pl.sum("passes_attempted").alias("passes_attempted"),
                pl.sum("passes_completed").alias("passes_completed"),
                pl.sum("key_passes").alias("key_passes"),
                pl.sum("tackles").alias("tackles"),
                pl.sum("interceptions").alias("interceptions"),
                pl.sum("duels_won").alias("duels_won"),
                pl.sum("duels_lost").alias("duels_lost"),
                pl.sum("fouls_committed").alias("fouls_committed"),
                pl.sum("yellow_cards").alias("yellow_cards"),
                pl.sum("red_cards").alias("red_cards"),
                pl.sum("xg").alias("xg"),
                pl.sum("xa").alias("xa"),
                pl.sum("goal_contribution").alias("goal_contribution"),
                pl.sum("g_xg").alias("g_xg"),
            ]
        )

        player_aggregated_stats = player_aggregated_stats.with_columns(
            [
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("goals") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("goals_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("assists") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("assists_90")
                ),
                (
                    pl.when(pl.col("shots") > 0)
                    .then(pl.col("shots_on_target") / pl.col("shots"))
                    .otherwise(None)
                    .round(2)
                    .alias("shot_accuracy")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("passes_attempted") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("passes_attempted_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("passes_completed") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("passes_completed_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("key_passes") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("key_passes_90")
                ),
                (
                    pl.when(pl.col("passes_attempted") > 0)
                    .then(pl.col("passes_completed") / pl.col("passes_attempted"))
                    .otherwise(None)
                    .round(2)
                    .alias("pass_accuracy")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("tackles") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("tackles_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("interceptions") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("interceptions_90")
                ),
                (
                    pl.when((pl.col("duels_won") + pl.col("duels_lost")) > 0)
                    .then(
                        pl.col("duels_won")
                        / (pl.col("duels_won") + pl.col("duels_lost"))
                    )
                    .otherwise(None)
                    .round(2)
                    .alias("duel_win_rate")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("fouls_committed") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("fouls_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("yellow_cards") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("yellow_cards_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("red_cards") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("red_cards_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("xg") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("xg_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("xa") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("xa_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("goal_contribution") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("goal_contribution_90")
                ),
                (
                    pl.when(pl.col("minutes_played") > 0)
                    .then(pl.col("g_xg") / pl.col("minutes_played") * 90)
                    .otherwise(None)
                    .round(2)
                    .alias("g_xg_90")
                ),
            ]
        )

        agg_player_view = players.join(
            player_aggregated_stats, on="player_id", how="left"
        )

        try:
            conn.register("temp_player_agg", agg_player_view.to_arrow())
            query = """
                        CREATE OR REPLACE TABLE agg_player_stats_polars AS
                        SELECT * FROM temp_player_agg
                        ORDER BY player_id;
                    """
            conn.execute(query)
            conn.execute("DROP VIEW IF EXISTS temp_player_agg")

            logger.info("Aggregated view created successfully!")
        except Exception as e:
            logger.error(f"Error saving aggregates table to database: {e}")
            raise
        finally:
            conn.close()
    elif query_type == "sql":
        logger.info("Creating players aggregated view using SQL...")
        try:
            conn.execute(agg_players_query)
            logger.info("Aggregated view created successfully!")
        except Exception as e:
            logger.error(f"Error saving aggregates table to database: {e}")
            raise
        finally:
            conn.close()
    else:
        raise ValueError(
            f"Invalid query type: {query_type}. Supported types are 'polars' and 'sql'."
        )


def filter_players(
    query_type: str = "polars",
    position: Optional[str] = None,
    age_min: int = 0,
    age_max: Optional[int] = None,
    goals_min: int = 0,
    goals_max: Optional[int] = None,
    assists_min: int = 0,
    assists_max: Optional[int] = None,
    minutes_played_min: int = 0,
    minutes_played_max: Optional[int] = None,
    shots_min: int = 0,
    shots_max: Optional[int] = None,
    shots_on_target_min: int = 0,
    shots_on_target_max: Optional[int] = None,
    passes_attempted_min: int = 0,
    passes_attempted_max: Optional[int] = None,
    passes_completed_min: int = 0,
    passes_completed_max: Optional[int] = None,
    key_passes_min: int = 0,
    key_passes_max: Optional[int] = None,
    tackles_min: int = 0,
    tackles_max: Optional[int] = None,
    interceptions_min: int = 0,
    interceptions_max: Optional[int] = None,
    duels_won_min: int = 0,
    duels_won_max: Optional[int] = None,
    duels_lost_min: int = 0,
    duels_lost_max: Optional[int] = None,
    fouls_committed_min: int = 0,
    fouls_committed_max: Optional[int] = None,
    yellow_cards_min: int = 0,
    yellow_cards_max: Optional[int] = None,
    red_cards_min: int = 0,
    red_cards_max: Optional[int] = None,
    xg_min: float = 0.0,
    xg_max: Optional[float] = None,
    xa_min: float = 0.0,
    xa_max: Optional[float] = None,
    goal_contribution_min: int = 0,
    goal_contribution_max: Optional[int] = None,
    g_xg_min: float = 0.0,
    g_xg_max: Optional[float] = None,
    goals_90_min: float = 0.0,
    goals_90_max: Optional[float] = None,
    assists_90_min: float = 0.0,
    assists_90_max: Optional[float] = None,
    shot_accuracy_min: float = 0.0,
    shot_accuracy_max: Optional[float] = None,
    passes_attempted_90_min: float = 0.0,
    passes_attempted_90_max: Optional[float] = None,
    passes_completed_90_min: float = 0.0,
    passes_completed_90_max: Optional[float] = None,
    pass_accuracy_min: float = 0.0,
    pass_accuracy_max: Optional[float] = None,
    key_passes_90_min: float = 0.0,
    key_passes_90_max: Optional[float] = None,
    tackles_90_min: float = 0.0,
    tackles_90_max: Optional[float] = None,
    interceptions_90_min: float = 0.0,
    interceptions_90_max: Optional[float] = None,
    duel_win_rate_min: float = 0.0,
    duel_win_rate_max: Optional[float] = None,
    fouls_90_min: float = 0.0,
    fouls_90_max: Optional[float] = None,
    yellow_cards_90_min: float = 0.0,
    yellow_cards_90_max: Optional[float] = None,
    red_cards_90_min: float = 0.0,
    red_cards_90_max: Optional[float] = None,
    xg_90_min: float = 0.0,
    xg_90_max: Optional[float] = None,
    xa_90_min: float = 0.0,
    xa_90_max: Optional[float] = None,
    goal_contribution_90_min: int = 0,
    goal_contribution_90_max: Optional[int] = None,
    g_xg_90_min: float = 0.0,
    g_xg_90_max: Optional[float] = None,
) -> pl.DataFrame:
    """
    Function to filter players based on specific criteria, such as minimum minutes played, goals scored, or position.

    Args:
        query_type: "polars" to use Polars for filtering, "sql" to use a SQL query. Default is "polars".
        *params: Various filtering parameters for player statistics. Each parameter has a minimum and maximum value filter.

    Returns:
        A Polars DataFrame containing the filtered player statistics.
    """
    conn = duckdb.connect(str(db_path))

    param_map = {
        "age": (age_min, age_max),
        "goals": (goals_min, goals_max),
        "assists": (assists_min, assists_max),
        "minutes_played": (minutes_played_min, minutes_played_max),
        "shots": (shots_min, shots_max),
        "shots_on_target": (shots_on_target_min, shots_on_target_max),
        "passes_attempted": (passes_attempted_min, passes_attempted_max),
        "passes_completed": (passes_completed_min, passes_completed_max),
        "key_passes": (key_passes_min, key_passes_max),
        "tackles": (tackles_min, tackles_max),
        "interceptions": (interceptions_min, interceptions_max),
        "duels_won": (duels_won_min, duels_won_max),
        "duels_lost": (duels_lost_min, duels_lost_max),
        "fouls_committed": (fouls_committed_min, fouls_committed_max),
        "yellow_cards": (yellow_cards_min, yellow_cards_max),
        "red_cards": (red_cards_min, red_cards_max),
        "xg": (xg_min, xg_max),
        "xa": (xa_min, xa_max),
        "goal_contribution": (goal_contribution_min, goal_contribution_max),
        "g_xg": (g_xg_min, g_xg_max),
        "goals_90": (goals_90_min, goals_90_max),
        "assists_90": (assists_90_min, assists_90_max),
        "shot_accuracy": (shot_accuracy_min, shot_accuracy_max),
        "passes_attempted_90": (passes_attempted_90_min, passes_attempted_90_max),
        "passes_completed_90": (passes_completed_90_min, passes_completed_90_max),
        "key_passes_90": (key_passes_90_min, key_passes_90_max),
        "pass_accuracy": (pass_accuracy_min, pass_accuracy_max),
        "tackles_90": (tackles_90_min, tackles_90_max),
        "interceptions_90": (interceptions_90_min, interceptions_90_max),
        "duel_win_rate": (duel_win_rate_min, duel_win_rate_max),
        "fouls_90": (fouls_90_min, fouls_90_max),
        "yellow_cards_90": (yellow_cards_90_min, yellow_cards_90_max),
        "red_cards_90": (red_cards_90_min, red_cards_90_max),
        "xg_90": (xg_90_min, xg_90_max),
        "xa_90": (xa_90_min, xa_90_max),
        "goal_contribution_90": (goal_contribution_90_min, goal_contribution_90_max),
        "g_xg_90": (g_xg_90_min, g_xg_90_max),
    }

    if query_type == "polars":
        try:
            player_match_stats = conn.execute(
                """SELECT * FROM agg_player_stats_polars"""
            ).pl()

            expr = None
            for col, (min, max) in param_map.items():
                if min is not None:
                    cond = pl.col(col) >= min
                    expr = cond if expr is None else (expr & cond)
                if max is not None:
                    cond = pl.col(col) <= max
                    expr = cond if expr is None else (expr & cond)

            if position is not None:
                cond = pl.col("position") == position
                expr = cond if expr is None else (expr & cond)

            result = (
                player_match_stats
                if expr is None
                else player_match_stats.filter(expr).sort("player_id")
            )
            return result
        except Exception as e:
            logger.error(f"Error filtering player stats: {e}")
            raise
        finally:
            conn.close()
    elif query_type == "sql":
        try:
            conds = []
            for col, (min, max) in param_map.items():
                if min is not None:
                    conds.append(f"{col} >= {min}")
                if max is not None:
                    conds.append(f"{col} <= {max}")

            if position is not None:
                conds.append(f"position = {repr(position)}")

            where_clause = " AND ".join(conds) if conds else "1=1"
            query = f"SELECT * FROM agg_player_stats_sql WHERE {where_clause} ORDER BY player_id;"

            result = conn.execute(query).pl()
            return result
        except Exception as e:
            logger.error(f"Error filtering player stats: {e}")
            raise
        finally:
            conn.close()
    else:
        raise ValueError(
            f"Invalid query type: {query_type}. Supported types are 'polars' and 'sql'."
        )


def get_team_summary(
    query_type: str = "polars", team_id: Optional[str] = None
) -> pl.DataFrame:
    """
    Function to get a summary of the team main statistics.

    Args:
        query_type: "polars" to use Polars for querying, "sql" to use a SQL query. Default is "polars".
        team_id: Optional team ID to filter the summary for a specific team. If None, returns summary for all teams.

    Returns:
        A Polars DataFrame containing the team summary statistics.
    """
    conn = duckdb.connect(str(db_path))

    if query_type == "polars":
        try:
            player_match_stats = conn.execute(
                """SELECT * FROM agg_player_stats_polars"""
            ).pl()
            teams = conn.execute(
                """SELECT team_id, league, name AS team_name FROM dim_teams"""
            ).pl()

            team_stats = (
                player_match_stats.join(
                    teams.select(["team_id", "team_name", "league"]),
                    on="team_id",
                    how="inner",
                )
                .group_by(["team_id", "league", "team_name"])
                .agg(
                    [
                        pl.col("age").mean().round(2).alias("avg_age"),
                        pl.col("goals").sum().alias("total_goals"),
                        pl.col("assists").sum().alias("total_assists"),
                        pl.col("minutes_played").sum().alias("total_minutes_played"),
                        pl.col("shots").sum().alias("total_shots"),
                        pl.col("shots_on_target").sum().alias("total_shots_on_target"),
                        pl.col("shot_accuracy")
                        .mean()
                        .round(2)
                        .alias("avg_shot_accuracy"),
                        pl.col("passes_attempted")
                        .sum()
                        .alias("total_passes_attempted"),
                        pl.col("passes_completed")
                        .sum()
                        .alias("total_passes_completed"),
                        pl.col("pass_accuracy")
                        .mean()
                        .round(2)
                        .alias("avg_pass_accuracy"),
                        pl.col("key_passes").sum().alias("total_key_passes"),
                        pl.col("tackles").sum().alias("total_tackles"),
                        pl.col("interceptions").sum().alias("total_interceptions"),
                        pl.col("duels_won").sum().alias("total_duels_won"),
                        pl.col("duels_lost").sum().alias("total_duels_lost"),
                        pl.col("duel_win_rate")
                        .mean()
                        .round(2)
                        .alias("avg_duel_win_rate"),
                        pl.col("fouls_committed").sum().alias("total_fouls_committed"),
                        pl.col("yellow_cards").sum().alias("total_yellow_cards"),
                        pl.col("red_cards").sum().alias("total_red_cards"),
                        pl.col("xg").sum().round(2).alias("total_xg"),
                        pl.col("xa").sum().round(2).alias("total_xa"),
                        pl.col("g_xg").sum().round(2).alias("total_g_xg"),
                    ]
                )
                .sort("team_name")
            )

            if team_id is not None:
                team_stats = team_stats.filter(pl.col("team_id") == team_id).drop(
                    "team_id"
                )

            return team_stats
        except Exception as e:
            logger.error(f"Error getting team summary: {e}")
            raise
        finally:
            conn.close()

    elif query_type == "sql":
        try:
            if team_id is not None:
                query = f""" 
                    SELECT 
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
                    WHERE a.team_id = '{team_id}'
                    GROUP BY t.team_id, t.name, t.league
                    ORDER BY t.name;
                """
                result = conn.execute(query).pl()
                return result
            else:
                result = conn.execute(teams_summary_query).pl()
                return result
        except Exception as e:
            logger.error(f"Error filtering player stats: {e}")
            raise
        finally:
            conn.close()
    else:
        raise ValueError(
            f"Invalid query type: {query_type}. Supported types are 'polars' and 'sql'."
        )


def get_team_top3(
    query_type: str = "polars", team_id: Optional[str] = None
) -> pl.DataFrame:
    """
    Function to get the top 3 players in each team based on goal contribution (Goals + Assists).

    Args:
        query_type: "polars" to use Polars for querying, "sql" to use a SQL query. Default is "polars".
        team_id: Optional team ID to filter the top 3 players for a specific team. If None, returns top 3 players for all teams.

    Returns:
        A Polars DataFrame containing the top 3 players for each team based on goal contribution, along with their statistics.
    """
    conn = duckdb.connect(str(db_path))

    if query_type == "polars":
        try:
            player_match_stats = conn.execute(
                """SELECT * FROM agg_player_stats_polars"""
            ).pl()
            teams = conn.execute(
                """SELECT team_id, league, name AS team_name FROM dim_teams"""
            ).pl()

            result = (
                player_match_stats.join(
                    teams.select(["team_id", "league"]), on="team_id", how="left"
                )
                .with_columns(
                    pl.col("goal_contribution")
                    .rank("ordinal", descending=True)
                    .over("team_id")
                    .alias("ranking")
                )
                .filter(pl.col("ranking") <= 3)
            )

            if team_id is not None:
                result = result.filter(pl.col("team_id") == team_id)

            result = result.select(
                [
                    "league",
                    "team_name",
                    pl.col("name").alias("player_name"),
                    "position",
                    "age",
                    "minutes_played",
                    "goals",
                    "assists",
                    "goal_contribution",
                ]
            ).sort(["team_name", "goal_contribution"], descending=[False, True])

            return result
        except Exception as e:
            logger.error(f"Error getting team summary: {e}")
            raise
        finally:
            conn.close()

    elif query_type == "sql":
        try:
            if team_id is not None:
                query = f"""WITH agg_player_stats_with_league AS (
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
                                    WHERE ranking <= 3 AND team_id = '{team_id}'
                                    ORDER BY team_name ASC, goal_contribution DESC;"""
                result = conn.execute(query).pl()
                return result
            else:
                result = conn.execute(teams_top3_query).pl()
                return result
        except Exception as e:
            logger.error(f"Error filtering player stats: {e}")
            raise
        finally:
            conn.close()
    else:
        raise ValueError(
            f"Invalid query type: {query_type}. Supported types are 'polars' and 'sql'."
        )


def create_players_report(query_type: str = "polars"):
    """
    Function to create custom player reports based on specific criteria, such as effective scorers, defensive midfielders, and creative attackers.
    Each report is saved as a CSV file in the outputs/analysis directory.

    Args:
        query_type: "polars" to use Polars for filtering, "sql" to use a SQL query.
    """
    logger.info("Creating player reports...")

    # Effective scorers report - players with at least 5 goals and a goals-xg difference of at least 2
    timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
    effective_scorers = filter_players(query_type, goals_min=5, g_xg_min=2).select(
        ["name", "team_name", "age", "position", "minutes_played", "goals", "g_xg"]
    )
    effective_scorers.write_csv(analysis_path / f"effective_scorers_{timestamp}.csv")
    print(effective_scorers)

    # Defensive midfielders report - players in midfield position with pass accuracy of at least 75%, at least 3 interceptions per 90 and a duel win rate of at least 50%
    timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
    defensive_midfielders = filter_players(
        query_type,
        position="Midfielder",
        pass_accuracy_min=0.75,
        interceptions_90_min=3,
        duel_win_rate_min=0.5,
    ).select(
        [
            "name",
            "team_name",
            "age",
            "minutes_played",
            "pass_accuracy",
            "interceptions_90",
            "duel_win_rate",
        ]
    )
    defensive_midfielders.write_csv(
        analysis_path / f"defensive_midfielders_{timestamp}.csv"
    )
    print(defensive_midfielders)

    # Creative attackers report - players with at least 2 key passes per 90 and a goal contribution per 90 of at least 0.5
    timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
    creative_attackers = filter_players(
        query_type, key_passes_90_min=2, goal_contribution_90_min=0.5
    ).select(
        [
            "name",
            "team_name",
            "age",
            "minutes_played",
            "key_passes_90",
            "goal_contribution_90",
        ]
    )
    creative_attackers.write_csv(analysis_path / f"creative_attackers_{timestamp}.csv")
    print(creative_attackers)

    logger.info("Player reports created!")


def create_teams_report(query_type: str = "polars", team_id: Optional[str] = None):
    """
    Function to create team reports, including a team summary report and a team top 3 report.
    Each report is saved as a CSV file in the outputs/analysis directory.

    Args:
        query_type: "polars" to use Polars for querying, "sql" to use a SQL query.
        team_id: Optional team ID to filter the reports for a specific team. If None, creates reports for all teams.
    """
    logger.info(
        f"Creating team reports for team_id={team_id if team_id else 'all teams'}..."
    )

    # Team summary report - Includes all metrics from the player aggregated view, aggregated at team level.
    timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
    team_report = get_team_summary(query_type, team_id)
    team_report.write_csv(
        analysis_path / f"team_summary_{team_id if team_id else 'all'}_{timestamp}.csv"
    )
    print(get_team_summary(query_type, team_id))

    logger.info(
        f"Team summary report created for team_id={team_id if team_id else 'all teams'}."
    )

    logger.info(
        f"Creating team top 3 report for team_id={team_id if team_id else 'all teams'}..."
    )

    # Team top 3 report - Top 3 players in each team based on goal contribution, with their main statistics.
    timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
    team_players_report = get_team_top3(query_type, team_id)
    team_players_report.write_csv(
        analysis_path / f"team_top3_{team_id if team_id else 'all'}_{timestamp}.csv"
    )
    print(get_team_top3(query_type, team_id))

    logger.info(
        f"Team top 3 report created for team_id={team_id if team_id else 'all teams'}!"
    )


def main():
    query_type = "sql"
    create_player_aggregated_view(query_type)
    create_players_report(query_type)
    create_teams_report(query_type)
    create_teams_report(query_type, team_id="T001")  # Arsenal


if __name__ == "__main__":
    main()
