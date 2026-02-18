"""
Data transformation script for cleaning and normalizing the data.
It also stores the final data in a DuckDB database.
Handles all quality issues mentioned in requirements.
"""

import polars as pl
from datetime import datetime
import duckdb
from typing import Dict, Optional
from logging import Logger, basicConfig, FileHandler, Formatter, getLogger, INFO
from pathlib import Path
from dateutil.parser import parse
from ingestion import load_from_landing
from db_utils import get_table_type, table_queries


# Define main paths
project_path: Path = Path(__file__).resolve().parents[1]
transformation_path = project_path / "outputs" / "logs" / "transformation"
transformation_path.mkdir(parents=True, exist_ok=True)
db_path = project_path / "data" / "processed" / "scouting.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)


# Set up logging
basicConfig(level=INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger: Logger = getLogger(name="Transformation")

timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
handler = FileHandler(
    transformation_path / f"transformation_log_{timestamp}.log", encoding="utf-8"
)
handler.setLevel(INFO)
handler.setFormatter(
    Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)


# Main transformation logic
def map_player_position(position: Optional[str]) -> str:
    """
    Map raw position to standardized categories.

    Args:
        position: Raw position string.

    Returns:
        Standardized position or 'Unknown'.
    """
    if position is None or position == "":
        return "Unknown"

    position = str(position).lower()
    if position in ["gk", "goalkeeper"]:
        return "Goalkeeper"
    elif position in ["defender", "cb", "centre-back"]:
        return "Defender"
    elif position in ["midfielder", "cm", "central midfielder"]:
        return "Midfielder"
    elif position in ["forward", "striker", "st"]:
        return "Forward"

    logger.warning(f"Unrecognized position: {position}")
    return "Unknown"


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """
    Parse date from mixed formats using 'dateutil.parse'.

    Args:
        date_str: Date string or None.

    Returns:
        Normalized date string or None.
    """
    if date_str is None or date_str == "":
        return None

    date_str = str(date_str).strip()

    try:
        date = parse(date_str)
        return date.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"Could not parse date: {date_str}")
        return None


def calculate_age(date_of_birth: str):
    """
    Calculate age from date of birth.

    Args:
        date_of_birth: Date of birth string in 'YYYY-MM-DD' format.

    Returns: Age in years or None if invalid.
    """
    try:
        dob = datetime.strptime(parse_date(date_of_birth), "%Y-%m-%d")
        today = datetime.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

        return age
    except Exception as e:
        logger.warning(
            f"error calculating age from date_of_birth: {date_of_birth}, error: {e}"
        )
        return None


def transform_teams(teams_df: pl.LazyFrame) -> pl.DataFrame:
    """
    Clean teams data (only remove duplicates).

    Args:
        teams_df: Raw teams LazyFrame.

    Returns:
        Cleaned teams DataFrame.
    """
    logger.info("Transforming teams data...")

    if teams_df.count().collect().shape[1] == 0:
        logger.warning("Teams dataset is empty, skipping transformation.")
        return pl.DataFrame()

    try:
        # Remove duplicates
        teams_df = teams_df.unique(subset=["team_id"], keep="last").collect()
        logger.info(f"Teams transformed: {len(teams_df)} rows")
        return teams_df
    except Exception as e:
        logger.exception("Error transforming teams data: %s", e)
        return pl.DataFrame()


def transform_players(players_df: pl.LazyFrame, team_id_list: list) -> pl.DataFrame:
    """
    Clean players data.

    Args:
        players_df: Raw players LazyFrame.
        team_id_list: List of team_ids for searching orphaned records.

    Returns:
        Cleaned players DataFrame.
    """
    logger.info("Transforming players data...")

    if players_df.count().collect().shape[1] == 0:
        logger.warning("Players dataset is empty, skipping transformation.")
        return pl.DataFrame()

    try:
        # Normalize positions
        if "position" in players_df.columns:
            players_df = players_df.with_columns(
                pl.col("position")
                .map_elements(map_player_position, return_dtype=pl.Utf8)
                .alias("position")
            )

        logger.info("Position normalization completed.")

        # Normalize date formats
        date_cols = ["date_of_birth", "contract_until"]
        for col in date_cols:
            if col in players_df.columns:
                players_df = players_df.with_columns(
                    pl.col(col)
                    .map_elements(parse_date, return_dtype=pl.Utf8)
                    .alias(f"{col}")
                )

        # Filter out orphaned records
        players_df = players_df.filter(pl.col("team_id").is_in(team_id_list))

        # Remove duplicates
        players_df = players_df.unique(subset=["player_id"], keep="last").collect()

        # Create new columns
        players_df = players_df.with_columns(
            pl.col("date_of_birth")
            .map_elements(calculate_age, return_dtype=pl.Int16)
            .alias("age")
        )

        logger.info(
            f"Players transformed: {len(players_df)} rows, {len(players_df.columns)} columns"
        )
        return players_df
    except Exception as e:
        logger.exception("Error transforming players data: %s", e)
        return pl.DataFrame()


def transform_matches(matches_df: pl.LazyFrame) -> pl.DataFrame:
    """
    Clean matches data.

    Args:
        matches_df: Raw matches LazyFrame.

    Returns:
        Cleaned matches DataFrame.
    """
    logger.info("Transforming matches data...")

    if matches_df.count().collect().shape[1] == 0:
        logger.warning("Matches dataset is empty, skipping transformation.")
        return pl.DataFrame()

    try:
        # Normalize date formats
        if "match_date" in matches_df.columns:
            matches_df = matches_df.with_columns(
                pl.col("match_date")
                .map_elements(parse_date, return_dtype=pl.Utf8)
                .alias("match_date")
            )

        # Remove duplicates
        matches_df = matches_df.unique(subset=["match_id"], keep="last").collect()

        logger.info(f"Matches transformed: {len(matches_df)} rows")
        return matches_df
    except Exception as e:
        logger.exception("Error transforming matches data: %s", e)
        return pl.DataFrame()


def transform_player_match_stats(
    stats_df: pl.LazyFrame, player_id_list: list, match_id_list: list
) -> pl.DataFrame:
    """
    Clean player match stats.

    Args:
        stats_df: Raw player match stats LazyFrame.
        player_id_list: List of player_ids for searching orphaned records.
        match_id_list: List of match_ids for searching orphaned records.

    Returns:
        Cleaned player stats DataFrame.
    """
    logger.info("Transforming player match stats...")

    if stats_df.count().collect().shape[1] == 0:
        logger.warning("Player match stats dataset is empty, skipping transformation.")
        return pl.DataFrame()

    try:
        # Filter out orphaned records
        stats_df = stats_df.filter(
            pl.col("player_id").is_in(player_id_list)
            & pl.col("match_id").is_in(match_id_list)
        )

        # Fix negative xG values
        stats_df = stats_df.with_columns(
            pl.when(pl.col("xg") < 0)
            .then(pl.lit(None))
            .otherwise(pl.col("xg"))
            .alias("xg")
        )

        # Fix xG > shots
        stats_df = stats_df.with_columns(
            pl.when(pl.col("xg") > pl.col("shots"))
            .then(pl.lit(None))
            .otherwise(pl.col("xg"))
            .alias("xg")
        )

        # Remove duplicates
        stats_df = stats_df.unique(
            subset=["player_id", "match_id"], keep="last"
        ).collect()

        # Create new columns
        stats_df = stats_df.with_columns(
            (pl.col("goals") + pl.col("assists")).alias("goal_contributions"),
            (pl.col("goals") - pl.col("xg")).alias("g_xg"),
        )

        logger.info(f"Player match stats transformed: {len(stats_df)} rows")
        return stats_df
    except Exception as e:
        logger.exception("Error transforming player match stats: %s", e)
        return pl.DataFrame()


def transform_match_events(
    events_df: pl.LazyFrame,
    match_id_list: list,
    team_id_list: list,
    player_id_list: list,
) -> pl.DataFrame:
    """
    Clean match events data.

    Args:
        events_df: Raw match events LazyFrame.
        match_id_list: List of match_ids for searching orphaned records.
        team_id_list: List of team_ids for searching orphaned records.
        player_id_list: List of player_ids for searching orphaned records.

    Returns:
        Cleaned events DataFrame.
    """
    logger.info("Transforming match events...")

    if events_df.count().collect().shape[1] == 0:
        logger.warning("Match events dataset is empty, skipping transformation.")
        return pl.DataFrame()

    try:
        # Filter out orphaned records
        events_df = events_df.filter(
            pl.col("match_id").is_in(match_id_list)
            & pl.col("team_id").is_in(team_id_list)
            & pl.col("player_id").is_in(player_id_list)
        )

        # Remove duplicate event_ids
        events_df = events_df.unique(subset=["event_id"], keep="last").collect()

        logger.info(f"Match events transformed: {len(events_df)} rows")
        return events_df
    except Exception as e:
        logger.exception("Error transforming match events data: %s", e)
        return pl.DataFrame()


def transform_all(datasets: Dict[str, pl.LazyFrame]) -> Dict[str, pl.DataFrame]:
    """
    Transform and clean all datasets.

    Args:
        datasets: Dictionary of raw DataFrames.

    Returns:
        Dictionary of transformed DataFrames.
    """
    transformed = {}

    if ("teams" in datasets) & (datasets["teams"].count().collect().shape[1] > 0):
        transformed["teams"] = transform_teams(datasets["teams"])
    if ("players" in datasets) & (
        datasets["players"].count().collect().shape[1] > 0
    ) and ("teams" in transformed) & (len(transformed["teams"]) > 0):
        transformed["players"] = transform_players(
            datasets["players"], transformed["teams"].select("team_id")
        )
    if ("matches" in datasets) and (datasets["matches"].count().collect().shape[1] > 0):
        transformed["matches"] = transform_matches(datasets["matches"])
    if (
        ("player_match_stats" in datasets)
        and (datasets["player_match_stats"].count().collect().shape[1] > 0)
        and ("players" in transformed)
        and (len(transformed["players"]) > 0)
        and ("matches" in transformed)
        and (len(transformed["matches"]) > 0)
    ):
        transformed["player_match_stats"] = transform_player_match_stats(
            datasets["player_match_stats"],
            transformed["players"].select("player_id"),
            transformed["matches"].select("match_id"),
        )
    if (
        ("match_events" in datasets)
        and (datasets["match_events"].count().collect().shape[1] > 0)
        and ("matches" in transformed)
        and (len(transformed["matches"]) > 0)
        and ("teams" in transformed)
        and (len(transformed["teams"]) > 0)
        and ("players" in transformed)
        and (len(transformed["players"]) > 0)
    ):
        transformed["match_events"] = transform_match_events(
            datasets["match_events"],
            transformed["matches"].select("match_id"),
            transformed["teams"].select("team_id"),
            transformed["players"].select("player_id"),
        )

    return transformed


def save_data_to_db(datasets: Dict[str, pl.DataFrame]):
    """
    Save transformed data to a DuckDB database.

    Args:
        datasets: Transformed DataFrames.
    """
    logger.info(f"Saving data to DuckDB: {db_path}")

    conn = duckdb.connect(str(db_path))

    try:
        for name, df in datasets.items():
            if df.is_empty():
                logger.warning(f"Skipping empty dataset: {name}")
                continue

            table_name = get_table_type(name) + "_" + name

            # Register the DataFrame with DuckDB
            conn.register(f"temp_{name}", df.to_arrow())

            # Check if table already exists
            result = conn.execute(f"""
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_name = '{table_name}'
                         """).fetchone()

            exists_flg = result[0] > 0 if result else False

            query = ""
            if exists_flg:
                # Upsert rows
                query = f"""
                        INSERT OR REPLACE INTO {table_name} 
                        SELECT * FROM temp_{name}
                    """
                conn.execute(query)
            else:
                # Create or table
                query = table_queries[name]
                conn.execute(query)

                query = f"""
                        INSERT INTO {table_name} 
                        SELECT * FROM temp_{name}
                    """
                conn.execute(query)

            # Clean up temporary view
            conn.execute(f"DROP VIEW IF EXISTS temp_{name}")

        logger.info(f"All data saved to {db_path}")

    except Exception as e:
        logger.error(f"Error saving data to database: {e}")
        raise
    finally:
        conn.close()


def main() -> None:
    logger.info("Starting raw data transformation...")

    try:
        datasets = load_from_landing()
        transformed = transform_all(datasets)
        save_data_to_db(transformed)
    except Exception as e:
        logger.exception("Transformation step failed with an unexpected error: %s", e)
    finally:
        try:
            logger.removeHandler(handler)
            handler.close()
        except Exception as e:
            logger.exception("Failed to close transformation file handler: %s", e)


if __name__ == "__main__":
    main()
