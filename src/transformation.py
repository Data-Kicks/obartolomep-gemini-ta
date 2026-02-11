"""
Data transformation script for cleaning and normalizing the data.
It also stores the final data in a DuckDB database.
Handles all quality issues mentioned in requirements.
"""
import polars as pl
from datetime import datetime
from typing import Dict, Optional
from logging import Logger, basicConfig, FileHandler, Formatter, getLogger, INFO
from pathlib import Path
from dateutil.parser import parse
from ingestion import load_from_landing


# Define main paths
project_path: Path = Path(__file__).resolve().parents[1]
transformation_path = project_path / "outputs" / "logs" / "transformation"
transformation_path.mkdir(parents=True, exist_ok=True)
db_path = project_path / "data" / "processed" / "scouting.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)


# Set up logging
basicConfig(
    level=INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger: Logger = getLogger(name="Transformation")

timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
handler = FileHandler(transformation_path / f"transformation_log_{timestamp}.log", encoding="utf-8")
handler.setLevel(INFO)
handler.setFormatter(Formatter(fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
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

def transform_teams(teams_df: pl.LazyFrame) -> pl.DataFrame:
    """
    Clean teams data (only remove duplicates).
    
    Args:
        teams_df: Raw teams LazyFrame.
        
    Returns:
        Cleaned teams DataFrame.
    """
    logger.info("Transforming teams data...")

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
    
    try:
        # Normalize positions
        if "position" in players_df.columns:
            players_df = players_df.with_columns(
                pl.col("position").map_elements(
                    map_player_position, return_dtype=pl.Utf8
                ).alias("position")
            )
            
        logger.info("Position normalization completed.")
        
        # Normalize date formats
        date_cols = ["date_of_birth", "contract_until"]
        for col in date_cols:
            if col in players_df.columns:
                players_df = players_df.with_columns(
                pl.col(col).map_elements(
                    parse_date, return_dtype=pl.Utf8
                ).alias(f"{col}")
                )

        # Filter out orphaned records
        players_df = players_df.filter(
            pl.col("team_id").is_in(team_id_list)
        )
        
        # Remove duplicates
        players_df = players_df.unique(subset=["player_id"], keep="last").collect()
        
        logger.info(f"Players transformed: {len(players_df)} rows, {len(players_df.columns)} columns")
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
    
    try:
        # Normalize date formats
        if "match_date" in matches_df.columns:
            matches_df = matches_df.with_columns(
                pl.col("match_date").map_elements(
                    parse_date, return_dtype=pl.Utf8
                ).alias("match_date")
            )
        
        # Remove duplicates
        matches_df = matches_df.unique(subset=["match_id"], keep="last").collect()
        
        logger.info(f"Matches transformed: {len(matches_df)} rows")
        return matches_df
    except Exception as e:
        logger.exception("Error transforming matches data: %s", e)
        return pl.DataFrame()

def transform_player_match_stats(stats_df: pl.LazyFrame, player_id_list: list, match_id_list: list) -> pl.DataFrame:
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

    # Filter out orphaned records
    stats_df = stats_df.filter(
        pl.col("player_id").is_in(player_id_list) &
        pl.col("match_id").is_in(match_id_list)
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
    stats_df = stats_df.unique(subset=["player_id", "match_id"], keep="last").collect()
    
    logger.info(f"Player match stats transformed: {len(stats_df)} rows")
    return stats_df

def transform_match_events(events_df: pl.LazyFrame, match_id_list: list, team_id_list: list, player_id_list: list) -> pl.DataFrame:
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

    # Filter out orphaned records
    events_df = events_df.filter(
        pl.col("match_id").is_in(match_id_list) &
        pl.col("team_id").is_in(team_id_list) &
        pl.col("player_id").is_in(player_id_list)
    )

    # Remove duplicate event_ids
    events_df = events_df.unique(subset=["event_id"], keep="last").collect()
    
    logger.info(f"Match events transformed: {len(events_df)} rows")
    return events_df

def transform_all(datasets: Dict[str, pl.LazyFrame]) -> Dict[str, pl.DataFrame]:
    """
    Transform and clean all datasets.
    
    Args:
        datasets: Dictionary of raw DataFrames.
        
    Returns:
        Dictionary of transformed DataFrames.
    """
    transformed = {}

    if "teams" in datasets:
        transformed["teams"] = transform_teams(datasets["teams"])
    if "players" in datasets:
        transformed["players"] = transform_players(
            datasets["players"],
            transformed["teams"].select("team_id")
        )
    if "matches" in datasets:
        transformed["matches"] = transform_matches(datasets["matches"])
    if "player_match_stats" in datasets:
        transformed["player_match_stats"] = transform_player_match_stats(
            datasets["player_match_stats"],
            transformed["players"].select("player_id"),
            transformed["matches"].select("match_id")
        )
    if "match_events" in datasets:
        transformed["match_events"] = transform_match_events(
            datasets["match_events"],
            transformed["matches"].select("match_id"),
            transformed["teams"].select("team_id"),
            transformed["players"].select("player_id")
        )
    
    return transformed

def main() -> None:
    logger.info("Starting raw data transformation...")

    datasets = load_from_landing()
    transformed = transform_all(datasets)

    logger.info("Data transformation step finished! See transformation report for mor information.")
    logger.removeHandler(handler)
    handler.close()

if __name__ == "__main__":
    main()