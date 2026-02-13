"""
Data validation script using Pydantic for data quality checks.
Validates landing data against defined schemas and business rules.
"""
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationError
from typing import Dict, List, Optional, Any
from datetime import datetime
import polars as pl
from pathlib import Path
from logging import Logger, basicConfig, FileHandler, Formatter, getLogger, INFO
from ingestion import load_from_landing


# Define main paths
project_path: Path = Path(__file__).resolve().parents[1]
validation_path = project_path / "outputs" / "logs" / "validation"
validation_path.mkdir(parents=True, exist_ok=True)


# Set up logging
basicConfig(
    level=INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger: Logger = getLogger(name="Validation")

timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
handler = FileHandler(validation_path / f"validation_log_{timestamp}.log", encoding="utf-8")
handler.setLevel(INFO)
handler.setFormatter(Formatter(fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


# Pydantic models for each dataset to enforce schema and business rules
class Team(BaseModel):
    team_id: str
    name: str
    league: str
    stadium: str
    city: str

class Player(BaseModel):
    player_id: str
    name: str
    team_id: str
    position: str
    date_of_birth: Optional[str] = None
    nationality: Optional[str] = None
    market_value: Optional[float] = None
    contract_until: Optional[str] = None
    
    @field_validator('date_of_birth')
    def validate_birth_date_format(value):
        """Validate date_of_birth formats (YYYY-MM-DD or DD/MM/YYYY)."""
        formats = ["%Y-%m-%d", "%d/%m/%Y"]

        for format in formats:
            try:
                datetime.strptime(value, format)
                return value
            except ValueError:
                continue
        
        raise ValueError(f"Invalid date_of_birth format: {value}. Expected YYYY-MM-DD or DD/MM/YYYY")

    @field_validator('contract_until')
    def validate_contract_until_date_format(value):
        """Validate contract_until date formats (YYYY-MM-DD or DD/MM/YYYY)."""
        formats = ["%Y-%m-%d", "%d/%m/%Y"]

        for format in formats:
            try:
                if value is not None:
                    datetime.strptime(value, format)
                return value
            except ValueError:
                continue
        
        raise ValueError(f"Invalid contract_until format: {value}. Expected YYYY-MM-DD or DD/MM/YYYY")
    
    @field_validator('market_value')
    def validate_market_value(value):
        """Market value should be positive."""
        if value is not None and value < 0:
            raise ValueError(f"Market value cannot be negative: {value}")
        return value

class Match(BaseModel):
    match_id: str
    competition: str
    season: str 
    match_date: str
    home_team_id: str
    away_team_id: str
    home_score: int = Field(ge=0)
    away_score: int = Field(ge=0)
    venue: str
    attendance: int = Field(ge=0)
    referee: str
    
    @field_validator('match_date')
    def validate_match_date(value):
        """Validate match_date format."""
        formats = ["%Y-%m-%d", "%d/%m/%Y"]
        for format in formats:
            try:
                datetime.strptime(value, format)
                return value
            except ValueError:
                continue
        raise ValueError(f"Invalid match_date format: {value}. Expected YYYY-MM-DD or DD/MM/YYYY")

class PlayerMatchStats(BaseModel):
    player_id: str
    match_id: str
    minutes_played: int = Field(ge=0)
    goals: int = Field(ge=0)
    assists: int = Field(ge=0)
    shots: int = Field(ge=0)
    shots_on_target: int = Field(ge=0)
    passes_attempted: int = Field(ge=0)
    passes_completed: int = Field(ge=0)
    key_passes: int = Field(ge=0)
    tackles: int = Field(ge=0)
    interceptions: int = Field(ge=0)
    duels_won: int = Field(ge=0)
    duels_lost: int = Field(ge=0)
    fouls_committed: int = Field(ge=0)
    yellow_cards: int = Field(ge=0, le=2)
    red_cards: int = Field(ge=0, le=1)
    xg: float = Field(ge=0)
    xa: float = Field(ge=0)
    
    @model_validator(mode='after')
    def validate_shots_on_target(self):
        """Shots on target cannot be greater than total shots."""
        if self.shots_on_target > self.shots:
            raise ValueError(f"Shots on target ({self.shots_on_target}) cannot be greater than total shots ({self.shots})")
        return self
    
    @model_validator(mode='after')
    def validate_passes_completed(self):
        """Completed passes cannot be greater than attempted passes."""
        if self.passes_completed > self.passes_attempted:
            raise ValueError(f"Passes completed ({self.passes_completed}) cannot be greater than passes attempted ({self.passes_attempted})")
        return self
    
    @model_validator(mode='after')
    def validate_xg_logic(self):
        """Validate xG cannot be greater than total shots."""
        if self.xg > self.shots:
            raise ValueError(f"xG ({self.xg}) cannot be greater than total shots ({self.shots})")
        return self

class MatchEvent(BaseModel):
    event_id: str
    match_id: str
    minute: int = Field(ge=0)
    second: int = Field(ge=0, le=59)
    event_type: str
    player_id: str
    team_id: str
    x_start: float = Field(None, ge=0, le=100)
    y_start: float = Field(None, ge=0, le=100)
    x_end: Optional[float] = Field(None, ge=0, le=100)
    y_end: Optional[float] = Field(None, ge=0, le=100)
    outcome: str
    body_part: Optional[str] = None
    pass_type: Optional[str] = None
    recipient_id: Optional[str] = None

    @model_validator(mode='after')
    def validate_pass_destination(self):
        """Validate xG cannot be greater than total shots."""
        if self.event_type == "pass" and (self.x_end is None or self.y_end is None):
            raise ValueError(f"Pass ({self.event_id}) does not have a destination")
        return self

    @model_validator(mode='after')
    def validate_pass_type(self):
        """Validate xG cannot be greater than total shots."""
        if self.event_type == "pass" and self.pass_type is None:
            raise ValueError(f"Pass ({self.event_id}) does not have a type")
        return self

    @model_validator(mode='after')
    def validate_recipient_id(self):
        """Validate xG cannot be greater than total shots."""
        if self.event_type == "pass" and self.recipient_id is None:
            raise ValueError(f"Pass ({self.event_id}) does not have a recipient")
        return self

    @model_validator(mode='after')
    def validate_body_part(self):
        """Validate xG cannot be greater than total shots."""
        if self.event_type in ["pass", "shot"] and self.body_part is None:
            raise ValueError(f"Pass ({self.event_id}) does not indicate a body part")
        return self


# Main validation logic
def validate_teams(teams_df: pl.DataFrame) -> Dict[str, Any]:
    """
    Validate teams dataset.
    
    Args:
        teams_df: Teams DataFrame.
        
    Returns:
        Dict with teams data validation results.
    """
    logger.info("Validating teams dataset...")

    results = {
        "total_rows": len(teams_df),
        "valid_rows": 0,
        "invalid_rows": 0,
        "errors": [],
        "warnings": []
    }
    
    if teams_df.is_empty():
        results["warnings"].append("Teams dataset is empty")
        return results
    
    # Check for duplicate team ids
    duplicates = teams_df.group_by("team_id").agg(count = pl.len()).filter(pl.col("count") > 1)
    if len(duplicates) > 0:
        results["warnings"].append(f"Found {len(duplicates)} duplicate team ids: {duplicates['team_id'].to_list()}")
    
    # Validate each team row
    for i, row in enumerate(teams_df.to_dicts()):
        try:
            Team(**row)
            results["valid_rows"] += 1
        except ValidationError as e:
            results["invalid_rows"] += 1
            results["errors"].append({
                "row": i,
                "team_id": row.get("team_id"),
                "errors": e.errors()
            })
            logger.debug(f"Team validation error row {i}: {e.errors()}")

    logger.info("Teams data validation finished with %d valid rows and %d invalid rows.", results["valid_rows"], results["invalid_rows"])

    return results

def validate_players(players_df: pl.DataFrame, team_id_list: list) -> Dict[str, Any]:
    """
    Validate players data.
    
    Args:
        players_df: Players DataFrame.
        team_id_list: List of team_ids for checking orphaned records.
        
    Returns:
        Dict with players data validation results.
    """
    logger.info("Validating players dataset...")

    results = {
        "total_rows": len(players_df),
        "valid_rows": 0,
        "invalid_rows": 0,
        "errors": [],
        "warnings": [],
        "null_counts": {}
    }
    
    if players_df.is_empty():
        results["warnings"].append("Players dataset is empty")
        return results

    # Check for orphaned player records
    orp_players = players_df.filter(
            pl.col("team_id").is_in(team_id_list).not_()
        )
    if len(orp_players) > 0:
        results["warnings"].append(f"Found {len(orp_players)} orphaned team ids: {orp_players['team_id'].to_list()}")

    # Check for duplicate player ids
    duplicates = players_df.group_by("player_id").agg(count = pl.len()).filter(pl.col("count") > 1)
    if len(duplicates) > 0:
        results["warnings"].append(f"Found {len(duplicates)} duplicate player ids: {duplicates['player_id'].to_list()}")
    
    # Check position labels
    if "position" in players_df.columns:
        unique_positions = players_df["position"].unique().to_list()
        results["warnings"].append(f"Found {len(unique_positions)} different position labels: {unique_positions}")
    
    # Check values in market_value
    if "market_value" in players_df.columns:
        null_market = players_df["market_value"].is_null().sum()
        null_pct = (null_market / len(players_df)) * 100
        results["null_counts"]["market_value"] = {
            "null_count": null_market,
            "null_percentage": null_pct
        }
    
    # Check null values in contract_until
    if "contract_until" in players_df.columns:
        null_contract = players_df["contract_until"].is_null().sum()
        null_pct = (null_contract / len(players_df)) * 100
        results["null_counts"]["contract_until"] = {
            "null_count": null_contract,
            "null_percentage": null_pct
        }
    
    # Validate each player row
    for i, row in enumerate(players_df.to_dicts()):
        try:
            Player(**row)
            results["valid_rows"] += 1
        except ValidationError as e:
            results["invalid_rows"] += 1
            results["errors"].append({
                "row": i,
                "player_id": row.get("player_id"),
                "errors": e.errors()
            })

    logger.info("Players data validation finished with %d valid rows and %d invalid rows.", results["valid_rows"], results["invalid_rows"])

    return results

def validate_matches(matches_df: pl.DataFrame) -> Dict[str, Any]:
    """
    Validate matches dataset.
    
    Args:
        matches_df: Matches DataFrame.
        
    Returns:
        Dict with matches data validation results.
    """
    logger.info("Validating matches dataset...")

    results = {
        "total_rows": len(matches_df),
        "valid_rows": 0,
        "invalid_rows": 0,
        "errors": [],
        "warnings": []
    }
    
    if matches_df.is_empty():
        results["warnings"].append("Matches dataset is empty")
        return results
    
    # Check for duplicate match ids
    duplicates = matches_df.group_by("match_id").agg(count = pl.len()).filter(pl.col("count") > 1)
    if len(duplicates) > 0:
        results["warnings"].append(f"Found {len(duplicates)} duplicate match ids: {duplicates['match_id'].to_list()}")
    
    # Validate each match row
    for i, row in enumerate(matches_df.to_dicts()):
        try:
            Match(**row)
            results["valid_rows"] += 1
        except ValidationError as e:
            results["invalid_rows"] += 1
            results["errors"].append({
                "row": i,
                "match_id": row.get("match_id"),
                "errors": e.errors()
            })
    
    logger.info("Matches data validation finished with %d valid rows and %d invalid rows.", results["valid_rows"], results["invalid_rows"])

    return results

def validate_player_match_stats(stats_df: pl.DataFrame, player_id_list: list, match_id_list: list) -> Dict[str, Any]:
    """
    Validate player match statistics.
    
    Args:
        stats_df: Player match stats DataFrame.
        player_id_list: List of player_ids for checking orphaned records.
        match_id_list: List of match_ids for checking orphaned records.
        
    Returns:
        Dict with player match stats validation results.
    """
    logger.info("Validating player match stats dataset...")

    results = {
        "total_rows": len(stats_df),
        "valid_rows": 0,
        "invalid_rows": 0,
        "errors": [],
        "warnings": []
    }
    
    if stats_df.is_empty():
        results["warnings"].append("Player match stats dataset is empty")
        return results

    # Check for orphaned player stats records
    orp_players = stats_df.filter(
            pl.col("player_id").is_in(player_id_list).not_()
        )
    if len(orp_players) > 0:
        results["warnings"].append(f"Found {len(orp_players)} orphaned player ids: {orp_players['player_id'].to_list()}")

    orp_matches = stats_df.filter(
            pl.col("match_id").is_in(match_id_list).not_()
        )
    if len(orp_matches) > 0:
        results["warnings"].append(f"Found {len(orp_matches)} orphaned match ids: {orp_matches['match_id'].to_list()}")
    
    # Check for duplicate event_ids
    if ("player_id" in stats_df.columns) & ("match_id" in stats_df.columns):
        duplicates = (stats_df.select(["player_id", "match_id"])
                              .group_by(["player_id", "match_id"])
                              .agg(count = pl.len()).filter(pl.col("count") > 1))
        if len(duplicates) > 0:
            duplicates = duplicates.with_columns(pl.concat_str([
                    pl.col("player_id"), 
                    pl.col("match_id")
                ], separator= "-").alias("player_match_id"))
            results["warnings"].append(f"Found {len(duplicates)} duplicate player_match ids: {duplicates['player_match_id'].to_list()}")

    # Validate each player match stats row
    for i, row in enumerate(stats_df.to_dicts()):
        try:
            PlayerMatchStats(**row)
            results["valid_rows"] += 1
        except ValidationError as e:
            results["invalid_rows"] += 1
            results["errors"].append({
                "row": i,
                "player_id": row.get("player_id"),
                "match_id": row.get("match_id"),
                "errors": e.errors()
            })

    logger.info("Player match stats data validation finished with %d valid rows and %d invalid rows.", results["valid_rows"], results["invalid_rows"])

    return results

def validate_match_events(events_df: pl.DataFrame, match_id_list: list, team_id_list: list, player_id_list: list) -> Dict[str, Any]:
    """
    Validate match events dataset.
    
    Args:
        events_df: Match events DataFrame.
        match_id_list: List of match_ids for checking orphaned records.
        team_id_list: List of team_ids for checking orphaned records.
        player_id_list: List of player_ids for checking orphaned records.

    Returns:
        Dict with match events data validation results.
    """
    logger.info("Validating match events dataset...")

    results = {
        "total_rows": len(events_df),
        "valid_rows": 0,
        "invalid_rows": 0,
        "errors": [],
        "warnings": []
    }
    
    if events_df.is_empty():
        results["warnings"].append("Match events dataset is empty")
        return results

    # Check for orphaned match event records
    orp_matches = events_df.filter(
            pl.col("match_id").is_in(match_id_list).not_()
        )
    if len(orp_matches) > 0:
        results["warnings"].append(f"Found {len(orp_matches)} orphaned match ids: {orp_matches['match_id'].to_list()}")
    
    orp_teams = events_df.filter(
            pl.col("team_id").is_in(team_id_list).not_()
        )
    if len(orp_teams) > 0:
        results["warnings"].append(f"Found {len(orp_teams)} orphaned team ids: {orp_teams['team_id'].to_list()}")
    
    orp_players = events_df.filter(
            pl.col("player_id").is_in(player_id_list).not_()
        )
    if len(orp_players) > 0:
        results["warnings"].append(f"Found {len(orp_players)} orphaned player ids: {orp_players['player_id'].to_list()}")

    # Check for duplicate event_ids
    if "event_id" in events_df.columns:
        duplicates = events_df.select("event_id").group_by("event_id").agg(count = pl.len()).filter(pl.col("count") > 1)
        if len(duplicates) > 0:
            results["warnings"].append(f"Found {len(duplicates)} duplicate event ids: {duplicates['event_id'].to_list()}")
    
    # Validate each match events row
    for i, row in enumerate(events_df.to_dicts()):
        try:                
            MatchEvent(**row)
            results["valid_rows"] += 1
        except (ValidationError, ValueError) as e:
            results["invalid_rows"] += 1
            results["errors"].append({
                "row": i,
                "event_id": row.get("event_id"),
                "errors": str(e)
            })

    logger.info("Match events data validation finished with %d valid rows and %d invalid rows.", results["valid_rows"], results["invalid_rows"])

    return results

def load_and_validate_data():
    """
    Loads data and starts data validation.
    """
    datasets = load_from_landing()

    validation_results = {}

    if datasets["teams"].count().collect().shape[1] > 0:
        validation_results["teams"] = validate_teams(datasets["teams"].collect())
    else:
        logger.info("Team dataset is empty. Skipping teams validation.")
        validation_results["teams"] = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "errors": [],
            "warnings": ["Teams dataset is empty"]
        }

    if datasets["players"].count().collect().shape[1] > 0:
        validation_results["players"] = validate_players(
                datasets["players"].collect(), 
                datasets["teams"].select("team_id").collect()
        )
    else:
        logger.info("Players dataset is empty. Skipping players validation.")
        validation_results["players"] = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "errors": [],
            "warnings": ["Players dataset is empty"]
        }

    if datasets["matches"].count().collect().shape[1] > 0:
        validation_results["matches"] = validate_matches(datasets["matches"].collect())
    else:
        logger.info("Matches dataset is empty. Skipping matches validation.")
        validation_results["matches"] = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "errors": [],
            "warnings": ["Matches dataset is empty"]
        }

    if datasets["player_match_stats"].count().collect().shape[1] > 0:
        validation_results["player_match_stats"] = validate_player_match_stats(
                datasets["player_match_stats"].collect(),
                datasets["players"].select("player_id").collect(),
                datasets["matches"].select("match_id").collect()
        )
    else:
        logger.info("Player match stats dataset is empty. Skipping player match stats validation.")
        validation_results["player_match_stats"] = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "errors": [],
            "warnings": ["Player match stats dataset is empty"]
        }

    if datasets["match_events"].count().collect().shape[1] > 0:
        validation_results["match_events"] = validate_match_events(
                datasets["match_events"].collect(),
                datasets["matches"].select("match_id").collect(),
                datasets["teams"].select("team_id").collect(),
                datasets["players"].select("player_id").collect()
        )
    else:
        logger.info("Match events dataset is empty. Skipping match events validation.")
        validation_results["match_events"] = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "errors": [],
            "warnings": ["Match events dataset is empty"]
        }

    return validation_results

def create_validation_report(results_df: dict[str, dict[str, Any]]):
    """
    Writes a formatted validation report to a text file.
    """
    report_path = validation_path / f"validation_report_{timestamp}.txt"

    lines: List[str] = []
    lines.append("=" * 80)
    lines.append("DATA VALIDATION REPORT")
    lines.append("=" * 80)
    lines.append("")

    datasets = ["teams", "players", "matches", "player_match_stats", "match_events"]
    for dataset_name in datasets:
        results = results_df.get(dataset_name, {})
        total = results.get("total_rows", 0)
        valid = results.get("valid_rows", 0)
        validity_pct = (valid / total * 100) if total > 0 else 0.0
        lines.append(f"{dataset_name.upper()}:")
        lines.append(f"  Rows: {total:,} | Correct: {valid:,} ({validity_pct:.1f}%)")

        if results.get("warnings"):
            lines.append(f"  Warnings: {len(results['warnings'])}")
            for warning in results["warnings"]:
                lines.append(f"    - {warning}")

        if results.get("errors"):
            lines.append(f"  Errors: {len(results['errors'])}")
            for error in results["errors"]:
                if error.get('errors')[0]['type'] == 'value_error':
                    lines.append(f"    - {error.get('row')}: {error.get('errors')[0]['msg']}")
                else:
                    lines.append(f"    - {error.get('row')}: {str(error.get('errors')[0]['loc']).replace('(', '').replace(')','')} {error.get('errors')[0]['msg']}")

        if results.get("null_counts"):
            lines.append("  Null analysis:")
            for col, counts in results["null_counts"].items():
                lines.append(f"    - {col}: {counts['null_count']} ({counts['null_percentage']:.1f}%)")
        lines.append("")

    lines.append("=" * 80)
    lines.append("")

    try:
        report_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("Validation report written to %s", report_path)
    except Exception as e:
        logger.exception("Failed to write validation report to %s: %s", report_path, e)

def main():
    logger.info("Starting raw data validation...")
    try:
        results = load_and_validate_data()
        create_validation_report(results)
        logger.info("Data validation step finished! See validation report for more information.")
    except Exception as e:
        logger.exception("Validation step failed with an unexpected error: %s", e)
    finally:
        try:
            logger.removeHandler(handler)
            handler.close()
        except Exception as e:
            logger.exception("Failed to close validation file handler: %s", e)

if __name__ == "__main__":
    main()