"""
Pytest configuration and fixtures.
"""

import pytest
import polars as pl
from pathlib import Path
import sys

src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


@pytest.fixture
def sample_teams_df():
    """
    Create sample teams dataframe for testing.
    """
    return pl.DataFrame(
        {
            "team_id": ["T001", "T002", "T003"],
            "name": ["Manchester United", "Tottenham Hotspur", "Aston Villa"],
            "league": ["Premier League", "Premier League", "Premier League"],
            "stadium": ["Old Trafford", "Tottenham Hotspur Stadium", "Villa Park"],
            "city": ["Manchester", "London", "Birmingham"],
        }
    )


@pytest.fixture
def sample_teams_df_with_duplicates():
    """
    Create sample teams dataframe with duplicates for testing.
    """
    return pl.DataFrame(
        {
            "team_id": ["T001", "T001"],
            "name": ["Liverpool FC", "Liverpool FC Updated"],
            "league": ["Premier League", "Premier League"],
            "stadium": ["Anfield", "Anfield"],
            "city": ["Liverpool", "Liverpool"],
        }
    )


@pytest.fixture
def sample_players_df():
    """
    Create sample players dataframe for testing.
    """
    return pl.DataFrame(
        {
            "player_id": ["P001", "P002", "P003", "P004"],
            "name": ["Marcus Silva", "Diego Silva", "Bruno Jackson", "Marcus Garcia"],
            "team_id": ["T001", "T002", "T003", "T001"],
            "position": ["forward", "striker", "gk", "midfielder"],
            "date_of_birth": ["1987-06-24", "1987-12-19", "1993-01-07", "2002-11-25"],
            "nationality": ["Spain", "France", "England", "Spain"],
            "market_value": [50000000, 35000000, 40000000, 80000000],
            "contract_until": ["2025-06-30", "2024-06-30", "2026-06-30", "2027-06-30"],
        }
    )


@pytest.fixture
def sample_players_df_with_issues():
    """
    Create sample players dataframe with data quality issues (1 orphaned, 1 duplicated).
    """
    return pl.DataFrame(
        {
            "player_id": ["P001", "P003", "P002", "P002"],
            "name": ["Marcus Silva", "Diego Silva", "Diego Silva", "Pedro Gonzalez"],
            "team_id": ["T001", "T999", "T002", "T003"],
            "position": ["forward", "striker", "striker", "gk"],
            "date_of_birth": ["1987-06-24", "24/06/1987", "19/12/1987", "invalid-date"],
            "nationality": ["Spain", "France", "France", "England"],
            "market_value": [50000000, 35000000, 35000000, 40000000],
            "contract_until": ["2025-06-30", "2025-09-30", "2024-06-30", "2026-06-30"],
        }
    )


@pytest.fixture
def sample_matches_df():
    """
    Create sample matches dataframe for testing.
    """
    return pl.DataFrame(
        {
            "match_id": ["M001", "M002", "M003"],
            "competition": ["Premier League", "Premier League", "Premier League"],
            "season": ["2023-2024", "2023-2024", "2023-2024"],
            "match_date": ["2024-01-15", "2024-01-20", "2024-01-25"],
            "home_team_id": ["T001", "T002", "T001"],
            "away_team_id": ["T002", "T003", "T003"],
            "home_score": [2, 1, 3],
            "away_score": [1, 1, 0],
            "venue": ["Old Trafford", "Tottenham Hotspur Stadium", "Old Trafford"],
            "attendance": [95000, 80000, 70000],
            "referee": ["Michael Oliver", "Stuart Attwell", "Craig Pawson"],
        }
    )


@pytest.fixture
def sample_matches_df_with_duplicates():
    return pl.DataFrame(
        {
            "match_id": ["M001", "M001"],
            "competition": ["Premier League", "Premier League"],
            "season": ["2023-2024", "2023-2024"],
            "match_date": ["2024-01-15", "2024-01-16"],
            "home_team_id": ["T001", "T001"],
            "away_team_id": ["T002", "T002"],
            "home_score": [2, 3],
            "away_score": [1, 0],
            "venue": ["Anfield", "Anfield"],
            "attendance": [95000, 95000],
            "referee": ["Martinez", "Martinez"],
        }
    )


@pytest.fixture
def sample_player_match_stats_df():
    """
    Create sample player match stats dataframe for testing.
    """
    return pl.DataFrame(
        {
            "player_id": ["P001", "P001", "P002", "P003"],
            "match_id": ["M001", "M002", "M001", "M003"],
            "minutes_played": [90, 85, 90, 90],
            "goals": [1, 0, 1, 0],
            "assists": [1, 1, 0, 0],
            "shots": [5, 3, 4, 0],
            "shots_on_target": [3, 2, 2, 0],
            "passes_attempted": [80, 75, 45, 30],
            "passes_completed": [75, 70, 40, 25],
            "key_passes": [4, 3, 2, 0],
            "tackles": [1, 2, 0, 0],
            "interceptions": [0, 1, 0, 0],
            "duels_won": [8, 6, 10, 0],
            "duels_lost": [4, 3, 5, 0],
            "fouls_committed": [1, 0, 2, 0],
            "yellow_cards": [0, 1, 0, 0],
            "red_cards": [0, 0, 0, 0],
            "xg": [0.8, 0.3, 0.6, 0.0],
            "xa": [0.5, 0.4, 0.2, 0.0],
        }
    )


@pytest.fixture
def sample_player_match_stats_df_with_issues():
    """
    Create sample player match stats with data quality issues (2 orphaned, 1 negative xG).
    """
    return pl.DataFrame(
        {
            "player_id": ["P001", "P001", "P999", "P003"],
            "match_id": ["M001", "M999", "M001", "M003"],
            "minutes_played": [90, 85, 90, 90],
            "goals": [1, 0, 1, 0],
            "assists": [1, 1, 0, 0],
            "shots": [5, 3, 4, 0],
            "shots_on_target": [6, 2, 2, 0],
            "passes_attempted": [80, 75, 45, 30],
            "passes_completed": [85, 70, 40, 25],
            "key_passes": [4, 3, 2, 0],
            "tackles": [1, 2, 0, 0],
            "interceptions": [0, 1, 0, 0],
            "duels_won": [8, 6, 10, 0],
            "duels_lost": [4, 3, 5, 0],
            "fouls_committed": [1, 0, 2, 0],
            "yellow_cards": [0, 3, 0, 0],
            "red_cards": [0, 0, 0, 0],
            "xg": [0.8, -0.3, 0.6, 0.0],
            "xa": [0.5, 0.4, 0.2, 0.0],
        }
    )


@pytest.fixture
def sample_match_events_df():
    """
    Create sample match events dataframe for testing.
    """
    return pl.DataFrame(
        {
            "event_id": ["E001", "E002", "E003", "E004"],
            "match_id": ["M001", "M001", "M002", "M003"],
            "minute": [10, 15, 20, 25],
            "second": [30, 45, 15, 0],
            "event_type": ["pass", "shot", "pass", "tackle"],
            "player_id": ["P001", "P001", "P002", "P003"],
            "team_id": ["T001", "T001", "T002", "T003"],
            "x_start": [50.0, 75.0, 60.0, 40.0],
            "y_start": [30.0, 40.0, 50.0, 20.0],
            "x_end": [80.0, 95.0, 70.0, None],
            "y_end": [50.0, 60.0, 30.0, None],
            "outcome": ["successful", "goal", "successful", "successful"],
            "body_part": ["right_foot", "right_foot", "head", None],
            "pass_type": ["through_ball", None, "cross", None],
            "recipient_id": ["P002", None, "P001", None],
        }
    )


@pytest.fixture
def sample_match_events_df_with_issues():
    """
    Create sample match events with data quality issues (1 orphaned).
    """
    return pl.DataFrame(
        {
            "event_id": ["E001", "E002", "E003", "E003"],
            "match_id": ["M001", "M001", "M999", "M003"],
            "minute": [10, -5, 20, 25],
            "second": [30, 60, 15, 0],
            "event_type": ["pass", "shot", "pass", "pass"],
            "player_id": ["P001", "P001", "P999", "P003"],
            "team_id": ["T001", "T001", "T999", "T003"],
            "x_start": [50.0, 120.0, 60.0, 40.0],
            "y_start": [30.0, 40.0, 50.0, 20.0],
            "x_end": [80.0, 95.0, None, None],
            "y_end": [50.0, 60.0, None, None],
            "outcome": ["successful", "goal", "successful", "successful"],
            "body_part": ["right_foot", "right_foot", None, None],
            "pass_type": ["through_ball", None, None, None],
            "recipient_id": ["P002", None, None, None],
        }
    )
