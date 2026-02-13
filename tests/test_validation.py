"""
Unit tests for validation script functions.
"""
import polars as pl
from validation import (
    validate_teams, validate_players, validate_matches,
    validate_player_match_stats, validate_match_events
)


class TestValidateTeams:
    """
    Test validate_teams function.
    """
    def test_validate_teams_success(self, sample_teams_df):
        """
        Test successful teams validation.
        """
        results = validate_teams(sample_teams_df)
        
        assert results["total_rows"] == 3
        assert results["valid_rows"] == 3
        assert results["invalid_rows"] == 0
        assert len(results["errors"]) == 0
        assert len(results["warnings"]) == 0

    def test_validate_teams_with_duplicates(self, sample_teams_df_with_duplicates):
        """
        Test teams validation with duplicate team_ids.
        """
        results = validate_teams(sample_teams_df_with_duplicates)
        
        assert results["total_rows"] == 2
        assert results["valid_rows"] == 2
        assert results["invalid_rows"] == 0
        assert len(results["warnings"]) == 1
        assert "duplicate team ids" in results["warnings"][0]

    def test_validate_teams_empty(self):
        """
        Test teams validation with empty dataframe
        ."""
        empty_df = pl.DataFrame(schema={
            "team_id": pl.Utf8,
            "name": pl.Utf8,
            "league": pl.Utf8,
            "stadium": pl.Utf8,
            "city": pl.Utf8
        })
        
        results = validate_teams(empty_df)
        
        assert results["total_rows"] == 0
        assert results["valid_rows"] == 0
        assert results["invalid_rows"] == 0
        assert "Teams dataset is empty" in results["warnings"]

    def test_validate_teams_invalid_data(self):
        """
        Test teams validation with invalid data.
        """
        invalid_df = pl.DataFrame({
            "team_id": ["T001", None],
            "name": ["Liverpool FC", "Tottenham Hotspur"],
            "league": ["Premier League", "Premier League"],
            "stadium": ["Anfield", "Tottenham Hotspur Stadium"],
            "city": ["Liverpool", "London"]
        })
        
        results = validate_teams(invalid_df)
        
        assert results["total_rows"] == 2
        assert results["valid_rows"] == 1
        assert results["invalid_rows"] == 1
        assert len(results["errors"]) == 1
        assert results["errors"][0]["team_id"] is None


class TestValidatePlayers:
    """
    Test validate_players function.
    """
    def test_validate_players_success(self, sample_players_df, sample_teams_df):
        """
        Test successful players validation.
        """
        team_ids = sample_teams_df.select("team_id")
        results = validate_players(sample_players_df, team_ids)
        
        assert results["total_rows"] == 4
        assert results["valid_rows"] == 4
        assert results["invalid_rows"] == 0
        assert len(results["errors"]) == 0
        
        # Should have warnings for position labels
        position_warnings = [w for w in results["warnings"] if "position labels" in w]
        assert len(position_warnings) > 0

    def test_validate_players_with_issues(self, sample_players_df_with_issues, sample_teams_df):
        """
        Test players validation with data quality issues.
        """
        team_ids = sample_teams_df.select("team_id")
        results = validate_players(sample_players_df_with_issues, team_ids)
        
        assert results["total_rows"] == 4
        assert results["invalid_rows"] > 0
        assert len(results["errors"]) > 0
        assert len(results["warnings"]) > 0
        
        # Check for orphaned records warning
        orphaned_warnings = [w for w in results["warnings"] if "orphaned" in w]
        assert len(orphaned_warnings) > 0
        
        # Check for duplicate player ids warning
        duplicate_warnings = [w for w in results["warnings"] if "duplicate player ids" in w]
        assert len(duplicate_warnings) > 0
        
        # Check null counts
        assert "market_value" in results["null_counts"]
        assert "contract_until" in results["null_counts"]

    def test_validate_players_empty(self, sample_teams_df):
        """
        Test players validation with empty dataframe.
        """
        empty_df = pl.DataFrame(schema={
            "player_id": pl.Utf8,
            "name": pl.Utf8,
            "team_id": pl.Utf8,
            "position": pl.Utf8,
            "date_of_birth": pl.Utf8,
            "nationality": pl.Utf8,
            "market_value": pl.Float64,
            "contract_until": pl.Utf8
        })
        
        team_ids = sample_teams_df.select("team_id")
        results = validate_players(empty_df, team_ids)
        
        assert results["total_rows"] == 0
        assert "Players dataset is empty" in results["warnings"]

    def test_validate_players_orphaned_only(self, sample_players_df, sample_teams_df):
        """
        Test validation with only orphaned players.
        """
        team_ids = ["T001", "T002"]
        results = validate_players(sample_players_df, team_ids)
        
        assert results["total_rows"] == 4
        assert results["valid_rows"] == 4
        assert results["invalid_rows"] == 0
        
        orphaned_warnings = [w for w in results["warnings"] if "orphaned" in w]
        assert len(orphaned_warnings) == 1


class TestValidateMatches:
    """
    Test validate_matches function.
    """

    def test_validate_matches_success(self, sample_matches_df):
        """
        Test successful matches validation.
        """
        results = validate_matches(sample_matches_df)
        
        assert results["total_rows"] == 3
        assert results["valid_rows"] == 3
        assert results["invalid_rows"] == 0
        assert len(results["errors"]) == 0
        assert len(results["warnings"]) == 0

    def test_validate_matches_with_duplicates(self, sample_matches_df_with_duplicates):
        """
        Test matches validation with duplicate match_ids.
        """
        results = validate_matches(sample_matches_df_with_duplicates)
        
        assert results["total_rows"] == 2
        assert results["valid_rows"] == 2
        assert results["invalid_rows"] == 0
        assert len(results["warnings"]) == 1
        assert "duplicate match ids" in results["warnings"][0]

    def test_validate_matches_empty(self):
        """
        Test matches validation with empty dataframe.
        """
        empty_df = pl.DataFrame(schema={
            "match_id": pl.Utf8,
            "competition": pl.Utf8,
            "season": pl.Utf8,
            "match_date": pl.Utf8,
            "home_team_id": pl.Utf8,
            "away_team_id": pl.Utf8,
            "home_score": pl.Int64,
            "away_score": pl.Int64,
            "venue": pl.Utf8,
            "attendance": pl.Int64,
            "referee": pl.Utf8
        })
        
        results = validate_matches(empty_df)
        
        assert results["total_rows"] == 0
        assert "Matches dataset is empty" in results["warnings"]

    def test_validate_matches_invalid_scores(self):
        """
        Test matches validation with invalid scores.
        """
        invalid_df = pl.DataFrame({
            "match_id": ["M001", "M002"],
            "competition": ["Premier League", "Premier League"],
            "season": ["2023-2024", "2023-2024"],
            "match_date": ["2024-01-15", "invalid-date"],
            "home_team_id": ["T001", "T002"],
            "away_team_id": ["T002", "T001"],
            "home_score": [-1, 2],
            "away_score": [1, -2],
            "venue": ["Anfield", "Tottenham Hotspur Stadium"],
            "attendance": [95000, -100],
            "referee": ["Michael Oliver", "Stuart Attwell"]
        })
        
        results = validate_matches(invalid_df)
        
        assert results["total_rows"] == 2
        assert results["invalid_rows"] == 2
        assert len(results["errors"]) == 2


class TestValidatePlayerMatchStats:
    """
    Test validate_player_match_stats function.
    """
    def test_validate_player_match_stats_success(self, sample_player_match_stats_df, 
                                                sample_players_df, sample_matches_df):
        """
        Test successful player match stats validation.
        """
        player_ids = sample_players_df.select("player_id")
        match_ids = sample_matches_df.select("match_id")
        
        results = validate_player_match_stats(
            sample_player_match_stats_df,
            player_ids,
            match_ids
        )
        
        assert results["total_rows"] == 4
        assert results["valid_rows"] == 4
        assert results["invalid_rows"] == 0
        assert len(results["errors"]) == 0

    def test_validate_player_match_stats_with_issues(self, 
                                                    sample_player_match_stats_df_with_issues,
                                                    sample_players_df, sample_matches_df):
        """
        Test player match stats validation with data quality issues.
        """
        player_ids = sample_players_df.select("player_id")
        match_ids = sample_matches_df.select("match_id")
        
        results = validate_player_match_stats(
            sample_player_match_stats_df_with_issues,
            player_ids,
            match_ids
        )
        
        assert results["total_rows"] == 4
        assert results["invalid_rows"] > 0
        assert len(results["errors"]) > 0
        assert len(results["warnings"]) > 0
        
        # Check for orphaned records warnings
        orphaned_players = [w for w in results["warnings"] if "orphaned player ids" in w]
        assert len(orphaned_players) == 1
        
        orphaned_matches = [w for w in results["warnings"] if "orphaned match ids" in w]
        assert len(orphaned_matches)  == 1

    def test_validate_player_match_stats_empty(self, sample_players_df, sample_matches_df):
        """
        Test player match stats validation with empty dataframe.
        """
        empty_df = pl.DataFrame(schema={
            "player_id": pl.Utf8,
            "match_id": pl.Utf8,
            "minutes_played": pl.Int64,
            "goals": pl.Int64,
            "assists": pl.Int64,
            "shots": pl.Int64,
            "shots_on_target": pl.Int64,
            "passes_attempted": pl.Int64,
            "passes_completed": pl.Int64,
            "key_passes": pl.Int64,
            "tackles": pl.Int64,
            "interceptions": pl.Int64,
            "duels_won": pl.Int64,
            "duels_lost": pl.Int64,
            "fouls_committed": pl.Int64,
            "yellow_cards": pl.Int64,
            "red_cards": pl.Int64,
            "xg": pl.Float64,
            "xa": pl.Float64
        })
        
        player_ids = sample_players_df.select("player_id")
        match_ids = sample_matches_df.select("match_id")
        
        results = validate_player_match_stats(empty_df, player_ids, match_ids)
        
        assert results["total_rows"] == 0
        assert "Player match stats dataset is empty" in results["warnings"]


class TestValidateMatchEvents:
    """
    Test validate_match_events function.
    """

    def test_validate_match_events_success(self, sample_match_events_df,
                                          sample_matches_df, sample_teams_df, sample_players_df):
        """
        Test successful match events validation.
        """
        match_ids = sample_matches_df.select("match_id")
        team_ids = sample_teams_df.select("team_id")
        player_ids = sample_players_df.select("player_id")
        
        results = validate_match_events(
            sample_match_events_df,
            match_ids,
            team_ids,
            player_ids
        )
        
        assert results["total_rows"] == 4
        assert results["valid_rows"] == 4
        assert results["invalid_rows"] == 0
        assert len(results["errors"]) == 0

    def test_validate_match_events_with_issues(self, sample_match_events_df_with_issues,
                                              sample_matches_df, sample_teams_df, sample_players_df):
        """
        Test match events validation with data quality issues.
        """
        match_ids = sample_matches_df.select("match_id")
        team_ids = sample_teams_df.select("team_id")
        player_ids = sample_players_df.select("player_id")
        
        results = validate_match_events(
            sample_match_events_df_with_issues,
            match_ids,
            team_ids,
            player_ids
        )
        
        assert results["total_rows"] == 4
        assert results["invalid_rows"] > 0
        assert len(results["errors"]) > 0
        assert len(results["warnings"]) > 0
        
        # Check for orphaned records warnings
        orphaned_matches = [w for w in results["warnings"] if "orphaned match ids" in w]
        assert len(orphaned_matches) == 1
        
        orphaned_teams = [w for w in results["warnings"] if "orphaned team ids" in w]
        assert len(orphaned_teams) == 1
        
        orphaned_players = [w for w in results["warnings"] if "orphaned player ids" in w]
        assert len(orphaned_players) == 1
        
        # Check for duplicate event ids warning
        duplicate_warnings = [w for w in results["warnings"] if "duplicate event ids" in w]
        assert len(duplicate_warnings) > 0

    def test_validate_match_events_empty(self, sample_matches_df, sample_teams_df, sample_players_df):
        """
        Test match events validation with empty dataframe.
        """
        empty_df = pl.DataFrame(schema={
            "event_id": pl.Utf8,
            "match_id": pl.Utf8,
            "minute": pl.Int64,
            "second": pl.Int64,
            "event_type": pl.Utf8,
            "player_id": pl.Utf8,
            "team_id": pl.Utf8,
            "x_start": pl.Float64,
            "y_start": pl.Float64,
            "x_end": pl.Float64,
            "y_end": pl.Float64,
            "outcome": pl.Utf8,
            "body_part": pl.Utf8,
            "pass_type": pl.Utf8,
            "recipient_id": pl.Utf8
        })
        
        match_ids = sample_matches_df.select("match_id")
        team_ids = sample_teams_df.select("team_id")
        player_ids = sample_players_df.select("player_id")
        
        results = validate_match_events(empty_df, match_ids, team_ids, player_ids)
        
        assert results["total_rows"] == 0
        assert "Match events dataset is empty" in results["warnings"]
