"""
Unit tests for transformation script functions.
"""

from datetime import datetime
from unittest.mock import patch

from transformation import (
    map_player_position,
    parse_date,
    calculate_age,
    transform_teams,
    transform_players,
    transform_matches,
    transform_player_match_stats,
    transform_match_events,
    transform_all,
)


class TestMappingFunctions:
    """
    Test mapping and helper functions.
    """

    def test_map_player_position_standard(self):
        """
        Test mapping standard positions.
        """
        assert map_player_position("gk") == "Goalkeeper"
        assert map_player_position("goalkeeper") == "Goalkeeper"
        assert map_player_position("defender") == "Defender"
        assert map_player_position("cb") == "Defender"
        assert map_player_position("midfielder") == "Midfielder"
        assert map_player_position("cm") == "Midfielder"
        assert map_player_position("forward") == "Forward"
        assert map_player_position("striker") == "Forward"

    def test_map_player_position_edge_cases(self):
        """
        Test mapping edge cases.
        """
        assert map_player_position(None) == "Unknown"
        assert map_player_position("") == "Unknown"
        assert map_player_position("unknown") == "Unknown"
        assert map_player_position("CENTER_BACK") == "Unknown"
        assert map_player_position("   ") == "Unknown"

    def test_parse_date_valid_formats(self):
        """
        Test parsing valid date formats.
        """
        assert parse_date("2023-01-15") == "2023-01-15"
        assert parse_date("15/01/2023") == "2023-01-15"
        assert parse_date("2023/01/15") == "2023-01-15"

    def test_parse_date_edge_cases(self):
        """
        Test parsing edge cases.
        """
        assert parse_date(None) is None
        assert parse_date("") is None
        assert parse_date("invalid-date") is None
        assert parse_date("2023-13-45") is None

    def test_calculate_age(self):
        """
        Test age calculation.
        """
        with patch("transformation.datetime") as mock_datetime:
            mock_datetime.today.return_value = datetime(2024, 1, 1)
            mock_datetime.strptime = datetime.strptime

            assert calculate_age("1990-01-01") == 34
            assert calculate_age("2000-12-31") == 23

    def test_calculate_age_invalid(self):
        """
        Test age calculation with invalid input.
        """
        assert calculate_age("invalid") is None
        assert calculate_age(None) is None
        assert calculate_age("") is None


class TestTransformTeams:
    """
    Test teams transformation.
    """

    def test_transform_teams_success(self, sample_teams_df):
        """
        Test successful teams transformation.
        """
        result = transform_teams(sample_teams_df.lazy())
        assert len(result) == 3
        assert "team_id" in result.columns
        assert "name" in result.columns

    def test_transform_teams_duplicates(self, sample_teams_df_with_duplicates):
        """
        Test teams transformation with duplicates.
        """
        result = transform_teams(sample_teams_df_with_duplicates.lazy())
        assert len(result) == 1
        assert result["name"][0] == "Liverpool FC Updated"


class TestTransformPlayers:
    """
    Test players transformation.
    """

    def test_transform_players_success(self, sample_players_df, sample_teams_df):
        """
        Test successful players transformation.
        """
        team_ids = sample_teams_df.select("team_id")
        result = transform_players(sample_players_df.lazy(), team_ids)

        assert len(result) == 4
        assert "position" in result.columns
        assert "age" in result.columns

    def test_transform_players_with_issues(
        self, sample_players_df_with_issues, sample_teams_df
    ):
        """
        Test players transformation with data quality issues.
        """
        team_ids = sample_teams_df.select("team_id")
        result = transform_players(sample_players_df_with_issues.lazy(), team_ids)

        # Should filter out orphaned records and duplicates
        assert len(result) == 2


class TestTransformMatches:
    """
    Test matches transformation.
    """

    def test_transform_matches_success(self, sample_matches_df):
        """
        Test successful matches transformation.
        """
        result = transform_matches(sample_matches_df.lazy())
        assert len(result) == 3
        assert "match_date" in result.columns

    def test_transform_matches_duplicates(self, sample_matches_df_with_duplicates):
        """
        Test matches transformation with duplicates.
        """
        result = transform_matches(sample_matches_df_with_duplicates.lazy())
        assert len(result) == 1


class TestTransformPlayerMatchStats:
    """
    Test player match stats transformation.
    """

    def test_transform_player_match_stats_success(
        self, sample_player_match_stats_df, sample_players_df, sample_matches_df
    ):
        """
        Test successful player match stats transformation.
        """
        player_ids = sample_players_df.select("player_id")
        match_ids = sample_matches_df.select("match_id")

        result = transform_player_match_stats(
            sample_player_match_stats_df.lazy(), player_ids, match_ids
        )

        assert len(result) == 4
        assert "goal_contributions" in result.columns
        assert "g_xg" in result.columns

    def test_transform_player_match_stats_fix_issues(
        self,
        sample_player_match_stats_df_with_issues,
        sample_players_df,
        sample_matches_df,
    ):
        """
        Test fixing data quality issues in player match stats.
        """
        player_ids = sample_players_df.select("player_id")
        match_ids = sample_matches_df.select("match_id")

        result = transform_player_match_stats(
            sample_player_match_stats_df_with_issues.lazy(), player_ids, match_ids
        )

        # Should filter orphaned records
        assert len(result) == 2

        # Check xg fixes (negative xg -> None)
        xg_values = result["xg"].to_list()
        assert all(x is None or x >= 0 for x in xg_values)


class TestTransformMatchEvents:
    """
    Test match events transformation.
    """

    def test_transform_match_events_success(
        self,
        sample_match_events_df,
        sample_matches_df,
        sample_teams_df,
        sample_players_df,
    ):
        """
        Test successful match events transformation.
        """
        match_ids = sample_matches_df.select("match_id")
        team_ids = sample_teams_df.select("team_id")
        player_ids = sample_players_df.select("player_id")

        result = transform_match_events(
            sample_match_events_df.lazy(), match_ids, team_ids, player_ids
        )

        assert len(result) > 0
        assert len(result) <= len(sample_match_events_df)

    def test_transform_match_events_filter_orphaned(
        self,
        sample_match_events_df_with_issues,
        sample_matches_df,
        sample_teams_df,
        sample_players_df,
    ):
        """
        Test filtering orphaned match events.
        """
        match_ids = sample_matches_df.select("match_id")
        team_ids = sample_teams_df.select("team_id")
        player_ids = sample_players_df.select("player_id")

        result = transform_match_events(
            sample_match_events_df_with_issues.lazy(), match_ids, team_ids, player_ids
        )

        assert len(result) == 3


class TestTransformAll:
    """
    Test the complete transformation pipeline.
    """

    def test_transform_all_success(
        self,
        sample_teams_df,
        sample_players_df,
        sample_matches_df,
        sample_player_match_stats_df,
        sample_match_events_df,
    ):
        """
        Test successful transformation of all datasets.
        """
        datasets = {
            "teams": sample_teams_df.lazy(),
            "players": sample_players_df.lazy(),
            "matches": sample_matches_df.lazy(),
            "player_match_stats": sample_player_match_stats_df.lazy(),
            "match_events": sample_match_events_df.lazy(),
        }

        result = transform_all(datasets)

        assert "teams" in result
        assert "players" in result
        assert "matches" in result
        assert "player_match_stats" in result
        assert "match_events" in result

        assert not result["teams"].is_empty()
        assert not result["players"].is_empty()
        assert not result["matches"].is_empty()
        assert not result["player_match_stats"].is_empty()
        assert not result["match_events"].is_empty()
