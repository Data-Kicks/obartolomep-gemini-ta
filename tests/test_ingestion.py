"""
Unit tests for ingestion script functions
"""

import polars as pl
import json

from ingestion import ingest_from_raw, load_from_landing


class TestIngestFromRaw:
    """
    Test the ingest_from_raw function.
    """

    def test_ingest_csv_success(self, tmp_path):
        """
        Test successful ingestion of CSV file.
        """
        src_dir = tmp_path / "raw"
        src_dir.mkdir()
        csv_file = src_dir / "test.csv"
        csv_file.write_text("col1,col2\n1,2\n3,4")

        dest_dir = tmp_path / "data" / "landing"

        ingest_from_raw(str(src_dir), str(dest_dir), force=True)

        parquet_files = list(dest_dir.glob("*.parquet"))
        assert len(parquet_files) == 1

        df = pl.read_parquet(parquet_files[0])
        assert df.shape == (2, 2)
        assert df.columns == ["col1", "col2"]

    def test_ingest_json_success(self, tmp_path):
        """
        Test successful ingestion of JSON file.
        """

        src_dir = tmp_path / "raw"
        src_dir.mkdir()
        json_file = src_dir / "test.json"
        json_data = {"id": 1, "name": "test"}
        json_file.write_text(json.dumps(json_data))

        dest_dir = tmp_path / "data" / "landing"

        ingest_from_raw(str(src_dir), str(dest_dir), force=True)

        parquet_files = list(dest_dir.glob("*.parquet"))
        assert len(parquet_files) == 1

        df = pl.read_parquet(parquet_files[0])
        assert df.shape == (1, 2)
        assert "file" in df.columns
        assert "data" in df.columns

    def test_ingest_invalid_json(self, tmp_path):
        """
        Test ingestion of invalid JSON file.
        """
        src_dir = tmp_path / "raw"
        src_dir.mkdir()
        json_file = src_dir / "invalid.json"
        json_file.write_text("{invalid json}")

        dest_dir = tmp_path / "data" / "landing"

        ingest_from_raw(str(src_dir), str(dest_dir))

        parquet_files = list(dest_dir.glob("*.parquet"))
        assert len(parquet_files) == 0

    def test_ingest_unsupported_file(self, tmp_path):
        """
        Test ingestion of unsupported file type.
        """
        src_dir = tmp_path / "raw"
        src_dir.mkdir()
        txt_file = src_dir / "test.txt"
        txt_file.write_text("some text")

        dest_dir = tmp_path / "data" / "landing"

        ingest_from_raw(str(src_dir), str(dest_dir))

        parquet_files = list(dest_dir.glob("*.parquet"))
        assert len(parquet_files) == 0


class TestLoadFromLanding:
    """
    Test the load_from_landing function.
    """

    def test_load_from_landing_success(self, tmp_path, monkeypatch):
        """
        Test successful loading from landing directory.
        """
        landing_dir = tmp_path / "data" / "landing"
        landing_dir.mkdir(parents=True)

        teams_df = pl.DataFrame({"team_id": ["T001"], "name": ["Liverpool FC"]})
        teams_df.write_parquet(landing_dir / "teams_20240101.parquet")

        players_df = pl.DataFrame({"player_id": ["P001"], "name": ["Bukayo Anderson"]})
        players_df.write_parquet(landing_dir / "players_20240101.parquet")

        player_match_stats_df = pl.DataFrame({"player_id": ["P001"], "goals": [1]})
        player_match_stats_df.write_parquet(
            landing_dir / "player_match_stats_20240101.parquet"
        )

        datasets = load_from_landing(landing_dir)

        assert "teams" in datasets
        assert "players" in datasets
        assert "player_match_stats" in datasets
        assert "matches" in datasets
        assert "match_events" in datasets

    def test_load_from_landing_matches_json(self, tmp_path, monkeypatch):
        """
        Test loading matches from JSON in landing.
        """
        landing_dir = tmp_path / "data" / "landing"
        landing_dir.mkdir(parents=True)

        match_data = [{"match_id": "M001", "competition": "Premier League"}]

        df = pl.DataFrame([{"file": "matches.json", "data": json.dumps(match_data)}])
        df.write_parquet(landing_dir / "matches.parquet")

        datasets = load_from_landing(landing_dir)

        matches_df = datasets["matches"].collect()
        assert len(matches_df) > 0
        assert "match_id" in matches_df.columns

    def test_load_from_landing_no_files(self, tmp_path):
        """
        Test loading with no files in landing.
        """
        landing_dir = tmp_path / "data" / "landing"
        landing_dir.mkdir(parents=True)

        datasets = load_from_landing(landing_dir)

        assert len(datasets["matches"].collect()) == 0
        assert len(datasets["match_events"].collect()) == 0
