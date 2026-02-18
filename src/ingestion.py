"""
Raw data ingestion script.
Only extracts raw data to landing zone with no transformation or cleaning.
"""

import polars as pl
import json
from typing import Dict
from pathlib import Path
from logging import Logger, basicConfig, FileHandler, Formatter, getLogger, INFO
from datetime import datetime
import duckdb
import os


# Define main paths
project_path: Path = Path(__file__).resolve().parents[1]
ingestion_path = project_path / "outputs" / "logs" / "ingestion"
ingestion_path.mkdir(parents=True, exist_ok=True)
db_path = project_path / "data" / "processed" / "scouting.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)


# Set up logging
basicConfig(level=INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger: Logger = getLogger(name="Ingestion")

timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")

try:
    handler = FileHandler(
        ingestion_path / f"ingestion_log_{timestamp}.log", encoding="utf-8"
    )
    handler.setLevel(INFO)
    handler.setFormatter(
        Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)
except Exception:
    logger.exception(
        "Failed to create file handler for ingestion logs, continuing with console logging only"
    )


# Main ingestion logic
def create_conf_database(table_name: str = "elt_config"):
    """
    Create configuration table in DuckDB if it doesn't exist.
    The table will have a single column 'last_ingestion_date' to track the last date of data ingestion.

    Args:
        table_name: Name of the configuration table to create (default: 'elt_config')
    """
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                last_ingestion_date VARCHAR NOT NULL DEFAULT '1900-01-01'
            );
        """)
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if row_count == 0:
            conn.execute(
                f"INSERT INTO {table_name} (last_ingestion_date) VALUES ('1900-01-01');"
            )
            logger.info(
                "Configuration table '%s' created successfully in DuckDB", table_name
            )
    except Exception as e:
        logger.exception("Failed to create configuration table '%s': %s", table_name, e)
    finally:
        conn.close()


def ingest_from_raw(path: str, output_dir: str, force: bool = False) -> None:
    """
    Ingest files (CSV and/or JSON) found at `path`, write each as a separate
    Parquet file into `output_dir`.

    Args:
        path: Directory or file path to read files from
        output_dir: Directory where individual parquet files will be written
        force: If True, ingests all files regardless of last_ingestion_date in DB.
               If False, only ingests files modified on or after last_ingestion_date.
    """
    src = Path(path)
    dest = Path(output_dir)

    if not src.exists():
        logger.error("Path not found: %s", src)
        return

    try:
        dest.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.exception("Failed to create output directory %s: %s", dest, e)
        return

    try:
        try:
            conn = duckdb.connect(str(db_path))
            try:
                res = conn.execute(
                    "SELECT last_ingestion_date FROM elt_config"
                ).fetchone()
                last_ingestion_date = datetime.strptime(res[0], "%Y-%m-%d").date()
            except Exception:
                logger.exception("Failed to read last_ingestion_date from DB")
            finally:
                conn.close()
        except Exception as e:
            logger.exception(
                "Failed to connect to DB to read last_ingestion_date: %s", e
            )

        files = sorted(
            [
                p
                for pat in ("*.csv", "*.json")
                for p in src.glob(pat)
                if p.is_file()
                and (
                    datetime.fromtimestamp(os.path.getmtime(p)).date()
                    >= last_ingestion_date
                    or force
                )
            ],
            key=lambda p: p.name,
        )
        logger.info(
            "Found %d new files since last ingestion date %s",
            len(files),
            last_ingestion_date,
        )
    except Exception as e:
        logger.exception("Failed to list files in %s: %s", src, e)
        return

    ingested_count = 0

    for file in sorted(files):
        try:
            suf: str = file.suffix.lower()
            df = None

            if suf == ".csv":
                try:
                    df: pl.DataFrame = pl.read_csv(file)
                    if len(df) == 0:
                        logger.warning("Empty CSV file, skipping: %s", file)
                        continue
                except Exception as e:
                    logger.exception("Error reading CSV %s: %s", file, e)
                    continue
            elif suf == ".json":
                file_content = []

                text = file.read_text(encoding="utf-8")
                if not text:
                    logger.warning("Empty JSON file, skipping: %s", file)
                    continue

                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError as e:
                    logger.exception("Failed to parse line in %s: %s", file, e)
                    if not parsed:
                        logger.warning("No JSON records parsed from %s, skipping", file)
                        continue

                file_content.append(
                    {"file": str(file), "data": json.dumps(parsed, ensure_ascii=False)}
                )
                try:
                    df = pl.DataFrame(file_content)
                except Exception as e:
                    logger.exception(
                        "Error writing dataframe %s: %s, skipping", file, e
                    )
                    continue

            else:
                logger.warning("Skipping unsupported suffix: %s", file)
                continue

            parquet_name: Path = f"{file.stem}.parquet"
            parquet_path: Path = dest / parquet_name

            try:
                df.write_parquet(parquet_path)
                ingested_count += 1
                logger.info(
                    "File created: %s (rows=%d cols=%d)",
                    parquet_path,
                    df.shape[0],
                    df.shape[1],
                )
            except Exception as e:
                logger.exception(
                    "Failed to write parquet for %s to %s: %s", file, parquet_path, e
                )
                continue

        except Exception as e:
            logger.exception("Failed to process %s: %s", file, e)

    if ingested_count > 0:
        today_str = datetime.now().date().isoformat()
        try:
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    "UPDATE elt_config SET last_ingestion_date = ?;", (today_str,)
                )
                logger.info(
                    "Updated elt_config.last_ingestion_date to %s (ingested %d files)",
                    today_str,
                    ingested_count,
                )
            except Exception:
                logger.exception(
                    "Failed to update elt_config.last_ingestion_date to %s", today_str
                )
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to connect to DB to update last_ingestion_date")


def load_from_landing(l_path: Path = None) -> Dict[str, pl.LazyFrame]:
    """
    Load datasets from landing zone into memory as Polars LazyFrames.

    Args:
        l_path: Optional path to landing directory. If None, defaults to project_path/data/landing.

    Returns:
        A dictionary with dataset names as keys and corresponding Polars LazyFrames as values.
    """
    if l_path is None:
        landing_path = project_path / "data" / "landing"
    else:
        landing_path = l_path

    datasets = {}

    try:
        teams_df = pl.scan_parquet(landing_path / "teams.parquet")
        datasets["teams"] = teams_df
    except Exception as e:
        logger.exception(
            "Error while loading teams, check if landing folder contains teams parquet files: %s",
            e,
        )
        datasets["teams"] = pl.LazyFrame()
        pass

    try:
        players_df = pl.scan_parquet(landing_path / "players.parquet")
        datasets["players"] = players_df
    except Exception as e:
        logger.exception(
            "Error while loading players, check if landing folder contains players parquet files: %s",
            e,
        )
        datasets["players"] = pl.LazyFrame()
        pass

    try:
        players_match_stats_df = pl.scan_parquet(
            landing_path / "player_match_stats.parquet"
        )
        datasets["player_match_stats"] = players_match_stats_df
    except Exception as e:
        logger.exception(
            "Error while loading player_match_stats, check if landing folder contains player_match_stats parquet files: %s",
            e,
        )
        datasets["player_match_stats"] = pl.LazyFrame()
        pass

    try:
        matches_file = pl.scan_parquet(landing_path / "matches.parquet").collect()
        matches_rows = pl.DataFrame()
        for row in matches_file.iter_rows(named=True):
            match = json.loads(row["data"])
            matches_rows = pl.concat([matches_rows, pl.DataFrame(match)])
        datasets["matches"] = matches_rows.lazy()
    except Exception as e:
        logger.exception(
            "Error while loading matches, check if landing folder contains matches parquet files: %s",
            e,
        )
        datasets["matches"] = pl.LazyFrame()
        pass

    try:
        match_events_file = pl.scan_parquet(
            landing_path / "match_events.parquet"
        ).collect()
        match_events_rows = pl.DataFrame()
        for row in match_events_file.iter_rows(named=True):
            match_events = json.loads(row["data"])
            match_events_rows = pl.concat(
                [match_events_rows, pl.DataFrame(match_events)]
            )
        datasets["match_events"] = match_events_rows.lazy()
    except Exception as e:
        logger.exception(
            "Error while loading match_events. Check if landing folder contains match_events parquet files: %s",
            e,
        )
        datasets["match_events"] = pl.LazyFrame()
        pass

    return datasets


def main() -> None:
    logger.info("Starting raw data ingestion...")

    try:
        create_conf_database()
        ingest_from_raw(
            path=project_path / "data" / "raw",
            output_dir=project_path / "data" / "landing",
        )
        logger.info(
            "Data ingestion step finished. See ingestion log for more information."
        )
    except Exception as e:
        logger.exception("Ingestion step failed with an unexpected error: %s", e)
    finally:
        try:
            logger.removeHandler(handler)
            handler.close()
        except Exception as e:
            logger.exception("Failed to close ingestion file handler: %s", e)


if __name__ == "__main__":
    main()
