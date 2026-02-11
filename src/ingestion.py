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


# Define main paths
project_path: Path = Path(__file__).resolve().parents[1]
ingestion_path = project_path / "outputs" / "logs" / "ingestion"
ingestion_path.mkdir(parents=True, exist_ok=True)


# Set up logging
basicConfig(
    level=INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger: Logger = getLogger(name="Ingestion")

timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")

try:
    handler = FileHandler(ingestion_path / f"ingestion_log_{timestamp}.log", encoding="utf-8")
    handler.setLevel(INFO)
    handler.setFormatter(Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
except Exception:
    logger.exception("Failed to create file handler for ingestion logs, continuing with console logging only")


    
# Main ingestion logic
def ingest_from_raw(
    path: str,
    output_dir: str,
) -> None:
    """
    Ingest files (CSV and/or JSON) found at `path`, write each as a separate
    Parquet file into `output_dir`.

    Parameters
    - path: directory or file path to read files from
    - output_dir: directory where individual parquet files will be written
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
        files = sorted(
            [p for pat in ("*.csv", "*.json") for p in src.glob(pat) if p.is_file()],
            key=lambda p: p.name,
        )
    except Exception as e:
        logger.exception("Failed to list files in %s: %s", src, e)
        return

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
                
                file_content.append({"file": str(file), "data": json.dumps(parsed, ensure_ascii=False)})
                try:
                    df = pl.DataFrame(file_content)
                except Exception as e:
                    logger.exception("Error writing dataframe %s: %s, skipping", file, e)
                    continue

            else:
                logger.warning("Skipping unsupported suffix: %s", file)
                continue
            
            timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
            parquet_name: str = f"{file.stem}_{timestamp}.parquet"
            parquet_path: Path = dest / parquet_name

            try:
                df.write_parquet(parquet_path)
                logger.info("File created: %s (rows=%d cols=%d)", parquet_path, df.shape[0], df.shape[1])
            except Exception as e:
                logger.exception("Failed to write parquet for %s to %s: %s", file, parquet_path, e)
                continue

        except Exception as e:
            logger.exception("Failed to process %s: %s", file, e)


def load_from_landing() -> Dict[str, pl.LazyFrame]:
    datasets = {}

    teams_df = pl.scan_parquet(project_path / "data/landing/teams_*.parquet")
    datasets["teams"] = teams_df

    players_df = pl.scan_parquet(project_path / "data/landing/players_*.parquet")
    datasets["players"] = players_df

    players_match_stats_df = pl.scan_parquet(project_path / "data/landing/player_match_stats_*.parquet")
    datasets["player_match_stats"] = players_match_stats_df

    try:
        matches_file = pl.scan_parquet(project_path / "data/landing/matches_*.parquet").collect()
        matches_rows = pl.DataFrame()
        for row in matches_file.iter_rows(named=True):
            match = json.loads(row["data"])
            matches_rows = pl.concat([matches_rows, pl.DataFrame(match)])
        datasets["matches"] = matches_rows.lazy()
    except Exception as e:
        logger.exception("Error while loading matches: %s", e)
        datasets["matches"] = pl.LazyFrame()

    try:
        match_events_file = pl.scan_parquet(project_path / "data/landing/match_events_*.parquet").collect()
        match_events_rows = pl.DataFrame()
        for row in match_events_file.iter_rows(named=True):
            match_events = json.loads(row["data"])
            match_events_rows = pl.concat([match_events_rows, pl.DataFrame(match_events)])
        datasets["match_events"] = match_events_rows.lazy()
    except Exception as e:
        logger.exception("Error while loading match_events: %s", e)
        datasets["match_events"] = pl.LazyFrame()

    return datasets
        
def main() -> None:
    logger.info("Starting raw data ingestion...")

    try:
        ingest_from_raw(path=project_path / "data" / "raw", output_dir=project_path / "data" / "landing")
        logger.info("Data ingestion step finished. See ingestion log for more information.")
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
