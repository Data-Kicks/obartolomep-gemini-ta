"""
Data extraction module for soccer scouting pipeline.
Only extracts raw data - no transformation or cleaning.
"""
import polars as pl
import json
from pathlib import Path
from typing import Union
from logging import Logger, basicConfig, getLogger, INFO
from datetime import datetime


def main() -> None:
    project_path: Path = Path(__file__).resolve().parents[1]
    ingest_files(path=project_path / "data" / "raw", output_dir=project_path / "data" / "landing")

def ingest_files(
    path: Union[str, Path],
    output_dir: Union[str, Path],
) -> None:
    """
    Ingest files (CSV and/or JSON) found at `path`, write each as a separate
    Parquet file into `output_dir`.

    Parameters
    - path: directory or file path to read files from
    - output_dir: directory where individual parquet files will be written
    """
    basicConfig(
        level=INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger: Logger = getLogger(name="Ingestion")

    src = Path(path)
    dest = Path(output_dir)

    if not src.exists():
        logger.error("Path not found: %s", src)
    else:
        dest.mkdir(parents=True, exist_ok=True)

        files: list[Path]= list(src.iterdir())

        for file in sorted(files):
            try:
                suf: str = file.suffix.lower()
                df = None

                if suf == ".csv":
                    # Read CSV into Polars DataFrame
                    try:
                        df: pl.DataFrame = pl.read_csv(file)
                        if len(df) == 0:
                            logger.warning("Empty CSV file, skipping: %s", file)
                            continue
                    except Exception as e:
                        logger.exception("Error reading CSV %s: %s", file, e)
                        continue
                elif suf == ".json":
                    file_content: list[dict[str, str]] = []

                    text = file.read_text(encoding="utf-8").strip()
                    if not text:
                        logger.warning("Empty JSON file, skipping: %s", file)
                        continue

                    data_list = None
                    try:
                        parsed = json.loads(text)
                        data_list: list[str] = [parsed]
                    except json.JSONDecodeError as e:
                        logger.exception("Failed to parse line in %s: %s", file, e)
                        if not data_list:
                            logger.warning("No JSON records parsed from %s, skipping", file)
                            continue
                    
                    file_content.append({"file": str(file), "data": json.dumps(data_list, ensure_ascii=False)})
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

                df.write_parquet(parquet_path)
                logger.info("Wrote parquet: %s (rows=%d cols=%d)", parquet_path, df.shape[0], df.shape[1])

            except Exception as e:
                logger.exception("Failed to process %s: %s", file, e)

if __name__ == "__main__":
    main()
