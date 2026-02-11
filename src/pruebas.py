import duckdb
from pathlib import Path

project_path: Path = Path(__file__).resolve().parents[1]
db_path = project_path / "data" / "processed" / "scouting.duckdb"

conn = duckdb.connect(str(db_path))

result = conn.execute("""SELECT * FROM agg_player_stats""").fetch_df()
print(result)
