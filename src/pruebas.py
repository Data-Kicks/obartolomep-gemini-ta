import duckdb
from pathlib import Path
from db_utils import teams_top3_query

project_path: Path = Path(__file__).resolve().parents[1]
db_path = project_path / "data" / "processed" / "scouting.duckdb"

conn = duckdb.connect(str(db_path))

result = conn.execute(teams_top3_query).fetch_df()
print(result)

conn.close()
