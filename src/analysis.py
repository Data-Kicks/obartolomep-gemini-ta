import polars as pl
from pathlib import Path
import duckdb
from logging import Logger, getLogger


# Define main paths
project_path: Path = Path(__file__).resolve().parents[1]
db_path = project_path / "data" / "processed" / "scouting.duckdb"
db_path.parent.mkdir(parents=True, exist_ok=True)


logger: Logger = getLogger(name="Analysis")


def create_player_aggregated_view():
    conn = duckdb.connect(str(db_path))

    player_match_stats = conn.execute("""SELECT * FROM fact_player_match_stats""").pl()

    player_aggregated_stats = player_match_stats.group_by("player_id").agg([
        pl.sum("goals").alias("goals"),
        pl.sum("assists").alias("assists"),
        pl.sum("minutes_played").alias("minutes_played"),
        pl.sum("shots").alias("shots"),
        pl.sum("shots_on_target").alias("shots_on_target"),
        pl.sum("passes_attempted").alias("passes_attempted"),
        pl.sum("passes_completed").alias("passes_completed"),
        pl.sum("key_passes").alias("key_passes"),
        pl.sum("tackles").alias("tackles"),
        pl.sum("interceptions").alias("interceptions"),
        pl.sum("duels_won").alias("duels_won"),
        pl.sum("duels_lost").alias("duels_lost"),
        pl.sum("fouls_committed").alias("fouls_committed"),
        pl.sum("yellow_cards").alias("yellow_cards"),
        pl.sum("red_cards").alias("red_cards"),
        pl.sum("xg").alias("xg"),
        pl.sum("xa").alias("xa")
    ])

    player_aggregated_stats = player_aggregated_stats.with_columns([
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("goals") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("goals_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("assists") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("assists_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("shots_on_target") / pl.col("shots"))
            .otherwise(None)
            .round(2)
            .alias("shot_accuracy")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("passes_completed") / pl.col("passes_attempted"))
            .otherwise(None)
            .round(2)
            .alias("pass_accuracy")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("key_passes") / pl.col("passes_attempted"))
            .otherwise(None)
            .round(2)
            .alias("key_passes_pass")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("tackles") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("tackles_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("interceptions") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("interceptions_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("duels_won") / (pl.col("duels_won") + pl.col("duels_lost")))
            .otherwise(None)
            .round(2)
            .alias("duel_win_rate")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("fouls_committed") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("fouls_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("yellow_cards") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("yellow_cards_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("red_cards") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("red_cards_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("xg") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2).alias("xg_90")),
        (pl.when(pl.col("minutes_played") >= 1)
            .then(pl.col("xa") / pl.col("minutes_played") * 90)
            .otherwise(None)
            .round(2)
            .alias("xa_90"))
    ])

    try:
        conn.register("temp_player_agg", player_aggregated_stats.to_arrow())
        query = """
                    CREATE OR REPLACE TABLE agg_player_stats AS
                    SELECT * FROM temp_player_agg
                """
        conn.execute(query)

        conn.execute("DROP VIEW IF EXISTS temp_player_agg")
    except Exception as e:
            logger.error(f"Error saving aggregates table to database: {e}")
            raise
    finally:
        conn.close()

def main():
    create_player_aggregated_view()

if __name__ == "__main__":
    main()