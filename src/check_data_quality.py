from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_bronze_table(df: DataFrame) -> dict:
    """Runs data quality checks on the Bronze raw games table and asserts integrity."""
    metrics_df = df.select(
        F.count("*").alias("total_rows"),
        F.count_distinct("game_id").alias("distinct_game_ids"),
        F.min("game_id").alias("min_game_id"),
        F.max("game_id").alias("max_game_id"),
        F.sum(F.when(F.col("game_id").isNull(), 1).otherwise(0)).alias(
            "null_game_ids"
        ),
        F.sum(
            F.when(
                F.col("pgn").isNull() | (F.trim(F.col("pgn")) == ""), 1
            ).otherwise(0)
        ).alias("empty_pgns"),
        F.sum(
            F.when(~F.col("pgn").startswith("[Event"), 1).otherwise(0)
        ).alias("invalid_pgn_starts"),
    )

    metrics = metrics_df.first()

    assert (
        metrics["total_rows"] is not None and metrics["total_rows"] > 0
    ), "ERROR: Table is empty."
    assert (
        metrics["null_game_ids"] == 0
    ), f"ERROR: Found {metrics['null_game_ids']} records with NULL in game_id."
    assert metrics["total_rows"] == metrics["distinct_game_ids"], (
        "ERROR: Duplicate primary keys found. Total rows:"
        f" {metrics['total_rows']}, distinct IDs: {metrics['distinct_game_ids']}."
    )
    assert (
        metrics["empty_pgns"] == 0
    ), f"ERROR: Found {metrics['empty_pgns']} empty PGN records."
    assert metrics["invalid_pgn_starts"] == 0, (
        f"ERROR: Found {metrics['invalid_pgn_starts']} records not starting with"
        " '[Event'."
    )

    return metrics.asDict()



## SQL Equivalent 
#   metrics_df = df.selectExpr(
#     "COUNT(*) AS total_rows",
#     "COUNT(DISTINCT game_id) AS distinct_game_ids",
#     "MIN(game_id) AS min_game_id",
#     "MAX(game_id) AS max_game_id",
#     "SUM(CASE WHEN game_id IS NULL THEN 1 ELSE 0 END) AS null_game_ids",
#     "SUM(CASE WHEN pgn IS NULL OR TRIM(pgn) = '' THEN 1 ELSE 0 END) AS empty_pgns",
#     "SUM(CASE WHEN NOT pgn LIKE '[Event%' THEN 1 ELSE 0 END) AS invalid_pgn_starts"
# )