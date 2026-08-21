# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ##Install Requirements

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ##Download File

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst -P /tmp/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decompress File

# COMMAND ----------

# MAGIC %sh zstd -d /tmp/lichess_db_standard_rated_2013-01.pgn.zst -o /tmp/lichess_db_standard_rated_2013-01.pgn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Table If Exists

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lichess.bronze.lichess_db_standard_rated_2013_01

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write File to Table

# COMMAND ----------

import sys
sys.path.append('..')

from src.pgn_parsing import stream_pgn_records, batch_generator

FILE = '/tmp/lichess_db_standard_rated_2013-01.pgn'
TABLE = 'lichess.bronze.lichess_db_standard_rated_2013_01'
BATCH_SIZE = 10000


def write_batch(spark_session, rows: list[dict], table_name: str) -> None:
    '''
    This function creates a spark dataframe from the rows and writes it to the Delta table
    '''
    if not rows:
        return

    df = spark_session.createDataFrame(
        rows,
        """
        game_id BIGINT,
        pgn STRING
        """)
    (
        df.write
        .format('delta')
        .mode('append')
        .saveAsTable(table_name)
    )

total_games = 0
with open(FILE, 'r', encoding="utf-8") as file:
    record_streams = stream_pgn_records(file)
    for batch in batch_generator(record_streams, BATCH_SIZE):
        write_batch(spark, batch, TABLE)
        total_games += len(batch)

print(f"Total Games Loaded: {total_games}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Content

# COMMAND ----------

from src.check_data_quality import validate_bronze_table

df_bronze = spark.table(TABLE)
validation_results = validate_bronze_table(df_bronze)

print(
    f"Bronze Quality Gate passed successfully for {validation_results['total_rows']:,} games."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete File

# COMMAND ----------

# MAGIC %sh rm /tmp/lichess_db_standard_rated_2013-01.pgn.zst /tmp/lichess_db_standard_rated_2013-01.pgn