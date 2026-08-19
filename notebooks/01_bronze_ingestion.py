# Databricks notebook source
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

FILE = '/tmp/lichess_db_standard_rated_2013-01.pgn'
TABLE = 'lichess.bronze.lichess_db_standard_rated_2013_01'
BATCH_SIZE = 10000


def write_batch(rows):
    '''
    This function creates a spark dataframe from the rows and writes it to the Delta table
    '''
    if not rows:
        return

    df = spark.createDataFrame(
        rows,
        """
        game_id BIGINT,
        pgn STRING
        """)
    (
        df.write
        .format('delta')
        .mode('append')
        .saveAsTable(TABLE)
    )

batch = []

with open(FILE, 'r') as file:

    current_game_rows = []

    game_id = 0
    for line in file:

        # A new PGN game starts with '[Event ...'
        if line.startswith('[Event') and current_game_rows:
            game_id += 1
            current_game_joined = ''.join(current_game_rows).strip()
            
            batch.append({
                'game_id': game_id,
                'pgn': current_game_joined
            })
            
            if len(batch) >= BATCH_SIZE:
                write_batch(batch)
                batch = []

            current_game_rows = []

        current_game_rows.append(line)

    # Write Last Game
    if current_game_rows:
        game_id += 1
        current_game_joined = "".join(current_game_rows).strip()
        
        batch.append({
            "game_id": game_id,
            "pgn": current_game_joined
        })
    
    write_batch(batch)

    print(f"Total Games Loaded: {game_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Content

# COMMAND ----------

df = spark.table(TABLE)

# For Future: ReWrite in Pure Spark
df.selectExpr(
    "COUNT(*) AS total_rows",
    "COUNT(DISTINCT game_id) AS distinct_game_ids",
    "MIN(game_id) AS min_game_id",
    "MAX(game_id) AS max_game_id",
    "SUM(CASE WHEN game_id IS NULL THEN 1 ELSE 0 END) AS null_game_ids",
    "SUM(CASE WHEN pgn IS NULL OR TRIM(pgn) = '' THEN 1 ELSE 0 END) AS empty_pgns",
    "SUM(CASE WHEN NOT pgn LIKE '[Event%' THEN 1 ELSE 0 END) AS invalid_pgn_starts"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete File

# COMMAND ----------

# MAGIC %sh rm /tmp/lichess_db_standard_rated_2013-01.pgn.zst /tmp/lichess_db_standard_rated_2013-01.pgn