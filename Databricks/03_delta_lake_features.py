# Databricks notebook source
# =============================================================================
# 03_delta_lake_features.py
# Trip Transaction Analytics Platform
# Purpose: Demonstrate Delta Lake advanced capabilities against trips_silver:
#          Time Travel, Version Querying, Restore, Shallow/Deep Clone,
#          Schema Evolution.
# =============================================================================

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F

SILVER_BASE = "abfss://silver@sttripanalyticsprod.dfs.core.windows.net/trip-analytics"
TABLE_PATH = f"{SILVER_BASE}/trips_silver"

dt = DeltaTable.forPath(spark, TABLE_PATH)

# COMMAND ----------
# -----------------------------------------------------------------------
# 1. TIME TRAVEL — inspect full version/operation history
# -----------------------------------------------------------------------
# SQL form:
#   DESCRIBE HISTORY trip_analytics.silver.trips_silver;
history_df = dt.history()
display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------
# -----------------------------------------------------------------------
# 2. VERSION QUERYING — read a prior snapshot two ways
# -----------------------------------------------------------------------
# (a) DataFrame API - by version number
df_v3 = spark.read.format("delta").option("versionAsOf", 3).load(TABLE_PATH)

# (b) DataFrame API - by timestamp (time-travel by wall clock)
df_asof = spark.read.format("delta").option("timestampAsOf", "2026-08-01").load(TABLE_PATH)

# (c) Pure SQL equivalent (run in a %sql cell or via spark.sql):
spark.sql("""
    SELECT * FROM trip_analytics.silver.trips_silver VERSION AS OF 3
""")
spark.sql("""
    SELECT * FROM trip_analytics.silver.trips_silver TIMESTAMP AS OF '2026-08-01'
""")

print("Row count at version 3:", df_v3.count())

# COMMAND ----------
# -----------------------------------------------------------------------
# 3. RESTORE — roll the live table back to a previous version
#    (used for recovering from a bad Silver load; always audited)
# -----------------------------------------------------------------------
# spark.sql("RESTORE TABLE trip_analytics.silver.trips_silver TO VERSION AS OF 3")
# or via path:
# spark.sql(f"RESTORE TABLE delta.`{TABLE_PATH}` TO VERSION AS OF 3")
print("RESTORE command staged (commented out to avoid accidental execution in this notebook).")

# COMMAND ----------
# -----------------------------------------------------------------------
# 4. SHALLOW CLONE — instant, metadata-only copy for dev/test/UAT
#    Points at the SAME underlying data files (no data duplication).
#    Ideal for: giving analysts a sandbox without doubling storage cost.
# -----------------------------------------------------------------------
spark.sql(f"""
    CREATE OR REPLACE TABLE trip_analytics.dev.trips_silver_shallow_clone
    SHALLOW CLONE trip_analytics.silver.trips_silver
""")

# COMMAND ----------
# -----------------------------------------------------------------------
# 5. DEEP CLONE — full physical copy of data + metadata
#    Ideal for: DR replicas, cross-region backups, point-in-time archives
#    before a risky schema migration.
# -----------------------------------------------------------------------
spark.sql(f"""
    CREATE OR REPLACE TABLE trip_analytics.archive.trips_silver_deep_clone_20260804
    DEEP CLONE trip_analytics.silver.trips_silver
""")

# COMMAND ----------
# -----------------------------------------------------------------------
# 6. SCHEMA EVOLUTION — add a new column without breaking existing writers
# -----------------------------------------------------------------------
trips_df = spark.read.format("delta").load(TABLE_PATH)

# Simulate a new business requirement: track a surge-pricing multiplier
trips_with_new_col = trips_df.withColumn(
    "surge_multiplier",
    F.when(F.col("pickup_hour").isin(7, 8, 17, 18), F.lit(1.5)).otherwise(F.lit(1.0))
)

(trips_with_new_col.write
    .format("delta")
    .mode("append")               # append with mergeSchema also works for full overwrites
    .option("mergeSchema", "true")  # <-- this is what enables schema evolution
    .save(TABLE_PATH))

# Alternative: set at session/table level so every writer in the job inherits it
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------
# -----------------------------------------------------------------------
# 7. VACUUM & OPTIMIZE (housekeeping — covered in depth in Performance doc)
# -----------------------------------------------------------------------
# Remove files no longer referenced by any retained version (default 7-day retention)
spark.sql("VACUUM trip_analytics.silver.trips_silver RETAIN 168 HOURS")

# Compact small files + Z-order for faster point/range lookups
spark.sql("OPTIMIZE trip_analytics.silver.trips_silver ZORDER BY (DriverID, trip_date)")

print("Delta Lake feature demonstration complete.")
