# Databricks notebook source
# =============================================================================
# 01_bronze_to_silver.py
# Trip Transaction Analytics Platform
# Purpose: Read raw Bronze parquet, cleanse, standardize, deduplicate,
#          enrich with audit columns, and write to Silver Delta tables.
# =============================================================================

# COMMAND ----------
dbutils.widgets.text("run_id", "manual-run")
dbutils.widgets.text("load_date", "")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

RUN_ID = dbutils.widgets.get("run_id")
LOAD_DATE = dbutils.widgets.get("load_date") or datetime.utcnow().strftime("%Y-%m-%d")

BRONZE_BASE = "abfss://bronze@sttripanalyticsprod.dfs.core.windows.net/trip-analytics"
SILVER_BASE = "abfss://silver@sttripanalyticsprod.dfs.core.windows.net/trip-analytics"

print(f"Run ID: {RUN_ID} | Load Date: {LOAD_DATE}")

# COMMAND ----------
# -----------------------------------------------------------------------
# Helper: standard audit columns applied to every Silver table
# -----------------------------------------------------------------------
def add_audit_columns(df, source_table):
    return (
        df.withColumn("_source_system", F.lit("TripOpsDB"))
          .withColumn("_source_table", F.lit(source_table))
          .withColumn("_ingestion_run_id", F.lit(RUN_ID))
          .withColumn("_load_date", F.lit(LOAD_DATE))
          .withColumn("_silver_processed_ts", F.current_timestamp())
          .withColumn("_is_current", F.lit(True))
    )

def dedup_on_natural_key(df, key_cols, order_col="ModifiedDate"):
    """Keep the most recently modified record per natural key."""
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

def read_bronze(table_name):
    return spark.read.parquet(f"{BRONZE_BASE}/{table_name}/loaddate={LOAD_DATE}")

# COMMAND ----------
# -----------------------------------------------------------------------
# LOCATIONS -> locations_silver
# -----------------------------------------------------------------------
locations_raw = read_bronze("Locations")

locations_silver = (
    locations_raw
    .dropDuplicates(["LocationID"])
    .transform(lambda d: dedup_on_natural_key(d, ["LocationCode"]))
    .withColumn("ZoneName", F.trim(F.initcap(F.col("ZoneName"))))
    .withColumn("City", F.trim(F.initcap(F.col("City"))))
    .withColumn("State", F.upper(F.trim(F.col("State"))))
    .filter(F.col("Latitude").isNotNull() & F.col("Longitude").isNotNull())
    .transform(lambda d: add_audit_columns(d, "Locations"))
)

(locations_silver.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{SILVER_BASE}/locations_silver"))

# COMMAND ----------
# -----------------------------------------------------------------------
# DRIVERS -> drivers_silver
# -----------------------------------------------------------------------
drivers_raw = read_bronze("Drivers")

drivers_silver = (
    drivers_raw
    .transform(lambda d: dedup_on_natural_key(d, ["DriverCode"]))
    .withColumn("FirstName", F.initcap(F.trim(F.col("FirstName"))))
    .withColumn("LastName", F.initcap(F.trim(F.col("LastName"))))
    .withColumn("Email", F.lower(F.trim(F.col("Email"))))
    .withColumn("Status", F.upper(F.trim(F.col("Status"))))
    # Null handling: unknown vehicle info defaulted rather than dropped
    .withColumn("VehicleMake", F.coalesce(F.col("VehicleMake"), F.lit("UNKNOWN")))
    .withColumn("VehicleModel", F.coalesce(F.col("VehicleModel"), F.lit("UNKNOWN")))
    # Business rule: a driver must have a valid license format (basic check)
    .filter(F.col("LicenseNumber").rlike("^LIC-[0-9]{6}$"))
    .transform(lambda d: add_audit_columns(d, "Drivers"))
)

(drivers_silver.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("Status")
    .save(f"{SILVER_BASE}/drivers_silver"))

# COMMAND ----------
# -----------------------------------------------------------------------
# CUSTOMERS -> customers_silver
# -----------------------------------------------------------------------
customers_raw = read_bronze("Customers")

customers_silver = (
    customers_raw
    .transform(lambda d: dedup_on_natural_key(d, ["CustomerCode"]))
    .withColumn("FirstName", F.initcap(F.trim(F.col("FirstName"))))
    .withColumn("LastName", F.initcap(F.trim(F.col("LastName"))))
    .withColumn("Email", F.lower(F.trim(F.col("Email"))))
    .withColumn("PreferredPaymentMethod", F.coalesce(F.col("PreferredPaymentMethod"), F.lit("Unspecified")))
    .withColumn("LoyaltyTier", F.coalesce(F.col("LoyaltyTier"), F.lit("Standard")))
    .transform(lambda d: add_audit_columns(d, "Customers"))
)

(customers_silver.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{SILVER_BASE}/customers_silver"))

# COMMAND ----------
# -----------------------------------------------------------------------
# TRIPS -> trips_silver  (core fact table; heaviest cleansing rules)
# -----------------------------------------------------------------------
trips_raw = read_bronze("Trips")

trips_silver = (
    trips_raw
    .transform(lambda d: dedup_on_natural_key(d, ["TripCode"]))
    # Null handling: drop records missing hard-required keys
    .filter(F.col("DriverID").isNotNull() & F.col("CustomerID").isNotNull())
    # Business rule: dropoff must be >= pickup when present
    .withColumn(
        "DropoffDateTime",
        F.when(F.col("DropoffDateTime") < F.col("PickupDateTime"), None)
         .otherwise(F.col("DropoffDateTime"))
    )
    # Business rule: negative / absurd fares are capped to null for downstream review, not silently kept
    .withColumn("FareAmount", F.when(F.col("FareAmount") < 0, None).otherwise(F.col("FareAmount")))
    .withColumn("TripDistanceMiles", F.when(F.col("TripDistanceMiles") < 0, F.lit(0)).otherwise(F.col("TripDistanceMiles")))
    # Standardization
    .withColumn("TripStatus", F.upper(F.trim(F.col("TripStatus"))))
    .withColumn("trip_date", F.to_date(F.col("PickupDateTime")))
    .withColumn("pickup_hour", F.hour(F.col("PickupDateTime")))
    # Recompute duration defensively instead of trusting source column
    .withColumn(
        "TripDurationMinCalc",
        F.when(F.col("DropoffDateTime").isNotNull(),
               (F.unix_timestamp("DropoffDateTime") - F.unix_timestamp("PickupDateTime")) / 60
        ).otherwise(F.col("TripDurationMin"))
    )
    .transform(lambda d: add_audit_columns(d, "Trips"))
)

(trips_silver.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .partitionBy("trip_date")
    .save(f"{SILVER_BASE}/trips_silver"))

# COMMAND ----------
# -----------------------------------------------------------------------
# PAYMENTS -> payments_silver
# -----------------------------------------------------------------------
payments_raw = read_bronze("Payments")

payments_silver = (
    payments_raw
    .transform(lambda d: dedup_on_natural_key(d, ["PaymentCode"]))
    .withColumn("PaymentMethod", F.upper(F.trim(F.col("PaymentMethod"))))
    .withColumn("PaymentStatus", F.upper(F.trim(F.col("PaymentStatus"))))
    .filter(F.col("Amount") >= 0)
    .transform(lambda d: add_audit_columns(d, "Payments"))
)

(payments_silver.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{SILVER_BASE}/payments_silver"))

# COMMAND ----------
# -----------------------------------------------------------------------
# Register Delta tables in Unity Catalog / metastore for SQL access
# -----------------------------------------------------------------------
for tbl in ["locations_silver", "drivers_silver", "customers_silver", "trips_silver", "payments_silver"]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS trip_analytics.silver.{tbl}
        USING DELTA
        LOCATION '{SILVER_BASE}/{tbl}'
    """)

print("Bronze -> Silver load complete for run_id:", RUN_ID)

# COMMAND ----------
# -----------------------------------------------------------------------
# Notebook audit write-back (called for every table; abbreviated to one
# summary row here — production version writes one row per table)
# -----------------------------------------------------------------------
audit_row = [(
    "01_bronze_to_silver", RUN_ID, dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().get(),
    "Bronze", "Silver", "ALL_SILVER_TABLES",
    datetime.utcnow(), datetime.utcnow(), "Succeeded",
    trips_silver.count(), None, None
)]
audit_cols = ["NotebookName","JobRunID","ClusterID","LayerFrom","LayerTo","TargetTable",
              "StartTime","EndTime","Status","RecordsProcessed","RecordsInserted","RecordsUpdated"]
audit_df = spark.createDataFrame(audit_row, audit_cols)

(audit_df.write
    .format("jdbc")
    .option("url", "jdbc:sqlserver://control-sql-host:1433;database=TripOpsControlDB")
    .option("dbtable", "ctl.NotebookAudit")
    .option("user", dbutils.secrets.get("kv-tripanalytics", "sql-control-user"))
    .option("password", dbutils.secrets.get("kv-tripanalytics", "sql-control-password"))
    .mode("append")
    .save())

dbutils.notebook.exit("SUCCESS")
