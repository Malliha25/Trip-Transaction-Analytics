# Databricks notebook source
# =============================================================================
# 02_silver_to_gold.py
# Trip Transaction Analytics Platform
# Purpose: Build the three Gold analytical marts:
#          Driver Performance, Customer Behavior, Peak Hour Analytics
# =============================================================================

# COMMAND ----------
dbutils.widgets.text("run_id", "manual-run")
RUN_ID = dbutils.widgets.get("run_id")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_BASE = "abfss://silver@sttripanalyticsprod.dfs.core.windows.net/trip-analytics"
GOLD_BASE = "abfss://gold@sttripanalyticsprod.dfs.core.windows.net/trip-analytics"

trips = spark.read.format("delta").load(f"{SILVER_BASE}/trips_silver")
drivers = spark.read.format("delta").load(f"{SILVER_BASE}/drivers_silver")
customers = spark.read.format("delta").load(f"{SILVER_BASE}/customers_silver")
payments = spark.read.format("delta").load(f"{SILVER_BASE}/payments_silver")

completed_trips = trips.filter(F.col("TripStatus") == "COMPLETED")

# COMMAND ----------
# =============================================================================
# GOLD MART 1: Driver Performance
# Metrics: Total Trips, Total Revenue, Average Rating, Revenue Per Day
# =============================================================================
driver_daily = (
    completed_trips
    .groupBy("DriverID", "trip_date")
    .agg(
        F.count("TripID").alias("daily_trips"),
        F.sum("TotalAmount").alias("daily_revenue")
    )
)

driver_perf_base = (
    completed_trips
    .groupBy("DriverID")
    .agg(
        F.count("TripID").alias("total_trips"),
        F.sum("TotalAmount").alias("total_revenue"),
        F.avg("DriverRating").alias("average_rating"),
        F.avg("TripDistanceMiles").alias("avg_trip_distance"),
        F.countDistinct("trip_date").alias("active_days"),
        F.min("trip_date").alias("first_trip_date"),
        F.max("trip_date").alias("last_trip_date")
    )
    .withColumn("revenue_per_day", F.round(F.col("total_revenue") / F.col("active_days"), 2))
)

# Window function: rank drivers by revenue within their home city
gold_driver_performance = (
    driver_perf_base
    .join(drivers.select("DriverID", "DriverCode", "FirstName", "LastName", "Status", "HomeLocationID"), "DriverID")
    .withColumn(
        "revenue_rank_overall",
        F.rank().over(Window.orderBy(F.col("total_revenue").desc()))
    )
    .withColumn(
        "revenue_rank_within_status",
        F.rank().over(Window.partitionBy("Status").orderBy(F.col("total_revenue").desc()))
    )
    .withColumn("_gold_processed_ts", F.current_timestamp())
    .withColumn("_ingestion_run_id", F.lit(RUN_ID))
)

(gold_driver_performance.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{GOLD_BASE}/gold_driver_performance"))

# COMMAND ----------
# =============================================================================
# GOLD MART 2: Customer Behavior
# Metrics: Ride Frequency, Customer Lifetime Value (CLV), Avg Trip Distance,
#          Preferred Payment Method (actual usage, not stated preference)
# =============================================================================
customer_payment_usage = (
    completed_trips.alias("t")
    .join(payments.alias("p"), F.col("t.TripID") == F.col("p.TripID"))
    .groupBy("t.CustomerID", "p.PaymentMethod")
    .agg(F.count("*").alias("usage_count"))
)

w_pref = Window.partitionBy("CustomerID").orderBy(F.col("usage_count").desc())
top_payment_method = (
    customer_payment_usage
    .withColumn("rn", F.row_number().over(w_pref))
    .filter(F.col("rn") == 1)
    .select("CustomerID", F.col("PaymentMethod").alias("actual_preferred_payment_method"))
)

customer_base = (
    completed_trips
    .groupBy("CustomerID")
    .agg(
        F.count("TripID").alias("total_rides"),
        F.sum("TotalAmount").alias("lifetime_value"),
        F.avg("TripDistanceMiles").alias("avg_trip_distance"),
        F.min("trip_date").alias("first_ride_date"),
        F.max("trip_date").alias("last_ride_date"),
        F.countDistinct("trip_date").alias("active_days")
    )
    .withColumn("ride_frequency_per_month",
        F.round(F.col("total_rides") / (F.greatest(F.datediff(F.col("last_ride_date"), F.col("first_ride_date")), F.lit(1)) / 30.0), 2)
    )
)

gold_customer_behavior = (
    customer_base
    .join(customers.select("CustomerID", "CustomerCode", "FirstName", "LastName", "LoyaltyTier"), "CustomerID")
    .join(top_payment_method, "CustomerID", "left")
    .withColumn("clv_segment",
        F.when(F.col("lifetime_value") >= 1000, "High Value")
         .when(F.col("lifetime_value") >= 300, "Mid Value")
         .otherwise("Low Value"))
    .withColumn("_gold_processed_ts", F.current_timestamp())
    .withColumn("_ingestion_run_id", F.lit(RUN_ID))
)

(gold_customer_behavior.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{GOLD_BASE}/gold_customer_behavior"))

# COMMAND ----------
# =============================================================================
# GOLD MART 3: Peak Hour Analytics
# Metrics: Peak Hours, Peak Zones, Revenue by Hour, Driver Utilization
# =============================================================================
hourly_revenue = (
    completed_trips
    .groupBy("pickup_hour")
    .agg(
        F.count("TripID").alias("trip_count"),
        F.sum("TotalAmount").alias("revenue"),
        F.countDistinct("DriverID").alias("distinct_drivers_active")
    )
)

total_active_drivers = drivers.filter(F.col("Status") == "ACTIVE").count()

hourly_with_utilization = (
    hourly_revenue
    .withColumn("driver_utilization_pct",
        F.round(100.0 * F.col("distinct_drivers_active") / F.lit(total_active_drivers), 2))
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.col("revenue").desc())))
    .withColumn("is_peak_hour", F.col("revenue_rank") <= 3)  # top-3 revenue hours flagged as peak
)

zone_volume = (
    completed_trips.alias("t")
    .join(spark.read.format("delta").load(f"{SILVER_BASE}/locations_silver").alias("l"),
          F.col("t.PickupLocationID") == F.col("l.LocationID"))
    .groupBy("l.ZoneName", "l.City")
    .agg(
        F.count("t.TripID").alias("pickup_count"),
        F.sum("t.TotalAmount").alias("zone_revenue")
    )
    .withColumn("zone_rank", F.rank().over(Window.orderBy(F.col("pickup_count").desc())))
    .withColumn("is_peak_zone", F.col("zone_rank") <= 5)
)

gold_peak_hour_analytics = hourly_with_utilization.withColumn("_ingestion_run_id", F.lit(RUN_ID))
gold_peak_zone_analytics = zone_volume.withColumn("_ingestion_run_id", F.lit(RUN_ID))

(gold_peak_hour_analytics.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{GOLD_BASE}/gold_peak_hour_analytics"))

(gold_peak_zone_analytics.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{GOLD_BASE}/gold_peak_zone_analytics"))

# COMMAND ----------
for tbl in ["gold_driver_performance", "gold_customer_behavior", "gold_peak_hour_analytics", "gold_peak_zone_analytics"]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS trip_analytics.gold.{tbl}
        USING DELTA
        LOCATION '{GOLD_BASE}/{tbl}'
    """)

print("Silver -> Gold marts built for run_id:", RUN_ID)
dbutils.notebook.exit("SUCCESS")
