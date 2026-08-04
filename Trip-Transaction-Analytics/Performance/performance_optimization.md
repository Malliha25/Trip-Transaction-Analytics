# Performance Optimization

## 1. Azure Data Factory

### Parallelism
```json
"typeProperties": {
  "parallelCopies": 4,
  "dataIntegrationUnits": 8
}
```
`PL_Raw_Data_Ingestion`'s Copy activity uses 4 parallel copy threads and 8 DIUs; the
`ForEach` in `PL_Incremental_Load` runs with `"isSequential": false, "batchCount": 5`
so up to 5 source tables extract concurrently instead of serially — cutting daily
Bronze load time roughly 4-5x versus a sequential loop.

### Partitioning (source-side read partitioning)
For large tables, enable physical partitioning on the Copy source so ADF splits the
read across parallel connections:
```json
"source": {
  "type": "SqlServerSource",
  "partitionOption": "DynamicRange",
  "partitionSettings": {
    "partitionColumnName": "TripID",
    "partitionUpperBound": "500000",
    "partitionLowerBound": "1"
  }
}
```

### Staged Copy
`PL_Raw_Data_Ingestion` enables `"enableStaging": true` — data lands in a Blob staging
area before the final write. This is required for Self-Hosted IR -> ADLS Gen2 copies at
scale (PolyBase-style bulk load path) and improves throughput versus a direct
row-by-row write.

## 2. Databricks

### Auto Optimize
```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```
Enabled on `trips_silver` and all Gold tables so small files produced by frequent daily
writes are automatically compacted in the background, avoiding the "small file problem"
that kills read performance over time.

### Z-Ordering
```sql
OPTIMIZE trip_analytics.silver.trips_silver ZORDER BY (DriverID, trip_date);
```
Co-locates related data physically, which dramatically speeds up predicate pushdown
for the platform's most common filter patterns (per-driver reports, per-day
aggregations feeding the Gold marts).

### Caching
```python
completed_trips.cache()   # reused across all three Gold mart builds in 02_silver_to_gold
```
`completed_trips` is read once from `trips_silver` and reused to build Driver
Performance, Customer Behavior, and Peak Hour marts — caching it avoids three separate
full scans of Silver.

### Broadcast Joins
```python
from pyspark.sql.functions import broadcast
zone_volume = (
    completed_trips.join(broadcast(locations_silver), "PickupLocationID")
)
```
`locations_silver` (tens of rows) is small enough to broadcast to every executor,
turning an expensive shuffle join against the multi-million-row `trips_silver` into a
map-side join.

## 3. Delta Tables

### Vacuum
```sql
VACUUM trip_analytics.silver.trips_silver RETAIN 168 HOURS;  -- 7-day default retention
```
Physically removes data files no longer referenced by any retained table version,
controlling storage cost. Retention is kept at the 7-day default (not lowered) to
preserve enough time-travel window for recovery and debugging.

### Optimize / File Compaction
```sql
OPTIMIZE trip_analytics.gold.gold_driver_performance;
```
Run nightly after the Silver-to-Gold notebook completes (chained as a final step in
`PL_Master_Orchestration` or a separate maintenance job) to compact the day's small
Parquet part-files written during `mode("overwrite")` / `MERGE` operations into fewer,
larger files — improving downstream Power BI DirectQuery and ad-hoc SQL performance.

### Predicate pushdown via partitioning
`trips_silver` is partitioned by `trip_date`; Gold queries and BI DirectQuery filters
that include a date range prune partitions automatically rather than scanning the
full table history.
