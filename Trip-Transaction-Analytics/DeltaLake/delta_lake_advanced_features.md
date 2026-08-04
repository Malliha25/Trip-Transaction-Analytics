# Delta Lake Advanced Features — Reference Guide

## 1. Time Travel

```sql
DESCRIBE HISTORY trip_analytics.silver.trips_silver;
```
Every write to a Delta table (INSERT, UPDATE, DELETE, MERGE, OPTIMIZE, VACUUM) creates
a new immutable **version** recorded in the `_delta_log`. Time travel lets you read any
historical version without separate snapshots or backups.

**Use cases:** auditing "what did this table look like at month-end close?", reproducing
a report, debugging a bad pipeline run, recovering from an accidental overwrite.

## 2. Version Querying

```sql
-- By version number
SELECT * FROM trips_silver VERSION AS OF 3;

-- By timestamp
SELECT * FROM trips_silver TIMESTAMP AS OF '2026-08-01 00:00:00';
```
```python
df = spark.read.format("delta").option("versionAsOf", 3).load(path)
df = spark.read.format("delta").option("timestampAsOf", "2026-08-01").load(path)
```

## 3. Restore

```sql
RESTORE TABLE trips_silver TO VERSION AS OF 3;
```
Rewrites the table's current state to match a prior version — itself logged as a new
version (so you can always "undo the undo"). Faster and safer than reprocessing
upstream data after a bad load.

## 4. Clone

### Shallow Clone
```sql
CREATE TABLE trips_clone SHALLOW CLONE trips_silver;
```
Copies only the **transaction log / metadata**, not the underlying Parquet files.
Near-instant, near-zero storage cost. The clone breaks if the source's files are
vacuumed past the clone's referenced version — so pair with `VACUUM` retention policy
awareness. Best for: dev/test sandboxes, quick "what-if" exploration.

### Deep Clone
```sql
CREATE TABLE trips_archive DEEP CLONE trips_silver;
```
Physically copies data files too. Fully independent of the source afterward. Best for:
disaster-recovery replicas, cross-region copies, long-term point-in-time archives before
a risky migration.

## 5. Schema Evolution

```python
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
```
or at session scope:
```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```
Allows new columns introduced by upstream source changes (e.g. a new
`surge_multiplier` field) to be added without a manual `ALTER TABLE` or pipeline outage.
Delta enforces schema on write by default — `mergeSchema` is an explicit opt-in,
which keeps unintentional schema drift (e.g. a renamed/misspelled column) from silently
polluting the table.

## 6. Common interview questions on this topic

**Q: How does Delta Lake implement ACID transactions on top of object storage?**
A: Via the `_delta_log` — a sequential, JSON-based transaction log where each entry
(a `.json` commit file) describes the exact set of files added/removed for that version.
Readers get snapshot isolation by only considering files listed as "active" as of the
version they're reading; writers use optimistic concurrency control with mutual exclusion
on the log entry write.

**Q: Shallow clone vs. deep clone — when would each cause a production incident if used incorrectly?**
A: Using a shallow clone as a long-lived "backup" is the classic mistake — once the
source table's `VACUUM` retention window passes, the shallow clone's referenced files
can be physically deleted, silently corrupting the clone. Deep clone is the safe choice
for anything that must outlive the source's vacuum cycle.

**Q: What's the difference between `MERGE INTO` upserts and simple `mode("overwrite")`?**
A: `MERGE INTO` performs row-level conditional insert/update/delete against a target
table using a join condition — ideal for CDC-style incremental Silver/Gold loads.
`overwrite` mode replaces the entire table (or a specific partition with
`replaceWhere`), which is appropriate for full-refresh Bronze loads but wasteful/
incorrect for large tables that only get partial daily updates.
