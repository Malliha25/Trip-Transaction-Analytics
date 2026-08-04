# Azure Data Engineering — 50 Interview Questions & Answers

## Azure Data Factory (1–10)

**1. What is Azure Data Factory and how does it differ from an ETL tool like SSIS?**
ADF is a cloud-native, serverless data integration and orchestration service. Unlike
SSIS, which runs on a fixed on-prem/VM engine, ADF elastically scales compute per
activity (via Integration Runtimes), supports code-free Mapping Data Flows built on
Spark, and orchestrates hybrid pipelines spanning on-prem and cloud without managing
infrastructure.

**2. What are the three types of Integration Runtime and when do you use each?**
Azure IR (cloud-to-cloud movement/transformation), Self-Hosted IR (connects to
on-prem or VNet-isolated sources — used here for `TripOpsDB`), and Azure-SSIS IR
(lift-and-shift execution of existing SSIS packages).

**3. Difference between a Pipeline, Activity, and Dataset?**
A Pipeline is a logical grouping of Activities (the executable units: Copy, Lookup,
ExecuteDataFlow, etc.). A Dataset is a named view of data pointing at a Linked
Service, describing the schema/location an Activity reads from or writes to.

**4. How do you implement incremental loading in ADF?**
Track a high-watermark column (e.g. `ModifiedDate`) in a control table, `Lookup` the
last watermark, filter the source query by it, copy the delta, then update the
watermark on success — exactly the pattern in `PL_Raw_Data_Ingestion` /
`ctl.WatermarkControl`.

**5. What's the difference between a Tumbling Window trigger and a Schedule trigger?**
A Schedule trigger simply fires a pipeline on a wall-clock cadence. A Tumbling Window
trigger owns a self-managed, retriable time-sliced execution model with built-in
dependency-on-prior-window support, ideal for backfills and strict sequential
processing.

**6. How do you handle pipeline failure and retries?**
Activity-level `policy.retry` / `retryIntervalInSeconds` for transient faults; a
`Fail` activity for explicit business-rule violations (e.g. row-count mismatch); and
dependency conditions of `Failed`/`Completed` to route to cleanup/alerting activities,
as in `PL_Master_Orchestration`'s failure path.

**7. What is a Mapping Data Flow and how does it execute?**
A visually-designed, code-free transformation defined in ADF but compiled and executed
as a Spark job on a Data Flow-managed cluster — used here (`DF_Validate_Bronze_Ingestion`)
for row-count/schema validation directly after Bronze landing.

**8. How do you parameterize a pipeline to service many source tables with one
definition?**
Define pipeline parameters (e.g. `SourceTableName`), reference them in dynamic content
expressions inside dataset/activity properties, and drive the parameter values from a
metadata table via a `ForEach` loop — the exact design of
`PL_Incremental_Load` -> `PL_Raw_Data_Ingestion`.

**9. What's the difference between Copy Activity's `parallelCopies` and Data
Integration Units (DIUs)?**
`parallelCopies` controls the number of concurrent read/write threads; DIUs represent
the total compute/network power (CPU, memory, network) ADF allocates to the copy —
increasing both, within source/sink limits, increases throughput.

**10. How would you monitor and alert on ADF pipeline failures in production?**
Combine ADF's native Monitor tab / Azure Monitor metrics & alerts, custom audit logging
to a control database (`ctl.PipelineAudit`), and a `WebActivity` failure path calling a
Logic App to push email/Teams notifications — the three-layer approach used in this
platform.

## Databricks & PySpark (11–20)

**11. Why use Databricks alongside ADF instead of doing everything in ADF Data Flows?**
Databricks gives full Spark/PySpark expressiveness (custom Python libraries, MLflow,
Delta Lake native features, unit-testable code in Repos) versus Data Flows' visual,
more constrained transformation surface — ADF is used here for orchestration/ingestion,
Databricks for the heavy Bronze->Silver->Gold transformation logic.

**12. What is the difference between `repartition()` and `coalesce()`?**
`repartition()` triggers a full shuffle and can increase or decrease partitions;
`coalesce()` avoids a shuffle by only merging existing partitions, so it can only
decrease partition count efficiently.

**13. Explain a broadcast join and when it helps.**
When one side of a join is small enough to fit in executor memory, Spark can send
(broadcast) a full copy to every executor and perform the join without a shuffle —
used in this platform for `trips_silver` join `locations_silver`.

**14. What's the difference between `cache()` and `persist()`?**
`cache()` is shorthand for `persist(StorageLevel.MEMORY_AND_DISK)`. `persist()` lets you
choose the storage level explicitly (memory-only, disk-only, serialized, replicated),
useful when memory is constrained.

**15. How do window functions work in PySpark and where are they used in this
project?**
`Window.partitionBy(...).orderBy(...)` defines an ordered group over which functions
like `row_number()`, `rank()`, `lag()` operate without collapsing rows — used for
deduplication (`row_number` picking the latest record per natural key) and for ranking
drivers by revenue in the Gold layer.

**16. What is the difference between a wide and a narrow transformation?**
Narrow transformations (`filter`, `map`) don't require data movement across partitions.
Wide transformations (`groupBy`, `join`, `distinct`) require a shuffle, which is the
primary performance cost in most Spark jobs.

**17. How do you handle schema drift in a PySpark ingestion pipeline?**
Read with a permissive/flexible schema (or `inferSchema` cautiously), then explicitly
validate required columns exist, and use Delta's `mergeSchema` option to safely absorb
new optional columns rather than failing the whole job.

**18. What's the purpose of `dbutils.widgets` in a notebook used by ADF?**
It exposes notebook parameters (e.g. `run_id`, `load_date`) that ADF's
`DatabricksNotebook` activity injects via `baseParameters`, letting one notebook serve
many pipeline runs without hardcoded values.

**19. How would you unit test PySpark transformation logic?**
Extract transformation logic into pure functions taking/returning DataFrames (as done
with `add_audit_columns`, `dedup_on_natural_key`), then test them against small local
SparkSession fixtures with `pytest`, asserting on collected rows rather than testing
against production data.

**20. What's the difference between a Databricks Job cluster and an all-purpose
(interactive) cluster?**
Job clusters are ephemeral — created for a specific job run and terminated
immediately after, which is cheaper and used for scheduled production notebooks. All-
purpose clusters stay up for interactive development/exploration and cost more if left
idle.

## Delta Lake (21–28)

**21. What problem does Delta Lake solve over plain Parquet on a data lake?**
ACID transactions, schema enforcement/evolution, time travel, and scalable metadata
handling — plain Parquet has none of these, leading to partial writes, no rollback, and
painful schema management at scale.

**22. How does Delta Lake achieve ACID guarantees?**
Through the `_delta_log` transaction log: every write is an atomic, ordered commit
recording exactly which files were added/removed, enabling snapshot isolation for
readers and optimistic concurrency control for writers.

**23. Explain time travel and a real use case.**
Querying `VERSION AS OF` / `TIMESTAMP AS OF` a prior state of a Delta table without
separate backups — used here to audit a Silver load or recover from a bad daily run.

**24. Shallow clone vs deep clone — trade-offs?**
Shallow clone copies metadata only (fast, cheap, but depends on source files staying
un-vacuumed); deep clone copies data physically (slower, storage cost, but fully
independent) — see `DeltaLake/delta_lake_advanced_features.md`.

**25. What does `OPTIMIZE ... ZORDER BY` actually do?**
Compacts small files into larger ones and co-locates rows with similar values in the
Z-order columns within the same files, so predicate pushdown on those columns skips
far more data at read time.

**26. Why is `VACUUM`'s default 7-day retention important not to shorten
carelessly?**
Any Delta table snapshot, clone, or long-running query referencing files older than
the retention window will break once `VACUUM` deletes them — shortening it trades
storage savings for time-travel/recoverability risk.

**27. How do `MERGE INTO` upserts work in Delta Lake?**
`MERGE INTO target USING source ON <condition> WHEN MATCHED THEN UPDATE ... WHEN NOT
MATCHED THEN INSERT ...` — enables idempotent, row-level incremental Silver/Gold loads
instead of full-table overwrites.

**28. What is schema enforcement and how does `mergeSchema` interact with it?**
By default Delta rejects writes with mismatched schemas (protecting against silent
data corruption). `mergeSchema=true` is an explicit, intentional opt-in that allows
new, compatible columns to be added — it does not bypass type-mismatch protection.

## Medallion Architecture & Incremental Loading (29–36)

**29. Explain the Medallion Architecture and why each layer exists.**
Bronze preserves raw, immutable source data for reprocessing/audit. Silver applies
cleansing, standardization, and business rules into a trusted, queryable form. Gold
aggregates Silver into consumption-ready marts optimized for specific business
questions (driver performance, customer behavior, peak analytics).

**30. Why keep Bronze data raw instead of cleansing on ingestion?**
So the entire pipeline can be replayed from source-fidelity data if a downstream bug
or requirement change is discovered — cleansing logic evolves, but you can't
regenerate deleted raw history.

**31. How do you decide what belongs in Silver vs Gold?**
Silver: entity-grain, deduplicated, standardized, still close to source structure.
Gold: business-grain (metrics, aggregates, KPIs) shaped specifically for a consumption
pattern (a BI mart, an ML feature table).

**32. What is a watermark and why not just reload the full table daily?**
A watermark is the highest value of a monotonically increasing column (or a change
sequence number) already processed. Full reloads don't scale with data volume/cost and
lose intraday delta granularity that a watermark-based incremental approach preserves.

**33. Watermark-based incremental loading vs. Change Data Capture (CDC) — trade-offs?**
Watermarking (via `ModifiedDate`) is simple but misses hard deletes and requires a
reliable, always-updated timestamp column. CDC (SQL Server Change Tracking/CT, or
Debezium-style log-based capture) captures inserts, updates, *and* deletes precisely
but adds source-system configuration overhead and complexity.

**34. How do you make an incremental pipeline idempotent (safe to re-run)?**
Use `MERGE INTO` (upsert) semantics keyed on natural/business keys rather than blind
`append`, so re-running a failed/partial load doesn't create duplicates.

**35. What happens if a daily incremental load partially fails halfway through?**
The watermark should only be advanced *after* a successful, validated write (as in
`PL_Raw_Data_Ingestion`'s "Update Watermark" step running after the row-count
validation gate) — so a failed run is safely retried from the same starting point next
time rather than silently skipping data.

**36. How would you backfill 90 days of missing history for one table without
re-running the whole platform?**
Temporarily reset that table's `LastWatermarkValue` in `ctl.WatermarkControl` to the
backfill start date, execute `PL_Raw_Data_Ingestion` for just that table/date range,
then let the watermark continue advancing normally — or better, run a parameterized
Tumbling Window trigger over the historical date range.

## Logic Apps, Monitoring & Ops (37–44)

**37. When would you choose Logic Apps over an Azure Function for alerting?**
Logic Apps' visual, connector-rich, low-code model is ideal for orchestrating
notifications across email/Teams/PagerDuty with minimal custom code — an Azure
Function is better suited when the "alert" requires custom business logic or heavy
compute.

**38. How do you avoid alert fatigue in a production data pipeline?**
Tiered severity (Warning vs Critical), a quality-score *threshold* gate rather than
alerting on every minor DQ rule failure, and routing only Critical alerts to paging
channels while Warnings go to a digest/dashboard.

**39. What should a good pipeline audit table capture at minimum?**
Start/end time, status, run ID (for correlation across systems), records
read/written/rejected, and error message — exactly the shape of
`ctl.PipelineAudit`/`ctl.NotebookAudit` here.

**40. How do you calculate a "data quality score" and use it operationally?**
Percentage of records passing all rules per table/run (`(checked - failed) /
checked * 100`), aggregated and used as a hard gate in orchestration (below threshold
halts/flags downstream Gold processing) rather than just a passive dashboard metric.

**41. What's the difference between monitoring and observability in this context?**
Monitoring answers "is it working?" (pipeline succeeded/failed, DQ score); observability
answers "why did it behave this way?" (drill into lineage, query history, detailed
error messages, and historical trend of a specific metric).

**42. How would you detect a "silent" data quality issue that doesn't fail the
pipeline (e.g. a source system starts sending 90% fewer rows)?**
Add a volume-anomaly check comparing today's row count against a rolling average/
threshold (not just null/duplicate checks), flagged as a DQ rule with Warning/Critical
severity.

**43. Where does row-level rejected/quarantined data go, and how is it resolved?**
To a dedicated `_rejected` Delta table per source (see `dq_framework.py`'s quarantine
write), tagged with the failing rule name and run ID, reviewed and either
corrected-and-replayed or written off by a data steward — never silently dropped.

**44. How do you correlate an issue across ADF, Databricks, and SQL Server logs?**
A shared `RunId`/correlation ID (ADF's `pipeline().RunId`) threaded through every
Databricks notebook parameter and every audit table row, so one ID lets you trace a
single execution end-to-end across all three systems.

## Performance, Security & Architecture (45–50)

**45. How would you diagnose a Spark job that's slow due to data skew?**
Check the Spark UI stage view for a small number of tasks taking far longer than
others; mitigate via salting the skewed key, broadcasting the small side of a join, or
repartitioning explicitly on a more even key.

**46. Why use Managed Identity instead of storage account keys for ADF/Databricks
access to ADLS Gen2?**
Storage keys are long-lived, full-account-access secrets that are easy to leak and
hard to rotate; Managed Identity is automatically rotated, scoped via RBAC to exactly
the needed containers, and requires no secret to store or manage at all.

**47. What is Unity Catalog and what problem did it solve that Databricks lacked
before?**
A unified governance layer across all Databricks workspaces providing one place for
access control, lineage, and auditing at the catalog/schema/table/column level — before
Unity Catalog, permissions were managed per-workspace with the older Hive metastore,
which didn't support fine-grained, cross-workspace governance.

**48. How would you design this platform to support multiple business units with
isolated data but shared infrastructure?**
Separate Unity Catalog catalogs (or schemas) per business unit with RBAC-scoped access,
shared cluster policies/job compute for cost efficiency, and parameterized ADF
pipelines/Databricks jobs driven by a business-unit dimension in the metadata table.

**49. What's your approach to right-sizing Databricks cluster compute for a daily
batch job like this one?**
Start with autoscaling job clusters bounded by observed data volume (min/max workers),
monitor Spark UI for shuffle spill and executor idle time, and iterate — over-
provisioning wastes DBU cost, under-provisioning causes spill-to-disk and slow stages.

**50. How would you evolve this batch-only platform toward near-real-time processing
if the business asked for it?**
Introduce Databricks Structured Streaming with Auto Loader reading new Bronze files
incrementally (replacing the daily ADF Copy trigger with a continuous/near-continuous
one), keep the same Silver/Gold Delta MERGE logic (which is already
append/upsert-friendly), and move the Data Quality gate to a streaming
`foreachBatch` check rather than a single end-of-run gate.
