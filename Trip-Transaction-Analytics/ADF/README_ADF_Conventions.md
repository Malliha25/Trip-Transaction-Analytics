# ADF Naming, File Layout & Dynamic Content Conventions

## Object naming
| Object | Convention | Example |
|---|---|---|
| Linked Service | `LS_<System>` | `LS_SqlServer_OnPrem`, `LS_Databricks` |
| Dataset | `DS_<Layer>_<Format>` | `DS_Bronze_Parquet`, `DS_Silver_Delta` |
| Pipeline | `PL_<Purpose>` | `PL_Raw_Data_Ingestion`, `PL_Master_Orchestration` |
| Data Flow | `DF_<Purpose>` | `DF_Validate_Bronze_Ingestion` |
| Trigger | `TR_<Frequency>_<Time>` | `TR_Daily_0100_UTC` |
| Integration Runtime | `SelfHostedIR-<Location>` | `SelfHostedIR-OnPrem` |

## Bronze file/folder naming strategy (ADLS Gen2)
```
bronze/trip-analytics/<TableName>/loaddate=YYYY-MM-DD/part-*.parquet
bronze/trip-analytics/Trips/loaddate=2026-08-04/part-00000.snappy.parquet
```
- Partitioned by **load date** (Hive-style `loaddate=`) so Databricks Auto Loader / batch reads can prune partitions and support reprocessing a single day without touching the rest of history.
- Filenames use ADF's default `part-*` naming from the Copy activity; a GUID suffix avoids collisions on concurrent runs.

## Silver / Gold layout (Delta Lake, managed by Databricks not ADF)
```
silver/trip-analytics/trips_silver/            (Delta table, partitioned by trip_date)
gold/trip-analytics/gold_driver_performance/    (Delta table)
gold/trip-analytics/gold_customer_behavior/
gold/trip-analytics/gold_peak_hour_analytics/
```

## Key dynamic content expressions used across pipelines
```
@formatDateTime(utcnow(),'yyyy-MM-dd')                        -- load date partition
@pipeline().RunId                                              -- correlation id for audit rows
@pipeline().TriggerTime                                        -- pipeline start time
@pipeline().TriggerType                                        -- Scheduled / Manual
@activity('Lookup Watermark').output.firstRow.LastWatermarkValue
@activity('Lookup Source Row Count').output.firstRow.RowCount
@item().SourceTable                                            -- inside ForEach
@less(int(activity('DQ Gate').output.runOutput.overall_quality_score), 95)
@concat('LOC-', formatNumber(item().n, '0000'))
```

## Parameterization pattern
Every table-specific pipeline (`PL_Raw_Data_Ingestion`) is fully parameterized on
`SourceSchemaName` / `SourceTableName` so **one pipeline services five source tables**
instead of five near-duplicate pipelines — driven entirely by the `ctl.WatermarkControl`
metadata table via the `ForEach` loop in `PL_Incremental_Load`. Adding a 6th source table
is a metadata insert, not a pipeline change.
