# Databricks notebook source
# =============================================================================
# dq_framework.py
# Trip Transaction Analytics Platform — Reusable Data Quality Framework
# Runs configurable rule sets against any Bronze/Silver/Gold table, writes
# failing rows to a quarantine "error table", logs results to
# ctl.DataQualityAudit, and computes an overall quality score used as a
# pipeline gate (see PL_Master_Orchestration -> "If Quality Score Below Threshold").
# =============================================================================

# COMMAND ----------
dbutils.widgets.text("run_id", "manual-run")
dbutils.widgets.text("layer", "Silver")
RUN_ID = dbutils.widgets.get("run_id")
LAYER = dbutils.widgets.get("layer")

from pyspark.sql import functions as F, DataFrame
from datetime import datetime
from dataclasses import dataclass
from typing import Callable, List

QUARANTINE_BASE = "abfss://quarantine@sttripanalyticsprod.dfs.core.windows.net/trip-analytics"

# COMMAND ----------
# -----------------------------------------------------------------------
# Rule definition model
# -----------------------------------------------------------------------
@dataclass
class DQRule:
    name: str
    category: str          # Null / Duplicate / Range / Referential / Format
    severity: str           # Warning / Critical
    predicate: Callable[[DataFrame], DataFrame]  # returns DataFrame of FAILING rows

def null_check(col_name):
    return DQRule(
        name=f"NullCheck_{col_name}",
        category="Null",
        severity="Critical",
        predicate=lambda df: df.filter(F.col(col_name).isNull())
    )

def duplicate_check(key_cols):
    def _pred(df):
        dupe_keys = (df.groupBy(*key_cols).count().filter(F.col("count") > 1).select(*key_cols))
        return df.join(dupe_keys, key_cols, "inner")
    return DQRule(
        name=f"DuplicateCheck_{'_'.join(key_cols)}",
        category="Duplicate",
        severity="Warning",
        predicate=_pred
    )

def range_check(col_name, min_val=None, max_val=None):
    def _pred(df):
        cond = F.lit(False)
        if min_val is not None:
            cond = cond | (F.col(col_name) < min_val)
        if max_val is not None:
            cond = cond | (F.col(col_name) > max_val)
        return df.filter(cond)
    return DQRule(
        name=f"RangeCheck_{col_name}",
        category="Range",
        severity="Warning",
        predicate=_pred
    )

def date_validity_check(col_name):
    return DQRule(
        name=f"DateValidityCheck_{col_name}",
        category="Format",
        severity="Critical",
        predicate=lambda df: df.filter(
            F.col(col_name).isNotNull() &
            ((F.col(col_name) < F.lit("2015-01-01")) | (F.col(col_name) > F.current_timestamp()))
        )
    )

def referential_check(df_child, fk_col, df_parent, pk_col):
    def _pred(df):
        return df.join(df_parent.select(pk_col), df[fk_col] == df_parent[pk_col], "left_anti")
    return DQRule(
        name=f"ReferentialCheck_{fk_col}",
        category="Referential",
        severity="Critical",
        predicate=_pred
    )

# COMMAND ----------
# -----------------------------------------------------------------------
# Engine: run all rules for a table, quarantine failures, log audit rows
# -----------------------------------------------------------------------
def run_dq_rules(df: DataFrame, table_name: str, rules: List[DQRule], layer: str = LAYER):
    total_checked = df.count()
    audit_rows = []
    overall_failed_row_ids = set()

    for rule in rules:
        failing_df = rule.predicate(df)
        failed_count = failing_df.count()

        if failed_count > 0:
            (failing_df
                .withColumn("_dq_rule_name", F.lit(rule.name))
                .withColumn("_dq_run_id", F.lit(RUN_ID))
                .withColumn("_dq_flagged_ts", F.current_timestamp())
                .write.format("delta").mode("append")
                .option("mergeSchema", "true")
                .save(f"{QUARANTINE_BASE}/{table_name}_rejected"))

        audit_rows.append((
            RUN_ID, layer, table_name, rule.name, rule.category,
            total_checked, failed_count, rule.severity, datetime.utcnow()
        ))

    audit_cols = ["RunID","LayerName","TableName","RuleName","RuleCategory",
                  "RecordsChecked","RecordsFailed","Severity","ExecutionDateTime"]
    audit_df = spark.createDataFrame(audit_rows, audit_cols)

    (audit_df.write.format("jdbc")
        .option("url", "jdbc:sqlserver://control-sql-host:1433;database=TripOpsControlDB")
        .option("dbtable", "ctl.DataQualityAudit")
        .option("user", dbutils.secrets.get("kv-tripanalytics", "sql-control-user"))
        .option("password", dbutils.secrets.get("kv-tripanalytics", "sql-control-password"))
        .mode("append")
        .save())

    total_failed = sum(r[6] for r in audit_rows)
    quality_score = round(100.0 * (total_checked - total_failed) / total_checked, 2) if total_checked else 100.0
    return quality_score, audit_df

# COMMAND ----------
# -----------------------------------------------------------------------
# Apply to trips_silver (example wiring — extend to every table)
# -----------------------------------------------------------------------
SILVER_BASE = "abfss://silver@sttripanalyticsprod.dfs.core.windows.net/trip-analytics"
trips_silver = spark.read.format("delta").load(f"{SILVER_BASE}/trips_silver")
drivers_silver = spark.read.format("delta").load(f"{SILVER_BASE}/drivers_silver")
customers_silver = spark.read.format("delta").load(f"{SILVER_BASE}/customers_silver")

trip_rules = [
    null_check("TripID"),
    null_check("DriverID"),
    null_check("CustomerID"),
    duplicate_check(["TripCode"]),
    range_check("FareAmount", min_val=0, max_val=1000),
    range_check("TripDistanceMiles", min_val=0, max_val=500),
    date_validity_check("PickupDateTime"),
    referential_check(trips_silver, "DriverID", drivers_silver, "DriverID"),
    referential_check(trips_silver, "CustomerID", customers_silver, "CustomerID"),
]

trips_quality_score, trips_dq_audit = run_dq_rules(trips_silver, "trips_silver", trip_rules)

print(f"trips_silver overall quality score: {trips_quality_score}%")

# COMMAND ----------
# -----------------------------------------------------------------------
# Exit value consumed by ADF's WebActivity gate
# (PL_Master_Orchestration -> "If Quality Score Below Threshold")
# -----------------------------------------------------------------------
import json
dbutils.notebook.exit(json.dumps({"overall_quality_score": trips_quality_score}))
