# Security & Governance

## 1. Storage Security (ADLS Gen2)
- **RBAC**: Storage account access granted via Azure RBAC roles
  (`Storage Blob Data Contributor` for ADF/Databricks service principals,
  `Storage Blob Data Reader` for BI/reporting identities) scoped at the **container**
  level (bronze/silver/gold/quarantine/staging) rather than the account level —
  enforcing least privilege between layers (e.g. BI tools get Gold read-only, never
  Bronze).
- **Managed Identity**: ADF and Databricks both authenticate to ADLS Gen2 using their
  **system-assigned Managed Identity** — no storage account keys or SAS tokens are
  used for service-to-service access, eliminating a whole class of credential-leak risk.
- **SAS vs. Service Principal**: SAS tokens are reserved for narrow, time-boxed,
  external-party scenarios (e.g. a one-off vendor drop-off container) with an
  expiration under 24 hours. All internal platform components use Managed Identity or
  a Service Principal backed by Key Vault — never long-lived SAS.
- **Network**: storage account firewall restricts access to the Databricks VNet
  (via VNet injection / Private Link) and ADF's Managed VNet Integration Runtime;
  public network access disabled in Prod.
- **Encryption**: encryption at rest via Microsoft-managed keys (upgradeable to
  customer-managed keys in Key Vault for regulated workloads); TLS 1.2+ enforced in
  transit.

## 2. Databricks Security
- **Unity Catalog**: the single governance layer across all workspaces —
  `trip_analytics` catalog with `bronze` / `silver` / `gold` / `dev` / `archive`
  schemas. Grants are managed with standard SQL:
  ```sql
  GRANT SELECT ON SCHEMA trip_analytics.gold TO `powerbi-service-principal`;
  GRANT USAGE, SELECT ON SCHEMA trip_analytics.silver TO `data-engineers`;
  DENY SELECT ON TABLE trip_analytics.silver.customers_silver TO `analysts` -- PII column-masking handled via a view instead
  ```
- **Cluster Policies**: enforce standardized, cost-controlled cluster shapes per
  persona (e.g. `job-cluster-policy` locks node types, autoscaling bounds, and forces
  auto-termination at 30 minutes; interactive clusters capped at a max DBU spend).
  Prevents an analyst from spinning up an oversized always-on cluster.
- **Secrets Management**: all credentials referenced through **Databricks Secret
  Scopes backed by Azure Key Vault** (`dbutils.secrets.get(scope, key)`), never
  embedded in notebook code.
- **PII handling**: `customers_silver.Email` / `Phone` and `drivers_silver.Email` /
  `Phone` / `LicenseNumber` are exposed to non-privileged roles only through masked
  views (Unity Catalog **row filters / column masks**), e.g.
  ```sql
  CREATE OR REPLACE FUNCTION mask_email(email STRING) RETURNS STRING
  RETURN CASE WHEN is_member('data-engineers') THEN email ELSE '***MASKED***' END;
  ALTER TABLE trip_analytics.silver.customers_silver
  ALTER COLUMN Email SET MASK mask_email;
  ```

## 3. Data Governance
- **Data Lineage**: Unity Catalog automatically captures table- and column-level
  lineage across every Databricks read/write (Bronze -> Silver -> Gold), viewable in
  the Catalog Explorer lineage graph — critical for impact analysis when a source
  schema changes.
- **Cataloging**: every Gold table is registered with a business glossary entry
  (owner, refresh cadence, SLA, PII classification) using Unity Catalog **tags**
  (e.g. `pii=false`, `refresh=daily`, `owner=data-engineering`).
- **Auditing**: Unity Catalog's built-in **audit log** (query history, grant changes,
  access events) is exported to a Log Analytics workspace for a full record of who
  accessed what and when — combined with the platform's own `ctl.PipelineAudit` /
  `ctl.NotebookAudit` / `ctl.DataQualityAudit` tables for process-level auditing.
- **Data classification**: source columns are tagged at ingestion (`PII`,
  `Financial`, `Public`) so downstream masking and retention policies apply
  automatically rather than depending on manual reviewer diligence.

## 4. Identity & access summary
| Principal | Bronze | Silver | Gold | Control DB |
|---|---|---|---|---|
| ADF Managed Identity | Read/Write | — | — | Read/Write |
| Databricks Job Service Principal | Read | Read/Write | Read/Write | Write (audit) |
| Data Engineers (AAD group) | Read | Read/Write (dev) | Read | Read |
| Analysts (AAD group) | — | Read (masked) | Read | — |
| Power BI Service Principal | — | — | Read | — |
