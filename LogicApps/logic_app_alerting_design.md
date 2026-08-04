# Logic Apps Alerting — Design & Configuration

## Purpose
Provide a single, reusable HTTP-triggered Logic App (`LA-TripAnalytics-Alerts`) that
ADF and Databricks both call via `WebActivity` / REST when:
1. An ADF pipeline activity fails
2. A Databricks job/notebook task fails
3. The Data Quality score drops below the configured threshold (95%)

## Trigger
**When a HTTP request is received** — exposes a callback URL secured with a SAS-style
query key. Expected JSON schema:
```json
{
  "type": "object",
  "properties": {
    "alertType": { "type": "string", "enum": ["PipelineFailure", "JobFailure", "DataQualityBelowThreshold"] },
    "pipelineName": { "type": "string" },
    "runId": { "type": "string" },
    "failureReason": { "type": "string" },
    "qualityScore": { "type": "string" },
    "timestamp": { "type": "string" }
  },
  "required": ["alertType", "runId", "timestamp"]
}
```

## Workflow steps
1. **HTTP trigger** receives the alert payload.
2. **Condition — alertType**: branches the message template (failure vs. quality-gate).
3. **Compose — Build Email Body**: HTML template (below) populated from trigger fields.
4. **Send an email (Office 365 Outlook / SMTP connector)** to the on-call distribution
   list (`data-eng-oncall@company.com`) plus, for Critical severity, a Teams channel
   webhook post.
5. **Condition — Business Hours Check** (optional): if outside business hours and
   severity is Critical, additionally fire an **Azure Monitor Action Group** to page
   via PagerDuty/Ops Genie integration.
6. **Response**: returns HTTP 200 to the calling ADF `WebActivity` so the pipeline can
   continue (non-blocking notification).

## Sample email template
```html
Subject: [ALERT] {alertType} — {pipelineName}

<h3 style="color:#c0392b;">Trip Transaction Analytics Platform — Alert</h3>
<table>
  <tr><td><b>Alert Type</b></td><td>{alertType}</td></tr>
  <tr><td><b>Pipeline / Job</b></td><td>{pipelineName}</td></tr>
  <tr><td><b>Run ID</b></td><td>{runId}</td></tr>
  <tr><td><b>Failure Reason</b></td><td>{failureReason}</td></tr>
  <tr><td><b>Quality Score</b></td><td>{qualityScore}%</td></tr>
  <tr><td><b>Timestamp (UTC)</b></td><td>{timestamp}</td></tr>
</table>
<p>View run details in
   <a href="https://adf.azure.com/monitoring/pipelineruns/{runId}">Azure Data Factory Monitor</a>.
</p>
```

## Configuration steps (Azure Portal)
1. Create Logic App `LA-TripAnalytics-Alerts` (Consumption plan is sufficient for this
   volume — a handful of alerts/day).
2. Add trigger **When an HTTP request is received**; generate schema from a sample
   payload; save to obtain the callback URL.
3. Add the Office 365 Outlook (or SMTP) connector; authenticate with a service account
   or Managed Identity where supported.
4. Add a **Condition** action branching on `alertType`.
5. Store the callback URL in Key Vault (`logicapp-alert-url`) and reference it from ADF
   via a **global parameter** (`LogicAppAlertUrl`) so the URL is never hardcoded in
   pipeline JSON.
6. Enable **Logic App run history retention** (90 days) for audit purposes.
7. Add Application Insights diagnostic settings on the Logic App for observability.

## Wiring from ADF / Databricks
- ADF: `WebActivity` (see `PL_Master_Orchestration.json`, activities
  `"Call Logic App - DQ Alert"` and `"Handle Pipeline Failure"`).
- Databricks: on job failure, a **Databricks Job "on failure" webhook notification**
  (configured in the Jobs UI / `webhook_notifications.on_failure`) points at the same
  Logic App URL for jobs that run outside ADF orchestration (e.g. ad-hoc backfills).
