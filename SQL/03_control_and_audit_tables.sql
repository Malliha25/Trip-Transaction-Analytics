/* =====================================================================
   Trip Transaction Analytics Platform
   CONTROL / METADATA-DRIVEN INCREMENTAL LOAD + AUDIT SCHEMA
   Deployed in a dedicated control database: TripOpsControlDB
   (kept separate from the OLTP source and separate from the lakehouse)
   ===================================================================== */
IF DB_ID('TripOpsControlDB') IS NULL CREATE DATABASE TripOpsControlDB;
GO
USE TripOpsControlDB;
GO

------------------------------------------------------------------------
-- 1. Watermark table -- drives incremental extraction per source table
------------------------------------------------------------------------
IF OBJECT_ID('ctl.WatermarkControl','U') IS NOT NULL DROP TABLE ctl.WatermarkControl;
IF SCHEMA_ID('ctl') IS NULL EXEC('CREATE SCHEMA ctl');
GO
CREATE TABLE ctl.WatermarkControl (
    WatermarkID       INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem      VARCHAR(50)   NOT NULL,       -- e.g. 'TripOpsDB'
    SourceSchema      VARCHAR(50)   NOT NULL,
    SourceTable       VARCHAR(100)  NOT NULL,
    WatermarkColumn   VARCHAR(100)  NOT NULL,        -- e.g. 'ModifiedDate'
    LastWatermarkValue DATETIME2    NOT NULL,
    LastRunID          VARCHAR(50)  NULL,
    LastLoadStatus      VARCHAR(20) NOT NULL DEFAULT 'Success', -- Success/Failed/Running
    LastUpdatedDate      DATETIME2  NOT NULL DEFAULT SYSUTCDATETIME(),
    IsActive              BIT       NOT NULL DEFAULT 1,
    CONSTRAINT UQ_Watermark_Table UNIQUE (SourceSystem, SourceSchema, SourceTable)
);
GO

INSERT INTO ctl.WatermarkControl (SourceSystem, SourceSchema, SourceTable, WatermarkColumn, LastWatermarkValue)
VALUES
('TripOpsDB','dbo','Trips','ModifiedDate','1900-01-01'),
('TripOpsDB','dbo','Drivers','ModifiedDate','1900-01-01'),
('TripOpsDB','dbo','Customers','ModifiedDate','1900-01-01'),
('TripOpsDB','dbo','Payments','ModifiedDate','1900-01-01'),
('TripOpsDB','dbo','Locations','ModifiedDate','1900-01-01');
GO

------------------------------------------------------------------------
-- 2. Pipeline Audit -- one row per ADF pipeline run
------------------------------------------------------------------------
IF OBJECT_ID('ctl.PipelineAudit','U') IS NOT NULL DROP TABLE ctl.PipelineAudit;
CREATE TABLE ctl.PipelineAudit (
    AuditID          BIGINT IDENTITY(1,1) PRIMARY KEY,
    PipelineName     VARCHAR(100)  NOT NULL,
    RunID            VARCHAR(50)   NOT NULL,
    TriggerType      VARCHAR(30)   NULL,             -- Scheduled/Manual/Tumbling
    SourceTable      VARCHAR(100)  NULL,
    StartTime        DATETIME2     NOT NULL,
    EndTime          DATETIME2     NULL,
    DurationSeconds   AS DATEDIFF(SECOND, StartTime, EndTime),
    Status           VARCHAR(20)   NOT NULL DEFAULT 'Running', -- Running/Succeeded/Failed
    RecordsRead      BIGINT        NULL,
    RecordsWritten    BIGINT       NULL,
    RecordsRejected    BIGINT      NULL DEFAULT 0,
    ErrorMessage       VARCHAR(4000) NULL,
    CreatedDate       DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_PipelineAudit_RunID ON ctl.PipelineAudit(RunID);
CREATE INDEX IX_PipelineAudit_Status ON ctl.PipelineAudit(Status);
GO

------------------------------------------------------------------------
-- 3. Notebook Audit -- one row per Databricks notebook / job task run
------------------------------------------------------------------------
IF OBJECT_ID('ctl.NotebookAudit','U') IS NOT NULL DROP TABLE ctl.NotebookAudit;
CREATE TABLE ctl.NotebookAudit (
    AuditID           BIGINT IDENTITY(1,1) PRIMARY KEY,
    NotebookName      VARCHAR(150)  NOT NULL,
    JobRunID          VARCHAR(50)   NOT NULL,
    ClusterID         VARCHAR(100)  NULL,
    LayerFrom         VARCHAR(20)   NULL,          -- Bronze/Silver
    LayerTo           VARCHAR(20)   NULL,          -- Silver/Gold
    TargetTable       VARCHAR(100)  NULL,
    StartTime         DATETIME2     NOT NULL,
    EndTime           DATETIME2     NULL,
    Status            VARCHAR(20)   NOT NULL DEFAULT 'Running',
    RecordsProcessed  BIGINT        NULL,
    RecordsInserted    BIGINT       NULL,
    RecordsUpdated      BIGINT      NULL,
    RecordsDeleted        BIGINT    NULL,
    ErrorMessage        VARCHAR(4000) NULL,
    CreatedDate         DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_NotebookAudit_JobRunID ON ctl.NotebookAudit(JobRunID);
GO

------------------------------------------------------------------------
-- 4. Data Quality Audit -- one row per DQ rule execution
------------------------------------------------------------------------
IF OBJECT_ID('ctl.DataQualityAudit','U') IS NOT NULL DROP TABLE ctl.DataQualityAudit;
CREATE TABLE ctl.DataQualityAudit (
    DQAuditID        BIGINT IDENTITY(1,1) PRIMARY KEY,
    RunID            VARCHAR(50)   NOT NULL,
    LayerName        VARCHAR(20)   NOT NULL,      -- Bronze/Silver/Gold
    TableName        VARCHAR(100)  NOT NULL,
    RuleName         VARCHAR(100)  NOT NULL,      -- e.g. 'NullCheck_TripID'
    RuleCategory     VARCHAR(30)   NOT NULL,      -- Null/Duplicate/Range/Referential/Format
    RecordsChecked    BIGINT       NOT NULL,
    RecordsFailed       BIGINT     NOT NULL,
    QualityScorePct   AS (CASE WHEN RecordsChecked = 0 THEN 100.0
                          ELSE CAST(100.0 * (RecordsChecked - RecordsFailed) / RecordsChecked AS DECIMAL(5,2)) END),
    Severity          VARCHAR(20)  NOT NULL DEFAULT 'Warning', -- Warning/Critical
    ExecutionDateTime  DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX IX_DQAudit_Table ON ctl.DataQualityAudit(TableName, LayerName);
GO

------------------------------------------------------------------------
-- 5. Sample audit rows (illustrative)
------------------------------------------------------------------------
INSERT INTO ctl.PipelineAudit (PipelineName, RunID, TriggerType, SourceTable, StartTime, EndTime, Status, RecordsRead, RecordsWritten, RecordsRejected)
VALUES
('PL_Master_Orchestration','20260803-0100-8841','Scheduled','ALL','2026-08-03 01:00:00','2026-08-03 01:14:22','Succeeded',791,791,0),
('PL_Raw_Data_Ingestion','20260804-0100-9123','Scheduled','Trips','2026-08-04 01:00:00','2026-08-04 01:05:10','Succeeded',350,350,0);

INSERT INTO ctl.NotebookAudit (NotebookName, JobRunID, LayerFrom, LayerTo, TargetTable, StartTime, EndTime, Status, RecordsProcessed, RecordsInserted, RecordsUpdated)
VALUES
('01_bronze_to_silver','9123-nb-01','Bronze','Silver','trips_silver','2026-08-04 01:06:00','2026-08-04 01:09:40','Succeeded',350,340,10),
('02_silver_to_gold','9123-nb-02','Silver','Gold','gold_driver_performance','2026-08-04 01:10:00','2026-08-04 01:11:55','Succeeded',60,60,0);

INSERT INTO ctl.DataQualityAudit (RunID, LayerName, TableName, RuleName, RuleCategory, RecordsChecked, RecordsFailed, Severity)
VALUES
('9123-nb-01','Silver','trips_silver','NullCheck_TripID','Null',350,0,'Critical'),
('9123-nb-01','Silver','trips_silver','DuplicateCheck_TripCode','Duplicate',350,2,'Warning'),
('9123-nb-01','Silver','trips_silver','RangeCheck_FareAmount','Range',350,1,'Warning');
GO

PRINT 'Control/audit schema deployed in TripOpsControlDB.';
