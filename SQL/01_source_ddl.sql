/* =====================================================================
   Trip Transaction Analytics Platform
   SOURCE SYSTEM DDL  (On-Premises SQL Server 2019+)
   Database: TripOpsDB
   =====================================================================
   Design notes:
   - Surrogate INT IDENTITY keys for OLTP performance
   - Natural keys retained as unique constraints for lineage/reconciliation
   - Auditable via CreatedDate / ModifiedDate on every table (drives the
     watermark-based incremental extraction pattern used by ADF)
   ===================================================================== */

IF DB_ID('TripOpsDB') IS NULL
BEGIN
    CREATE DATABASE TripOpsDB;
END
GO
USE TripOpsDB;
GO

/* -------------------------------------------------------------------
   1. Locations  (zones / geofences trips originate & terminate in)
   ------------------------------------------------------------------- */
IF OBJECT_ID('dbo.Locations','U') IS NOT NULL DROP TABLE dbo.Locations;
CREATE TABLE dbo.Locations (
    LocationID      INT IDENTITY(1,1) PRIMARY KEY,
    LocationCode    VARCHAR(20)   NOT NULL,
    ZoneName        VARCHAR(100)  NOT NULL,
    Borough         VARCHAR(50)   NOT NULL,
    City            VARCHAR(50)   NOT NULL,
    State           VARCHAR(50)   NOT NULL,
    Latitude        DECIMAL(9,6)  NOT NULL,
    Longitude       DECIMAL(9,6)  NOT NULL,
    IsActive        BIT           NOT NULL DEFAULT 1,
    CreatedDate     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Locations_Code UNIQUE (LocationCode)
);
CREATE INDEX IX_Locations_Zone ON dbo.Locations(ZoneName);
GO

/* -------------------------------------------------------------------
   2. Drivers
   ------------------------------------------------------------------- */
IF OBJECT_ID('dbo.Drivers','U') IS NOT NULL DROP TABLE dbo.Drivers;
CREATE TABLE dbo.Drivers (
    DriverID        INT IDENTITY(1,1) PRIMARY KEY,
    DriverCode      VARCHAR(20)   NOT NULL,
    FirstName       VARCHAR(50)   NOT NULL,
    LastName        VARCHAR(50)   NOT NULL,
    Email           VARCHAR(100)  NULL,
    Phone           VARCHAR(20)   NULL,
    LicenseNumber   VARCHAR(30)   NOT NULL,
    VehicleMake     VARCHAR(50)   NULL,
    VehicleModel    VARCHAR(50)   NULL,
    VehicleYear     SMALLINT      NULL,
    HireDate        DATE          NOT NULL,
    Status          VARCHAR(20)   NOT NULL DEFAULT 'Active', -- Active/Inactive/Suspended
    HomeLocationID  INT           NULL REFERENCES dbo.Locations(LocationID),
    CreatedDate     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Drivers_Code UNIQUE (DriverCode),
    CONSTRAINT UQ_Drivers_License UNIQUE (LicenseNumber),
    CONSTRAINT CK_Drivers_Status CHECK (Status IN ('Active','Inactive','Suspended'))
);
CREATE INDEX IX_Drivers_Status ON dbo.Drivers(Status);
CREATE INDEX IX_Drivers_ModifiedDate ON dbo.Drivers(ModifiedDate); -- watermark support
GO

/* -------------------------------------------------------------------
   3. Customers
   ------------------------------------------------------------------- */
IF OBJECT_ID('dbo.Customers','U') IS NOT NULL DROP TABLE dbo.Customers;
CREATE TABLE dbo.Customers (
    CustomerID      INT IDENTITY(1,1) PRIMARY KEY,
    CustomerCode    VARCHAR(20)   NOT NULL,
    FirstName       VARCHAR(50)   NOT NULL,
    LastName        VARCHAR(50)   NOT NULL,
    Email           VARCHAR(100)  NULL,
    Phone           VARCHAR(20)   NULL,
    SignupDate      DATE          NOT NULL,
    PreferredPaymentMethod VARCHAR(20) NULL, -- Card/Wallet/Cash
    LoyaltyTier     VARCHAR(20)   NOT NULL DEFAULT 'Standard', -- Standard/Silver/Gold/Platinum
    HomeLocationID  INT           NULL REFERENCES dbo.Locations(LocationID),
    CreatedDate     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Customers_Code UNIQUE (CustomerCode)
);
CREATE INDEX IX_Customers_ModifiedDate ON dbo.Customers(ModifiedDate);
GO

/* -------------------------------------------------------------------
   4. Trips  (fact / transaction grain: one row per trip)
   ------------------------------------------------------------------- */
IF OBJECT_ID('dbo.Trips','U') IS NOT NULL DROP TABLE dbo.Trips;
CREATE TABLE dbo.Trips (
    TripID          BIGINT IDENTITY(1,1) PRIMARY KEY,
    TripCode        VARCHAR(30)   NOT NULL,
    DriverID        INT           NOT NULL REFERENCES dbo.Drivers(DriverID),
    CustomerID      INT           NOT NULL REFERENCES dbo.Customers(CustomerID),
    PickupLocationID  INT         NOT NULL REFERENCES dbo.Locations(LocationID),
    DropoffLocationID INT         NOT NULL REFERENCES dbo.Locations(LocationID),
    RequestDateTime   DATETIME2   NOT NULL,
    PickupDateTime    DATETIME2   NOT NULL,
    DropoffDateTime   DATETIME2   NULL,
    TripDistanceMiles DECIMAL(8,2) NOT NULL,
    TripDurationMin   INT          NULL,
    FareAmount        DECIMAL(10,2) NOT NULL,
    TipAmount         DECIMAL(10,2) NOT NULL DEFAULT 0,
    TaxAmount         DECIMAL(10,2) NOT NULL DEFAULT 0,
    TotalAmount       AS (ISNULL(FareAmount,0)+ISNULL(TipAmount,0)+ISNULL(TaxAmount,0)) PERSISTED,
    TripStatus        VARCHAR(20)  NOT NULL DEFAULT 'Completed', -- Completed/Cancelled/NoShow
    CustomerRating     DECIMAL(2,1) NULL,  -- 1.0 - 5.0
    DriverRating        DECIMAL(2,1) NULL,
    CreatedDate        DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Trips_Code UNIQUE (TripCode),
    CONSTRAINT CK_Trips_Status CHECK (TripStatus IN ('Completed','Cancelled','NoShow')),
    CONSTRAINT CK_Trips_Rating_C CHECK (CustomerRating IS NULL OR CustomerRating BETWEEN 1 AND 5),
    CONSTRAINT CK_Trips_Rating_D CHECK (DriverRating IS NULL OR DriverRating BETWEEN 1 AND 5),
    CONSTRAINT CK_Trips_Distance CHECK (TripDistanceMiles >= 0)
);
CREATE INDEX IX_Trips_DriverID ON dbo.Trips(DriverID);
CREATE INDEX IX_Trips_CustomerID ON dbo.Trips(CustomerID);
CREATE INDEX IX_Trips_PickupDateTime ON dbo.Trips(PickupDateTime);
CREATE INDEX IX_Trips_ModifiedDate ON dbo.Trips(ModifiedDate); -- watermark support
GO

/* -------------------------------------------------------------------
   5. Payments  (one-to-one / one-to-many with Trips; supports splits)
   ------------------------------------------------------------------- */
IF OBJECT_ID('dbo.Payments','U') IS NOT NULL DROP TABLE dbo.Payments;
CREATE TABLE dbo.Payments (
    PaymentID       BIGINT IDENTITY(1,1) PRIMARY KEY,
    PaymentCode     VARCHAR(30)   NOT NULL,
    TripID          BIGINT        NOT NULL REFERENCES dbo.Trips(TripID),
    PaymentMethod   VARCHAR(20)   NOT NULL, -- Card/Wallet/Cash/Corporate
    PaymentStatus   VARCHAR(20)   NOT NULL DEFAULT 'Success', -- Success/Failed/Refunded/Pending
    Amount          DECIMAL(10,2) NOT NULL,
    ProcessedDateTime DATETIME2   NOT NULL,
    GatewayReference  VARCHAR(50) NULL,
    CreatedDate     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_Payments_Code UNIQUE (PaymentCode),
    CONSTRAINT CK_Payments_Method CHECK (PaymentMethod IN ('Card','Wallet','Cash','Corporate')),
    CONSTRAINT CK_Payments_Status CHECK (PaymentStatus IN ('Success','Failed','Refunded','Pending')),
    CONSTRAINT CK_Payments_Amount CHECK (Amount >= 0)
);
CREATE INDEX IX_Payments_TripID ON dbo.Payments(TripID);
CREATE INDEX IX_Payments_ModifiedDate ON dbo.Payments(ModifiedDate);
GO

PRINT 'Source schema TripOpsDB created successfully.';
