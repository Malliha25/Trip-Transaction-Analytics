/* =====================================================================
   Trip Transaction Analytics Platform
   SAMPLE DATA GENERATOR
   Generates 60 Locations, 60 Drivers, 80 Customers, 300+ Trips,
   300+ Payments with realistic referential relationships.
   Run AFTER 01_source_ddl.sql
   ===================================================================== */
USE TripOpsDB;
GO

------------------------------------------------------------------------
-- 1. LOCATIONS (60 zones)
------------------------------------------------------------------------
DECLARE @i INT = 1;
DECLARE @cities TABLE (City VARCHAR(50), State VARCHAR(50), Borough VARCHAR(50), Lat DECIMAL(9,6), Lon DECIMAL(9,6));
INSERT INTO @cities VALUES
('Houston','TX','Downtown',29.7604,-95.3698),
('Houston','TX','Midtown',29.7400,-95.3750),
('Dallas','TX','Uptown',32.7955,-96.8020),
('Dallas','TX','Deep Ellum',32.7845,-96.7836),
('Chicago','IL','Loop',41.8781,-87.6298),
('Chicago','IL','Wicker Park',41.9088,-87.6796),
('Philadelphia','PA','Center City',39.9526,-75.1652),
('Philadelphia','PA','Fishtown',39.9698,-75.1290),
('Atlanta','GA','Midtown',33.7838,-84.3833),
('Atlanta','GA','Buckhead',33.8484,-84.3781);

;WITH seq AS (
    SELECT TOP 60 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.Locations (LocationCode, ZoneName, Borough, City, State, Latitude, Longitude, IsActive)
SELECT
    CONCAT('LOC-', RIGHT('0000'+CAST(n AS VARCHAR(4)),4)),
    CONCAT(c.Borough,' Zone ', n),
    c.Borough, c.City, c.State,
    c.Lat + (CAST(n AS DECIMAL(9,6))*0.0007),
    c.Lon - (CAST(n AS DECIMAL(9,6))*0.0006),
    1
FROM seq
CROSS APPLY (SELECT TOP 1 * FROM @cities ORDER BY NEWID()) c;
GO

------------------------------------------------------------------------
-- 2. DRIVERS (60 drivers)
------------------------------------------------------------------------
DECLARE @fn TABLE(Name VARCHAR(30)); INSERT INTO @fn VALUES ('James'),('Maria'),('Ahmed'),('Wei'),('Priya'),('Carlos'),('Fatima'),('John'),('Linda'),('Omar');
DECLARE @ln TABLE(Name VARCHAR(30)); INSERT INTO @ln VALUES ('Smith'),('Garcia'),('Khan'),('Chen'),('Patel'),('Rodriguez'),('Ali'),('Johnson'),('Brown'),('Hassan');
DECLARE @makes TABLE(Make VARCHAR(20), Model VARCHAR(20)); INSERT INTO @makes VALUES ('Toyota','Camry'),('Honda','Accord'),('Ford','Fusion'),('Hyundai','Sonata'),('Nissan','Altima');

;WITH seq AS (SELECT TOP 60 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.Drivers (DriverCode, FirstName, LastName, Email, Phone, LicenseNumber, VehicleMake, VehicleModel, VehicleYear, HireDate, Status, HomeLocationID)
SELECT
    CONCAT('DRV-', RIGHT('0000'+CAST(n AS VARCHAR(4)),4)),
    f.Name, l.Name,
    LOWER(CONCAT(f.Name,'.',l.Name,n,'@tripops.com')),
    CONCAT('+1-713-', RIGHT('0000000'+CAST(1000000+n AS VARCHAR(7)),7)),
    CONCAT('LIC-', RIGHT('000000'+CAST(100000+n AS VARCHAR(6)),6)),
    m.Make, m.Model, 2016 + (n % 8),
    DATEADD(DAY, -1*(n*7 % 900), CAST(GETDATE() AS DATE)),
    CASE WHEN n % 17 = 0 THEN 'Suspended' WHEN n % 11 = 0 THEN 'Inactive' ELSE 'Active' END,
    (SELECT TOP 1 LocationID FROM dbo.Locations ORDER BY NEWID())
FROM seq
CROSS APPLY (SELECT TOP 1 * FROM @fn ORDER BY NEWID()) f
CROSS APPLY (SELECT TOP 1 * FROM @ln ORDER BY NEWID()) l
CROSS APPLY (SELECT TOP 1 * FROM @makes ORDER BY NEWID()) m;
GO

------------------------------------------------------------------------
-- 3. CUSTOMERS (80 customers)
------------------------------------------------------------------------
DECLARE @fn TABLE(Name VARCHAR(30)); INSERT INTO @fn VALUES ('Emily'),('Michael'),('Sara'),('David'),('Aisha'),('Kevin'),('Nina'),('Tom'),('Grace'),('Sam');
DECLARE @ln TABLE(Name VARCHAR(30)); INSERT INTO @ln VALUES ('Wilson'),('Davis'),('Lee'),('Martinez'),('Clark'),('Walker'),('Young'),('King'),('Wright'),('Scott');
DECLARE @pay TABLE(Method VARCHAR(20)); INSERT INTO @pay VALUES ('Card'),('Wallet'),('Cash');
DECLARE @tier TABLE(Tier VARCHAR(20)); INSERT INTO @tier VALUES ('Standard'),('Standard'),('Silver'),('Gold'),('Platinum');

;WITH seq AS (SELECT TOP 80 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.Customers (CustomerCode, FirstName, LastName, Email, Phone, SignupDate, PreferredPaymentMethod, LoyaltyTier, HomeLocationID)
SELECT
    CONCAT('CUS-', RIGHT('0000'+CAST(n AS VARCHAR(4)),4)),
    f.Name, l.Name,
    LOWER(CONCAT(f.Name,'.',l.Name,n,'@mail.com')),
    CONCAT('+1-281-', RIGHT('0000000'+CAST(2000000+n AS VARCHAR(7)),7)),
    DATEADD(DAY, -1*(n*11 % 1200), CAST(GETDATE() AS DATE)),
    p.Method, t.Tier,
    (SELECT TOP 1 LocationID FROM dbo.Locations ORDER BY NEWID())
FROM seq
CROSS APPLY (SELECT TOP 1 * FROM @fn ORDER BY NEWID()) f
CROSS APPLY (SELECT TOP 1 * FROM @ln ORDER BY NEWID()) l
CROSS APPLY (SELECT TOP 1 * FROM @pay ORDER BY NEWID()) p
CROSS APPLY (SELECT TOP 1 * FROM @tier ORDER BY NEWID()) t;
GO

------------------------------------------------------------------------
-- 4. TRIPS (350 trips across the last 30 days)
------------------------------------------------------------------------
;WITH seq AS (SELECT TOP 350 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.Trips (TripCode, DriverID, CustomerID, PickupLocationID, DropoffLocationID,
                       RequestDateTime, PickupDateTime, DropoffDateTime, TripDistanceMiles,
                       TripDurationMin, FareAmount, TipAmount, TaxAmount, TripStatus,
                       CustomerRating, DriverRating)
SELECT
    CONCAT('TRP-', RIGHT('000000'+CAST(n AS VARCHAR(6)),6)),
    d.DriverID, c.CustomerID, pl.LocationID, dl.LocationID,
    dt, DATEADD(MINUTE, 1+(n%5), dt),
    CASE WHEN n % 23 = 0 THEN NULL ELSE DATEADD(MINUTE, 8 + (n % 40), dt) END,
    CAST(1 + (n % 22) + (n % 3)*0.4 AS DECIMAL(8,2)),
    8 + (n % 45),
    CAST(5 + (n % 60) + 0.5 AS DECIMAL(10,2)),
    CASE WHEN n % 4 = 0 THEN CAST(1 + (n % 8) AS DECIMAL(10,2)) ELSE 0 END,
    CAST((5 + (n % 60)) * 0.0825 AS DECIMAL(10,2)),
    CASE WHEN n % 19 = 0 THEN 'Cancelled' WHEN n % 31 = 0 THEN 'NoShow' ELSE 'Completed' END,
    CASE WHEN n % 6 = 0 THEN NULL ELSE CAST(3.0 + (n % 21)*0.1 AS DECIMAL(2,1)) END,
    CASE WHEN n % 7 = 0 THEN NULL ELSE CAST(3.0 + (n % 21)*0.1 AS DECIMAL(2,1)) END
FROM seq
CROSS APPLY (SELECT TOP 1 DriverID FROM dbo.Drivers WHERE Status='Active' ORDER BY NEWID()) d
CROSS APPLY (SELECT TOP 1 CustomerID FROM dbo.Customers ORDER BY NEWID()) c
CROSS APPLY (SELECT TOP 1 LocationID FROM dbo.Locations ORDER BY NEWID()) pl
CROSS APPLY (SELECT TOP 1 LocationID FROM dbo.Locations ORDER BY NEWID()) dl
CROSS APPLY (SELECT DATEADD(MINUTE, -1*(n*37 % 43200), GETDATE()) AS dt) t;
GO

------------------------------------------------------------------------
-- 5. PAYMENTS (one per completed/no-show trip)
------------------------------------------------------------------------
INSERT INTO dbo.Payments (PaymentCode, TripID, PaymentMethod, PaymentStatus, Amount, ProcessedDateTime, GatewayReference)
SELECT
    CONCAT('PAY-', RIGHT('000000'+CAST(TripID AS VARCHAR(6)),6)),
    TripID,
    CASE WHEN TripID % 3 = 0 THEN 'Card' WHEN TripID % 3 = 1 THEN 'Wallet' ELSE 'Cash' END,
    CASE WHEN TripStatus = 'Cancelled' THEN 'Refunded'
         WHEN TripID % 29 = 0 THEN 'Failed'
         ELSE 'Success' END,
    TotalAmount,
    ISNULL(DropoffDateTime, PickupDateTime),
    CONCAT('GTW-', ABS(CHECKSUM(NEWID())) % 999999)
FROM dbo.Trips;
GO

PRINT 'Sample data generated: '
    + CAST((SELECT COUNT(*) FROM dbo.Locations) AS VARCHAR) + ' Locations, '
    + CAST((SELECT COUNT(*) FROM dbo.Drivers) AS VARCHAR) + ' Drivers, '
    + CAST((SELECT COUNT(*) FROM dbo.Customers) AS VARCHAR) + ' Customers, '
    + CAST((SELECT COUNT(*) FROM dbo.Trips) AS VARCHAR) + ' Trips, '
    + CAST((SELECT COUNT(*) FROM dbo.Payments) AS VARCHAR) + ' Payments.';
