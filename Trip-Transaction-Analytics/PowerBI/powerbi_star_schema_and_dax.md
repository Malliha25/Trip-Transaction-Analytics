# Power BI Reporting Layer

## Star Schema

```
                 DimDate
                    |
DimDriver -----  FactTrips  ----- DimCustomer
                    |
                DimLocation (used twice: pickup & dropoff via role-playing dimension)
```

### FactTrips (grain: one row per completed trip; sourced from `gold_driver_performance`
underlying detail, or directly from `trips_silver` for trip-level drillthrough)
| Column | Type | Notes |
|---|---|---|
| TripKey | int (surrogate) | |
| DriverKey | int (FK -> DimDriver) | |
| CustomerKey | int (FK -> DimCustomer) | |
| PickupLocationKey | int (FK -> DimLocation) | |
| DropoffLocationKey | int (FK -> DimLocation) | role-playing: `DimLocation (Dropoff)` |
| DateKey | int (FK -> DimDate) | |
| TripDistanceMiles | decimal | |
| FareAmount | decimal | |
| TipAmount | decimal | |
| TotalAmount | decimal | |
| CustomerRating | decimal | |
| DriverRating | decimal | |
| TripDurationMin | int | |

### DimDriver
`DriverKey, DriverCode, FullName, Status, VehicleMake, VehicleModel, HireDate, HomeLocationKey`

### DimCustomer
`CustomerKey, CustomerCode, FullName, LoyaltyTier, SignupDate, HomeLocationKey`

### DimDate
Standard calendar dimension: `DateKey, Date, Year, Quarter, Month, MonthName, Week,
DayOfWeek, DayName, IsWeekend, IsHoliday` — generated once and reused across all facts.

### DimLocation
`LocationKey, LocationCode, ZoneName, Borough, City, State, Latitude, Longitude`
(imported twice via **role-playing dimension** for Pickup vs. Dropoff analysis, created
in Power BI using `USERELATIONSHIP` rather than duplicating the physical table.)

## Key DAX measures

```dax
Total Revenue = SUM(FactTrips[TotalAmount])

Total Trips = COUNTROWS(FactTrips)

Average Fare = DIVIDE([Total Revenue], [Total Trips])

Average Customer Rating = AVERAGE(FactTrips[CustomerRating])

Revenue Per Day =
DIVIDE(
    [Total Revenue],
    DISTINCTCOUNT(FactTrips[DateKey])
)

Revenue MTD =
TOTALMTD([Total Revenue], DimDate[Date])

Revenue YoY % =
VAR PriorYear = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DimDate[Date]))
RETURN DIVIDE([Total Revenue] - PriorYear, PriorYear)

Customer Lifetime Value =
CALCULATE(
    [Total Revenue],
    ALLEXCEPT(FactTrips, FactTrips[CustomerKey])
)

Driver Utilization % =
VAR ActiveDriversInPeriod = DISTINCTCOUNT(FactTrips[DriverKey])
VAR TotalActiveDrivers = CALCULATE(DISTINCTCOUNT(DimDriver[DriverKey]), DimDriver[Status] = "Active")
RETURN DIVIDE(ActiveDriversInPeriod, TotalActiveDrivers)

Peak Hour Flag =
VAR HourlyRevenue = [Total Revenue]
VAR RankByHour = RANKX(ALL(DimDate[Hour]), [Total Revenue],, DESC)
RETURN IF(RankByHour <= 3, "Peak", "Off-Peak")

Trip Cancellation Rate =
VAR Cancelled = CALCULATE([Total Trips], FactTrips[TripStatus] = "Cancelled")
RETURN DIVIDE(Cancelled, [Total Trips])
```

## KPI Dashboard design (3 pages)

**Page 1 — Executive Summary**
- KPI cards: Total Revenue, Total Trips, Avg Fare, Avg Rating, Cancellation Rate
- Revenue trend line (daily, with MTD/YoY toggle)
- Map visual: revenue by pickup zone (uses DimLocation lat/long)

**Page 2 — Driver Performance**
- Table: Driver, Total Trips, Total Revenue, Avg Rating, Revenue/Day (from
  `gold_driver_performance`)
- Bar chart: Top 10 drivers by revenue
- Scatter: Trips vs. Rating (identify high-volume/low-rating outliers)

**Page 3 — Customer Behavior & Peak Analytics**
- CLV segment donut (High/Mid/Low value, from `gold_customer_behavior`)
- Ride frequency histogram
- Heatmap: Revenue by Hour x Day-of-Week (from `gold_peak_hour_analytics`)
- Peak zone bar chart

## Business insights this layer enables
- Identify under-utilized driver hours to inform incentive/surge design.
- Flag high-CLV customers for retention campaigns before churn signals appear.
- Correlate low driver ratings with specific vehicle types or zones for targeted
  coaching.
- Right-size driver supply against the peak-hour/peak-zone demand curve.
