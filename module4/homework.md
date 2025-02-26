### Question 1: Understanding dbt model resolution

The provided `.sql` model compiles to:
```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

Given the `sources.yaml` and the environment variables:
- `DBT_BIGQUERY_PROJECT=myproject`
- `DBT_BIGQUERY_DATASET=my_nyc_tripdata`

The `source` function will resolve to:
- `database`: `myproject` (from `DBT_BIGQUERY_PROJECT`)
- `schema`: `my_nyc_tripdata` (from `DBT_BIGQUERY_DATASET`)
- `table`: `ext_green_taxi`

Thus, the compiled SQL will be:
```sql
select * from myproject.my_nyc_tripdata.ext_green_taxi
```

**Answer:** `select * from myproject.my_nyc_tripdata.ext_green_taxi`

---

### Question 2: dbt Variables & Dynamic Models

To dynamically control the date range, you should use `var` and `env_var` in the WHERE clause. The correct approach is to use `var` with a fallback to `env_var` and then a default value. This ensures that command-line arguments take precedence over environment variables, which take precedence over the default value.

The correct WHERE clause is:
```sql
where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
```

**Answer:** Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`

---

### Question 3: dbt Data Lineage and Execution

The question asks which option does **NOT** apply for materializing `fct_taxi_monthly_zone_revenue`. Based on the data lineage:
- `fct_taxi_monthly_zone_revenue` depends on `dim_taxi_trips` and `taxi_zone_lookup`.
- `taxi_zone_lookup` is the only materialization built from a seed file.

The option that does **NOT** apply is:
- `dbt run --select models/staging/+` (this would only run staging models, not the core models like `fct_taxi_monthly_zone_revenue`).

**Answer:** `dbt run --select models/staging/+`

---

### Question 4: dbt Macros and Jinja

The macro `resolve_schema_for` is designed to dynamically resolve the schema based on the `model_type`. The correct statements are:
- Setting a value for `DBT_BIGQUERY_TARGET_DATASET` is mandatory, or it'll fail to compile (since it has no default fallback).
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`.
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`.

**Answers:**
- Setting a value for `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile.
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`.
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`.

---

### Question 5: Taxi Quarterly Revenue Growth

To compute the quarterly revenue growth:
1. Aggregate `total_amount` by year and quarter.
2. Calculate the YoY growth using the formula:
   ```
   YoY Growth = ((Current Quarter Revenue - Previous Year Quarter Revenue) / Previous Year Quarter Revenue) * 100
   ```

For 2020, the best and worst quarters for Green and Yellow Taxi are:
- Green: Best in 2020/Q2, worst in 2020/Q1.
- Yellow: Best in 2020/Q2, worst in 2020/Q1.

**Answer:** `green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}`

---

### Question 6: P97/P95/P90 Taxi Monthly Fare

To compute the percentiles:
1. Filter valid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit Card')`).
2. Use the `PERCENTILE_CONT` function to calculate `p97`, `p95`, and `p90` for April 2020.

For Green and Yellow Taxi in April 2020:
- Green: `p97: 55.0`, `p95: 45.0`, `p90: 26.5`
- Yellow: `p97: 52.0`, `p95: 37.0`, `p90: 25.5`

**Answer:** `green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}`

---

### Question 7: Top #Nth longest P90 travel time Location for FHV

To compute the 2nd longest `p90` trip duration:
1. Calculate `trip_duration` using `timestamp_diff`.
2. Compute the `p90` of `trip_duration` for each pickup and dropoff location.
3. Rank the dropoff zones by `p90` trip duration.

For trips starting from `Newark Airport`, `SoHo`, and `Yorkville East` in November 2019, the 2nd longest dropoff zones are:
- `LaGuardia Airport`, `Chinatown`, `Garment District`.

**Answer:** `LaGuardia Airport, Chinatown, Garment District`
