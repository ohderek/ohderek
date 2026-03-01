<div align="center">

<img src="https://capsule-render.vercel.app/api?type=rect&color=0d0d0d&height=180&text=NYC+Taxi+Lakehouse&fontSize=52&fontColor=ff6b35&fontAlignY=52&animation=fadeIn&desc=Databricks+%C2%B7+Delta+Lake+%C2%B7+Medallion+Architecture&descSize=19&descAlignY=75&descColor=c9a84c" />

<br/>

<img src="https://readme-typing-svg.demolab.com?font=IBM+Plex+Mono&weight=600&size=19&duration=3200&pause=900&color=ff6b35&center=true&vCenter=true&width=700&height=45&lines=3M+rows%2Fmonth.+Auto+Loader.+Exactly+once.;Bronze+%E2%86%92+Silver+%E2%86%92+Gold+%E2%86%92+Insight.;Z-ORDER%3A+60-80%25+scan+reduction.;DQ+flags.+Not+drops.+Every+row+survives." alt="Typing SVG" />

</div>

<br/>

> **Hypothetical Showcase** — production-grade lakehouse on the publicly available [NYC TLC Yellow Taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). No API key required — you can run this yourself on any Databricks workspace with Unity Catalog enabled.

---

## ◈ Architecture

```mermaid
flowchart LR
    subgraph SRC["TLC Public Data"]
        A["Monthly Parquet\nyellow_tripdata_2024-*.parquet\n~3M rows · 50-150 MB/month"]
    end

    subgraph UC["Unity Catalog Volume"]
        B["taxi_lakehouse.bronze.raw_files\nmanaged volume"]
    end

    subgraph MEDALLION["Delta Lake Medallion"]
        C[("Bronze\nraw_trips\nAuto Loader · change data feed")]
        D[("Silver\ntrips\nSHA2 key · 10 DQ rules\nZ-ORDER · MERGE upsert")]
        E1[("Gold\ntrips_by_zone\ndaily grain")]
        E2[("Gold\nhourly_patterns\nhour × DOW grain")]
        E3[("Gold\nfare_analysis\npayment × rate_code grain")]
        C -->|PySpark transforms| D
        D --> E1 & E2 & E3
    end

    A -->|01_setup.py| B
    B -->|cloudFiles Auto Loader| C
```

Two deployment options: **notebook pipeline** (step-by-step, ideal for learning) and **Delta Live Tables** (production, automatic lineage and DQ). Both produce identical output.

---

## ◈ Quick Start

**Prerequisites:** Databricks workspace with Unity Catalog · Runtime 14.3+ · ~500 MB storage

```python
# Step 1 — Run sql/catalog_setup.sql in a Databricks SQL editor
# Creates: taxi_lakehouse catalog, bronze/silver/gold schemas, all DDL

# Step 2 — Open notebooks/01_setup.py on a cluster
# Set months_to_download = 1  (or up to 12)
# Downloads Parquet files from TLC CDN directly to your Volume

# Step 3 — Run the pipeline
# Option A: Notebook by notebook
#   01_setup.py → 02_bronze_ingest.py → 03_silver_transform.py → 04_gold_aggregations.py

# Option B: Delta Live Tables (production)
#   UI → Delta Live Tables → Create Pipeline
#   Source: dlt/taxi_pipeline.py  |  Target: taxi_lakehouse  |  Mode: Triggered

# Option C: Scheduled Workflow
databricks bundle deploy && databricks bundle run taxi_lakehouse_pipeline
```

---

## ◈ How It Works

### Bronze — Auto Loader Ingestion

Auto Loader (`cloudFiles`) tracks ingested files via checkpoint, so new monthly Parquet files added to the Volume are automatically picked up without reprocessing all 12 months.

```python
spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", "parquet")
     .option("cloudFiles.schemaLocation", checkpoint_path)
     .schema(TRIPS_SCHEMA)                    # explicit — no inference scan
     .load(source_path)
```

`Trigger.AvailableNow` runs it as a controlled batch — processes all pending files, then stops. Two metadata columns are added: `_source_file` (which Parquet file the row came from) and `_ingested_at` (broker receipt time).

### Silver — Clean, Key, Validate

```python
# SHA2 surrogate key — deterministic, deduplicates across vendor sources
F.sha2(F.concat_ws("|", vendor_id, pickup_datetime, pu_location_id), 256)
```

Ten DQ rules are applied as **soft failures** — violating rows are flagged with `dq_valid=false` and `dq_flags` array, not dropped. Gold tables filter `dq_valid = true`. The full failure breakdown is queryable:

```sql
SELECT flag, COUNT(*) AS trip_count
FROM taxi_lakehouse.silver.trips
LATERAL VIEW explode(dq_flags) t AS flag
WHERE NOT dq_valid
GROUP BY 1 ORDER BY 2 DESC;
```

MERGE upsert on `trip_id` makes the Silver write idempotent — replaying Bronze data always produces the same Silver rows.

### Gold — Three Analytics Tables

| Table | Grain | Key metrics |
|---|---|---|
| `trips_by_zone` | pickup_zone × date | Trip count, avg fare, avg tip %, borough |
| `hourly_patterns` | hour × day_of_week | Volume index, weekday vs weekend split |
| `fare_analysis` | payment_type × rate_code | Avg/median/P95 fare, tip %, % of total trips |

Gold uses `OVERWRITE` mode — each run creates a new Delta version. Delta time travel makes every previous snapshot queryable:

```sql
-- Compare this week's top zones to last month's
SELECT * FROM taxi_lakehouse.gold.trips_by_zone
TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL 30 DAYS
WHERE pu_borough = 'Manhattan'
ORDER BY trip_count DESC LIMIT 10;
```

---

## ◈ Key Design Decisions

| Decision | Rationale |
|---|---|
| Auto Loader over `COPY INTO` | File-level checkpoint prevents reprocessing; `Trigger.AvailableNow` gives batch semantics without a persistent stream |
| Explicit `StructType` schema | Schema inference reads a sample of every file — expensive at scale. Prevents silent drift if TLC changes column types. |
| SHA2 surrogate key | `(VendorID, tpep_pickup_datetime, PULocationID)` deduplicates across vendor sources; hash is deterministic so replay is safe |
| Soft DQ failures (flag, don't drop) | Analysts choose to include or exclude flagged data. Quarantine pattern (DLT) tracks violations in event log without stopping the pipeline. |
| `Z-ORDER BY (pu_location_id, pickup_datetime)` | Co-locates rows by location on the same file groups — 60–80% scan reduction on location-filtered queries |
| Gold `OVERWRITE` + time travel | Each run is a clean snapshot; full history available via Delta versioning without managing SCD logic |

### DLT Expectations (quarantine pattern)

```python
@dlt.expect_all_or_drop({
    "valid_duration": "trip_duration_minutes > 0 AND trip_duration_minutes <= 1440",
    "valid_distance": "trip_distance_miles >= 0 AND trip_distance_miles <= 500",
    "valid_fare":     "fare_amount >= 0 AND fare_amount <= 1000",
})
def trips():
    ...
```

Violating rows are silently quarantined to the DLT system table. Tracked in the event log — doesn't stop the pipeline or corrupt the Silver table.

---

## ◈ Sample Output

**Top 10 pickup zones (1 month):**

| Zone | Borough | Trips | Avg Fare |
|---|---|---|---|
| Midtown Center | Manhattan | 187,429 | $16.40 |
| Upper East Side North | Manhattan | 143,210 | $14.20 |
| JFK Airport | Queens | 124,330 | $58.90 |
| Times Sq/Theatre District | Manhattan | 118,752 | $15.10 |
| LaGuardia Airport | Queens | 98,213 | $31.50 |

**Tip rate by payment type:**

| Payment Type | Trips | Avg Fare | Avg Tip % |
|---|---|---|---|
| Credit Card | 2,134,819 | $16.80 | 22.4% |
| Cash | 891,204 | $14.20 | 0.0% |

---

## ◈ Tech Stack

<div align="center">

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

</div>

---

<div align="center">

<a href="https://www.linkedin.com/in/derek-o-halloran/">
  <img src="https://img.shields.io/badge/LINKEDIN-0d0d0d?style=for-the-badge&logo=linkedin&logoColor=ff6b35" />
</a>&nbsp;
<a href="https://github.com/ohderek/data-engineering-portfolio">
  <img src="https://img.shields.io/badge/DATA_PORTFOLIO-0d0d0d?style=for-the-badge&logo=github&logoColor=ff6b35" />
</a>

<br/><br/>

<img src="https://capsule-render.vercel.app/api?type=waving&color=0,0d0d0d,100,1a1a2e&height=100&section=footer" />

</div>
