# Online Course Analytics DLT Pipeline Documentation

## Overview

This documentation describes a multi-layered data pipeline for online course analytics, implemented using Databricks Delta Live Tables (DLT). The pipeline leverages the **medallion architecture** (Bronze → Silver → Gold), ensuring raw data integrity, robust cleaning and validation, and powerful business KPI aggregation.  
Each layer is explained with self-explanatory, commented SQL code to help users understand, audit, and extend the workflow.

---

## Bronze Layer Transformations

**Purpose:**  
The Bronze Layer stores raw ingested data directly from CSV files with minimal transformation. It adds ingestion metadata (timestamp and source file name) and acts as the immutable system of record.

### Bronze Tables Code

```sql
-- =========================
-- Bronze Layer: Raw Data Ingestion
-- =========================
-- Each table below ingests raw CSV data from its respective folder.
-- Adds metadata columns for ingestion timestamp and source file name.
-- Data is streamed to support both batch and incremental loads.

-- ---------------------------------
-- Users Bronze Table
-- ---------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE online_course_analytics_dlt.bronze_layer.users_bronze
COMMENT "Raw users data ingested from CSV"
AS
SELECT
  *,                                    -- All columns from source CSV (raw)
  current_timestamp() AS ingestion_time, -- Metadata: When was this row ingested
  _metadata.file_name AS source_file     -- Metadata: Which file did this row come from
FROM STREAM read_files(
  '/Volumes/online_course_analytics_dlt/raw_data/volume/users/', -- Path to raw users CSV files
  format => 'csv',                      -- CSV format
  header => 'true',                     -- First row contains column names
  inferSchema => 'true',                -- Automatically infer column types
  includeExistingFiles => 'true'        -- Include both new and existing files (streaming)
);

-- ---------------------------------
-- Courses Bronze Table
-- ---------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE online_course_analytics_dlt.bronze_layer.courses_bronze
COMMENT "Raw courses data ingested from CSV"
AS
SELECT
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/online_course_analytics_dlt/raw_data/volume/courses/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  includeExistingFiles => 'true'
);

-- ---------------------------------
-- Enrollments Bronze Table
-- ---------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE online_course_analytics_dlt.bronze_layer.enrollments_bronze
COMMENT "Raw enrollments data ingested from CSV"
AS
SELECT
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/online_course_analytics_dlt/raw_data/volume/enrollments/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  includeExistingFiles => 'true'
);

-- ---------------------------------
-- Assignments Bronze Table
-- ---------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE online_course_analytics_dlt.bronze_layer.assignments_bronze
COMMENT "Raw assignments data ingested from CSV"
AS
SELECT
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/online_course_analytics_dlt/raw_data/volume/assignments/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  includeExistingFiles => 'true'
);

-- ---------------------------------
-- Video Views Bronze Table
-- ---------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE online_course_analytics_dlt.bronze_layer.video_views_bronze
COMMENT "Raw video views data ingested from CSV"
AS
SELECT
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/online_course_analytics_dlt/raw_data/volume/video_views/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  includeExistingFiles => 'true'
);

-- ---------------------------------
-- Dropout Flags Bronze Table
-- ---------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE online_course_analytics_dlt.bronze_layer.dropout_flags_bronze
COMMENT "Raw dropout flags data ingested from CSV"
AS
SELECT
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/online_course_analytics_dlt/raw_data/volume/dropout_flags/',
  format => 'csv',
  header => 'true',
  inferSchema => 'true',
  includeExistingFiles => 'true'
);
```

---

## Silver Layer Transformations

**Purpose:**  
The Silver Layer performs data cleaning, standardization, deduplication, and referential integrity checks. This ensures that downstream analytics (Gold Layer) are based on trusted, accurate, and consistent data.

### Silver Tables Code

```sql
-- =========================
-- Silver Layer: Data Cleaning and Validation
-- =========================
-- Each table below applies business logic to clean, standardize, deduplicate, and validate the raw (bronze) data.
-- Referential integrity is enforced for foreign key relationships.

-- ---------------------------------
-- Users Silver Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.silver_layer.users_silver
COMMENT "Cleaned Users table"
AS
SELECT
  user_id,        -- Unique identifier for each user
  name,           -- User name (as received)
  CASE 
    WHEN age < 0 OR age IS NULL THEN NULL   -- Replace invalid or missing ages with NULL
    ELSE age 
  END AS age,
  country,        -- Country info, as received
  registration_date, -- Registration date
  ingestion_time, -- Metadata from bronze
  source_file     -- Metadata from bronze
FROM online_course_analytics_dlt.bronze_layer.users_bronze
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id                 -- Deduplicate by user_id
    ORDER BY ingestion_time DESC         -- Keep latest record per user
) = 1;

-- ---------------------------------
-- Courses Silver Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.silver_layer.courses_silver
COMMENT "Cleaned Courses table"
AS
SELECT
  course_id,                     -- Unique identifier for each course
  INITCAP(title) AS title,       -- Standardize course titles (capitalize)
  INITCAP(category) AS category, -- Standardize categories (capitalize)
  instructor_name,               -- Instructor name, as received
  ingestion_time,
  source_file
FROM online_course_analytics_dlt.bronze_layer.courses_bronze
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY course_id
    ORDER BY ingestion_time DESC
) = 1;

-- ---------------------------------
-- Enrollments Silver Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.silver_layer.enrollments_silver
COMMENT "Validated Enrollments with referential integrity"
AS
SELECT
  e.enrollment_id,     -- Unique enrollment record
  e.user_id,           -- Must reference valid user
  e.course_id,         -- Must reference valid course
  e.enrollment_date,   -- Date of enrollment
  e.ingestion_time,
  e.source_file
FROM online_course_analytics_dlt.bronze_layer.enrollments_bronze e
INNER JOIN online_course_analytics_dlt.silver_layer.users_silver u
  ON e.user_id = u.user_id             -- Enforce user foreign key
INNER JOIN online_course_analytics_dlt.silver_layer.courses_silver c
  ON e.course_id = c.course_id         -- Enforce course foreign key
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY enrollment_id
    ORDER BY e.ingestion_time DESC
) = 1; 
-- Only enrollments with valid user and course references are kept.

-- ---------------------------------
-- Assignments Silver Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.silver_layer.assignments_silver
COMMENT "Cleaned Assignments table"
AS
SELECT
  assignment_id,      -- Unique assignment record
  user_id,            -- Must reference valid user (checked downstream)
  course_id,          -- Must reference valid course (checked downstream)
  CASE 
    WHEN score < 0 OR score > 100 THEN NULL  -- Only keep scores in [0,100]
    ELSE score 
  END AS score,
  submission_date,    -- Date of submission
  ingestion_time,
  source_file
FROM online_course_analytics_dlt.bronze_layer.assignments_bronze
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY assignment_id
    ORDER BY ingestion_time DESC
) = 1;

-- ---------------------------------
-- Video Views Silver Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.silver_layer.video_views_silver
COMMENT "Cleaned Video Views table"
AS
SELECT
  view_id,            -- Unique view record
  user_id,            -- Must reference valid user (checked downstream)
  course_id,          -- Must reference valid course (checked downstream)
  video_id,           -- Unique video identifier
  CASE 
    WHEN watch_time < 0 THEN 0     -- Negative watch times set to 0
    ELSE watch_time 
  END AS watch_time,
  view_date,          -- Date of view
  ingestion_time,
  source_file
FROM online_course_analytics_dlt.bronze_layer.video_views_bronze
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY view_id
    ORDER BY ingestion_time DESC
) = 1;

-- ---------------------------------
-- Dropout Flags Silver Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.silver_layer.dropout_flags_silver
COMMENT "Cleaned Dropout Flags table"
AS
SELECT
  user_id,                -- User identifier
  course_id,              -- Course identifier
  CAST(dropped_out AS BOOLEAN) AS dropped_out, -- Enforce Boolean type (true/false)
  ingestion_time,
  source_file
FROM online_course_analytics_dlt.bronze_layer.dropout_flags_bronze
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, course_id
    ORDER BY ingestion_time DESC
) = 1;
```

---

## Gold Layer KPIs

**Purpose:**  
The Gold Layer computes business KPIs and aggregates for analytics and reporting. These tables provide actionable insights at user, course, and platform level.

### Gold Tables Code

```sql
-- =========================
-- Gold Layer: KPI Aggregations
-- =========================
-- Each table below computes high-level business metrics using the clean, validated Silver tables.
-- KPIs are grouped by user, course, or platform.

-- ---------------------------------
-- 1️⃣ User-Level KPI Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.gold_layer.user_kpis
COMMENT "Aggregated KPIs at user level with enhanced metrics"
AS
SELECT
    u.user_id,    -- User identifier
    u.name,       -- User name
    u.country,    -- User's country
    COUNT(DISTINCT e.course_id) AS total_courses_enrolled, -- # courses enrolled
    COUNT(DISTINCT CASE WHEN d.dropped_out = false OR d.dropped_out IS NULL THEN e.course_id END) AS total_courses_completed, -- # courses completed
    COUNT(DISTINCT a.assignment_id) AS total_assignments_submitted, -- # submitted assignments
    ROUND(AVG(a.score), 2) AS avg_assignment_score, -- Average assignment score
    COALESCE(SUM(v.watch_time), 0) AS total_watch_time, -- Total video watch time
    ROUND(COALESCE(SUM(v.watch_time)/NULLIF(COUNT(DISTINCT e.course_id),0),0),2) AS avg_watch_time_per_course, -- Avg. watch time per course
    MAX(CAST(d.dropped_out AS BOOLEAN)) AS dropped_out, -- Dropout status (true/false)
    ROUND(100 * COUNT(DISTINCT CASE WHEN d.dropped_out = false OR d.dropped_out IS NULL THEN e.course_id END) 
        / NULLIF(COUNT(DISTINCT e.course_id),0),2) AS completion_rate_percent, -- % of courses completed
    CURRENT_TIMESTAMP() AS kpi_generated_time -- When was this KPI calculated
FROM online_course_analytics_dlt.silver_layer.users_silver u
LEFT JOIN online_course_analytics_dlt.silver_layer.enrollments_silver e
    ON u.user_id = e.user_id
LEFT JOIN online_course_analytics_dlt.silver_layer.assignments_silver a
    ON u.user_id = a.user_id
LEFT JOIN online_course_analytics_dlt.silver_layer.video_views_silver v
    ON u.user_id = v.user_id
LEFT JOIN online_course_analytics_dlt.silver_layer.dropout_flags_silver d
    ON u.user_id = d.user_id AND e.course_id = d.course_id
GROUP BY
    u.user_id, u.name, u.country;

-- ---------------------------------
-- 2️⃣ Course-Level KPI Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.gold_layer.course_kpis
COMMENT "Aggregated KPIs at course level with enhanced metrics"
AS
SELECT
    c.course_id,    -- Course identifier
    c.title,        -- Course title
    c.category,     -- Course category
    c.instructor_name, -- Instructor
    COUNT(DISTINCT e.user_id) AS total_enrollments, -- # user enrollments
    COUNT(DISTINCT CASE WHEN d.dropped_out = false OR d.dropped_out IS NULL THEN e.user_id END) AS total_users_completed, -- # users completed
    COUNT(DISTINCT a.assignment_id) AS total_assignments_submitted, -- Assignments submitted
    ROUND(AVG(a.score), 2) AS avg_assignment_score, -- Average assignment score
    MAX(a.score) AS highest_score, -- Highest assignment score
    MIN(a.score) AS lowest_score,  -- Lowest assignment score
    COALESCE(SUM(v.watch_time), 0) AS total_watch_time, -- Total video watch time
    ROUND(COALESCE(SUM(v.watch_time)/NULLIF(COUNT(DISTINCT e.user_id),0),0),2) AS avg_watch_time_per_user, -- Avg. watch time per user
    ROUND(100 * COUNT(DISTINCT CASE WHEN d.dropped_out = false OR d.dropped_out IS NULL THEN e.user_id END) 
        / NULLIF(COUNT(DISTINCT e.user_id),0),2) AS completion_rate_percent, -- % of users completed
    ROUND(100 * SUM(CASE WHEN d.dropped_out THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT e.user_id), 0), 2) AS dropout_rate_percent, -- % dropout
    CURRENT_TIMESTAMP() AS kpi_generated_time
FROM online_course_analytics_dlt.silver_layer.courses_silver c
LEFT JOIN online_course_analytics_dlt.silver_layer.enrollments_silver e
    ON c.course_id = e.course_id
LEFT JOIN online_course_analytics_dlt.silver_layer.assignments_silver a
    ON c.course_id = a.course_id
LEFT JOIN online_course_analytics_dlt.silver_layer.video_views_silver v
    ON c.course_id = v.course_id
LEFT JOIN online_course_analytics_dlt.silver_layer.dropout_flags_silver d
    ON c.course_id = d.course_id AND e.user_id = d.user_id
GROUP BY
    c.course_id, c.title, c.category, c.instructor_name;

-- ---------------------------------
-- 3️⃣ Platform-Level KPI Table
-- ---------------------------------
CREATE OR REFRESH LIVE TABLE online_course_analytics_dlt.gold_layer.platform_kpis
COMMENT "Aggregated KPIs for the overall platform with enhanced metrics"
AS
SELECT
    COUNT(DISTINCT u.user_id) AS total_users, -- Total unique users
    COUNT(DISTINCT e.course_id) AS total_courses, -- Total unique courses
    COUNT(DISTINCT e.enrollment_id) AS total_enrollments, -- Total enrollments
    COUNT(DISTINCT a.assignment_id) AS total_assignments_submitted, -- Assignments submitted
    ROUND(AVG(a.score), 2) AS avg_assignment_score, -- Average assignment score overall
    MAX(a.score) AS highest_score, -- Highest assignment score overall
    MIN(a.score) AS lowest_score,  -- Lowest assignment score overall
    COALESCE(SUM(v.watch_time), 0) AS total_watch_time, -- Total watch time
    ROUND(COALESCE(SUM(v.watch_time)/NULLIF(COUNT(DISTINCT u.user_id),0),0),2) AS avg_watch_time_per_user, -- Avg. watch time per user
    ROUND(COALESCE(COUNT(DISTINCT CASE WHEN d.dropped_out = false OR d.dropped_out IS NULL THEN e.course_id END)
        /NULLIF(COUNT(DISTINCT e.course_id),0),0)*100,2) AS overall_completion_rate_percent, -- Platform-wide completion rate
    ROUND(100 * SUM(CASE WHEN d.dropped_out THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT u.user_id), 0), 2) AS overall_dropout_rate_percent, -- Platform-wide dropout rate
    ROUND(COALESCE(COUNT(DISTINCT e.course_id)/NULLIF(COUNT(DISTINCT u.user_id),0),0),2) AS avg_courses_per_user, -- Avg. courses enrolled per user
    CURRENT_TIMESTAMP() AS kpi_generated_time
FROM online_course_analytics_dlt.silver_layer.users_silver u
LEFT JOIN online_course_analytics_dlt.silver_layer.enrollments_silver e
    ON u.user_id = e.user_id
LEFT JOIN online_course_analytics_dlt.silver_layer.assignments_silver a
    ON u.user_id = a.user_id
LEFT JOIN online_course_analytics_dlt.silver_layer.video_views_silver v
    ON u.user_id = v.user_id
LEFT JOIN online_course_analytics_dlt.silver_layer.dropout_flags_silver d
    ON u.user_id = d.user_id AND e.course_id = d.course_id;
```

---

## KPI Definitions

| KPI                       | Description                                                         |
|---------------------------|---------------------------------------------------------------------|
| total_courses_enrolled    | Number of distinct courses a user is enrolled in                    |
| total_courses_completed   | Number of courses completed (not dropped out)                       |
| avg_assignment_score      | Average score received for assignments                              |
| completion_rate_percent   | Percentage of courses completed out of enrolled courses             |
| dropout_rate_percent      | Percentage of users/courses with dropout flags                      |
| total_watch_time          | Sum of all video view times                                         |
| ...                      | See code for additional metrics                                     |

---

## Notes

- **Immutability:** Bronze tables are not modified after ingestion.
- **Streaming:** Pipeline supports both batch and incremental ingestion.
- **Deduplication/Validation:** Silver tables ensure only latest, valid data is kept.
- **Integrity:** Silver layer enforces referential integrity for relationships.
- **Business Metrics:** Gold layer is ready for reporting and dashboarding.
