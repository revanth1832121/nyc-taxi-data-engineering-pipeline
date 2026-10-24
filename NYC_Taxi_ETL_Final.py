# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- NYC TAXI - FINAL PRODUCTION-STYLE ETL PIPELINE
# MAGIC --
# MAGIC -- FLOW:
# MAGIC -- Source
# MAGIC --   -> Data Quality Validation
# MAGIC --   -> Quarantine Invalid Records
# MAGIC --   -> Silver
# MAGIC --   -> Dimension Enrichment
# MAGIC --   -> Gold
# MAGIC --   -> Audit
# MAGIC --   -> Monitoring
# MAGIC --
# MAGIC -- IMPORTANT:
# MAGIC -- 1. Explicit column mappings are used.
# MAGIC -- 2. MERGE / NOT EXISTS provide idempotency.
# MAGIC -- 3. Invalid records never enter Silver/Gold.
# MAGIC -- ============================================================
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- STEP 1: QUARANTINE INVALID SOURCE RECORDS
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT INTO workspace.nyc_taxi.rejected_trips
# MAGIC (
# MAGIC     tpep_pickup_datetime,
# MAGIC     tpep_dropoff_datetime,
# MAGIC     trip_distance,
# MAGIC     fare_amount,
# MAGIC     pickup_zip,
# MAGIC     dropoff_zip,
# MAGIC     rejection_reason,
# MAGIC     batch_id,
# MAGIC     processed_timestamp,
# MAGIC     source_system
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     s.pickup_datetime,
# MAGIC     s.dropoff_datetime,
# MAGIC     s.trip_distance,
# MAGIC     s.fare_amount,
# MAGIC     s.pickup_zip,
# MAGIC     s.dropoff_zip,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN s.pickup_datetime IS NULL
# MAGIC           OR s.dropoff_datetime IS NULL
# MAGIC           OR s.dropoff_datetime <= s.pickup_datetime
# MAGIC             THEN 'INVALID DATETIME'
# MAGIC
# MAGIC         WHEN s.trip_distance IS NULL
# MAGIC           OR s.trip_distance <= 0
# MAGIC             THEN 'INVALID DISTANCE'
# MAGIC
# MAGIC         WHEN s.fare_amount IS NULL
# MAGIC           OR s.fare_amount <= 0
# MAGIC             THEN 'INVALID FARE'
# MAGIC
# MAGIC         ELSE 'UNKNOWN'
# MAGIC     END AS rejection_reason,
# MAGIC
# MAGIC     s.batch_id,
# MAGIC
# MAGIC     current_timestamp() AS processed_timestamp,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         s.source_system,
# MAGIC         'NYC_TAXI'
# MAGIC     ) AS source_system
# MAGIC
# MAGIC FROM workspace.nyc_taxi.source_trips s
# MAGIC
# MAGIC WHERE
# MAGIC (
# MAGIC        s.pickup_datetime IS NULL
# MAGIC
# MAGIC     OR s.dropoff_datetime IS NULL
# MAGIC
# MAGIC     OR s.dropoff_datetime <= s.pickup_datetime
# MAGIC
# MAGIC     OR s.trip_distance IS NULL
# MAGIC
# MAGIC     OR s.trip_distance <= 0
# MAGIC
# MAGIC     OR s.fare_amount IS NULL
# MAGIC
# MAGIC     OR s.fare_amount <= 0
# MAGIC )
# MAGIC
# MAGIC -- ------------------------------------------------------------
# MAGIC -- Prevent duplicate rejected records
# MAGIC -- ------------------------------------------------------------
# MAGIC
# MAGIC AND NOT EXISTS
# MAGIC (
# MAGIC     SELECT 1
# MAGIC
# MAGIC     FROM workspace.nyc_taxi.rejected_trips r
# MAGIC
# MAGIC     WHERE r.tpep_pickup_datetime
# MAGIC               <=> s.pickup_datetime
# MAGIC
# MAGIC       AND r.tpep_dropoff_datetime
# MAGIC               <=> s.dropoff_datetime
# MAGIC
# MAGIC       AND r.pickup_zip
# MAGIC               <=> s.pickup_zip
# MAGIC
# MAGIC       AND r.dropoff_zip
# MAGIC               <=> s.dropoff_zip
# MAGIC
# MAGIC       AND r.batch_id
# MAGIC               <=> s.batch_id
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- STEP 2: LOAD VALID RECORDS INTO SILVER
# MAGIC --
# MAGIC -- MERGE makes this load idempotent.
# MAGIC -- If the same source data is processed again,
# MAGIC -- the existing Silver record is not inserted again.
# MAGIC -- ============================================================
# MAGIC
# MAGIC MERGE INTO workspace.nyc_taxi.curated_trips AS target
# MAGIC
# MAGIC USING
# MAGIC (
# MAGIC     SELECT
# MAGIC         pickup_datetime,
# MAGIC         dropoff_datetime,
# MAGIC         trip_distance,
# MAGIC         fare_amount,
# MAGIC         pickup_zip,
# MAGIC         dropoff_zip,
# MAGIC         batch_id,
# MAGIC
# MAGIC         current_timestamp()
# MAGIC             AS processed_timestamp,
# MAGIC
# MAGIC         COALESCE(
# MAGIC             source_system,
# MAGIC             'NYC_TAXI'
# MAGIC         ) AS source_system
# MAGIC
# MAGIC     FROM workspace.nyc_taxi.source_trips
# MAGIC
# MAGIC     WHERE pickup_datetime IS NOT NULL
# MAGIC
# MAGIC       AND dropoff_datetime IS NOT NULL
# MAGIC
# MAGIC       AND dropoff_datetime > pickup_datetime
# MAGIC
# MAGIC       AND trip_distance IS NOT NULL
# MAGIC
# MAGIC       AND trip_distance > 0
# MAGIC
# MAGIC       AND fare_amount IS NOT NULL
# MAGIC
# MAGIC       AND fare_amount > 0
# MAGIC
# MAGIC ) AS source
# MAGIC
# MAGIC
# MAGIC ON  target.pickup_datetime
# MAGIC         = source.pickup_datetime
# MAGIC
# MAGIC AND target.dropoff_datetime
# MAGIC         = source.dropoff_datetime
# MAGIC
# MAGIC AND target.pickup_zip
# MAGIC         = source.pickup_zip
# MAGIC
# MAGIC AND target.dropoff_zip
# MAGIC         = source.dropoff_zip
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC
# MAGIC INSERT
# MAGIC (
# MAGIC     pickup_datetime,
# MAGIC     dropoff_datetime,
# MAGIC     trip_distance,
# MAGIC     fare_amount,
# MAGIC     pickup_zip,
# MAGIC     dropoff_zip,
# MAGIC     batch_id,
# MAGIC     processed_timestamp,
# MAGIC     source_system
# MAGIC )
# MAGIC
# MAGIC VALUES
# MAGIC (
# MAGIC     source.pickup_datetime,
# MAGIC     source.dropoff_datetime,
# MAGIC     source.trip_distance,
# MAGIC     source.fare_amount,
# MAGIC     source.pickup_zip,
# MAGIC     source.dropoff_zip,
# MAGIC     source.batch_id,
# MAGIC     source.processed_timestamp,
# MAGIC     source.source_system
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- STEP 3: LOAD / ENRICH GOLD
# MAGIC --
# MAGIC -- Silver is joined to the CURRENT ZIP dimension record.
# MAGIC --
# MAGIC -- Gold receives:
# MAGIC --   pickup_borough
# MAGIC --   pickup_zone_name
# MAGIC --   dimension_match_status
# MAGIC -- ============================================================
# MAGIC
# MAGIC MERGE INTO
# MAGIC     workspace.nyc_taxi.fact_taxi_trips_enriched AS target
# MAGIC
# MAGIC USING
# MAGIC (
# MAGIC     SELECT
# MAGIC
# MAGIC         s.pickup_datetime,
# MAGIC
# MAGIC         s.dropoff_datetime,
# MAGIC
# MAGIC         s.trip_distance,
# MAGIC
# MAGIC         s.fare_amount,
# MAGIC
# MAGIC         s.pickup_zip,
# MAGIC
# MAGIC         s.dropoff_zip,
# MAGIC
# MAGIC         s.batch_id,
# MAGIC
# MAGIC         s.processed_timestamp,
# MAGIC
# MAGIC         s.source_system,
# MAGIC
# MAGIC         d.borough
# MAGIC             AS pickup_borough,
# MAGIC
# MAGIC         d.zone_name
# MAGIC             AS pickup_zone_name,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN d.zipcode IS NOT NULL
# MAGIC                 THEN 'MATCHED'
# MAGIC
# MAGIC             ELSE 'UNMATCHED'
# MAGIC
# MAGIC         END AS dimension_match_status
# MAGIC
# MAGIC     FROM workspace.nyc_taxi.curated_trips s
# MAGIC
# MAGIC     LEFT JOIN workspace.nyc_taxi.dim_zipcode d
# MAGIC
# MAGIC         ON s.pickup_zip = d.zipcode
# MAGIC
# MAGIC        AND d.is_current = TRUE
# MAGIC
# MAGIC ) AS source
# MAGIC
# MAGIC
# MAGIC ON  target.pickup_datetime
# MAGIC         = source.pickup_datetime
# MAGIC
# MAGIC AND target.dropoff_datetime
# MAGIC         = source.dropoff_datetime
# MAGIC
# MAGIC AND target.pickup_zip
# MAGIC         = source.pickup_zip
# MAGIC
# MAGIC AND target.dropoff_zip
# MAGIC         = source.dropoff_zip
# MAGIC
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC
# MAGIC UPDATE SET
# MAGIC
# MAGIC     target.trip_distance
# MAGIC         = source.trip_distance,
# MAGIC
# MAGIC     target.fare_amount
# MAGIC         = source.fare_amount,
# MAGIC
# MAGIC     target.batch_id
# MAGIC         = source.batch_id,
# MAGIC
# MAGIC     target.processed_timestamp
# MAGIC         = source.processed_timestamp,
# MAGIC
# MAGIC     target.source_system
# MAGIC         = source.source_system,
# MAGIC
# MAGIC     target.pickup_borough
# MAGIC         = source.pickup_borough,
# MAGIC
# MAGIC     target.pickup_zone_name
# MAGIC         = source.pickup_zone_name,
# MAGIC
# MAGIC     target.dimension_match_status
# MAGIC         = source.dimension_match_status
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC
# MAGIC INSERT
# MAGIC (
# MAGIC     pickup_datetime,
# MAGIC     dropoff_datetime,
# MAGIC     trip_distance,
# MAGIC     fare_amount,
# MAGIC     pickup_zip,
# MAGIC     dropoff_zip,
# MAGIC     batch_id,
# MAGIC     processed_timestamp,
# MAGIC     source_system,
# MAGIC     pickup_borough,
# MAGIC     pickup_zone_name,
# MAGIC     dimension_match_status
# MAGIC )
# MAGIC
# MAGIC VALUES
# MAGIC (
# MAGIC     source.pickup_datetime,
# MAGIC     source.dropoff_datetime,
# MAGIC     source.trip_distance,
# MAGIC     source.fare_amount,
# MAGIC     source.pickup_zip,
# MAGIC     source.dropoff_zip,
# MAGIC     source.batch_id,
# MAGIC     source.processed_timestamp,
# MAGIC     source.source_system,
# MAGIC     source.pickup_borough,
# MAGIC     source.pickup_zone_name,
# MAGIC     source.dimension_match_status
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- STEP 4: PIPELINE AUDIT LOG
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT INTO workspace.nyc_taxi.pipeline_audit_log
# MAGIC (
# MAGIC     pipeline_name,
# MAGIC     run_timestamp,
# MAGIC     source_records,
# MAGIC     silver_records,
# MAGIC     gold_records,
# MAGIC     rejected_records,
# MAGIC     pipeline_status
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     'NYC_TAXI_ETL'
# MAGIC         AS pipeline_name,
# MAGIC
# MAGIC     current_timestamp()
# MAGIC         AS run_timestamp,
# MAGIC
# MAGIC
# MAGIC     -- Source count
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.source_trips
# MAGIC     )
# MAGIC         AS source_records,
# MAGIC
# MAGIC
# MAGIC     -- Silver count
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.curated_trips
# MAGIC     )
# MAGIC         AS silver_records,
# MAGIC
# MAGIC
# MAGIC     -- Gold count
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.fact_taxi_trips_enriched
# MAGIC     )
# MAGIC         AS gold_records,
# MAGIC
# MAGIC
# MAGIC     -- Quarantine count
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.rejected_trips
# MAGIC     )
# MAGIC         AS rejected_records,
# MAGIC
# MAGIC
# MAGIC     -- --------------------------------------------------------
# MAGIC     -- Determine pipeline status
# MAGIC     -- --------------------------------------------------------
# MAGIC
# MAGIC     CASE
# MAGIC
# MAGIC         WHEN
# MAGIC
# MAGIC         -- No invalid Silver records
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.curated_trips
# MAGIC
# MAGIC             WHERE pickup_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime <= pickup_datetime
# MAGIC
# MAGIC                OR trip_distance IS NULL
# MAGIC
# MAGIC                OR trip_distance <= 0
# MAGIC
# MAGIC                OR fare_amount IS NULL
# MAGIC
# MAGIC                OR fare_amount <= 0
# MAGIC         ) = 0
# MAGIC
# MAGIC
# MAGIC         AND
# MAGIC
# MAGIC
# MAGIC         -- No invalid Gold records
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.fact_taxi_trips_enriched
# MAGIC
# MAGIC             WHERE pickup_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime <= pickup_datetime
# MAGIC
# MAGIC                OR trip_distance IS NULL
# MAGIC
# MAGIC                OR trip_distance <= 0
# MAGIC
# MAGIC                OR fare_amount IS NULL
# MAGIC
# MAGIC                OR fare_amount <= 0
# MAGIC         ) = 0
# MAGIC
# MAGIC
# MAGIC         AND
# MAGIC
# MAGIC
# MAGIC         -- Silver and Gold reconcile
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.curated_trips
# MAGIC         )
# MAGIC
# MAGIC         =
# MAGIC
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.fact_taxi_trips_enriched
# MAGIC         )
# MAGIC
# MAGIC
# MAGIC         THEN 'SUCCESS'
# MAGIC
# MAGIC
# MAGIC         ELSE 'FAILED'
# MAGIC
# MAGIC
# MAGIC     END AS pipeline_status;
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- STEP 5: CREATE / REFRESH PIPELINE HEALTH VIEW
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW
# MAGIC workspace.nyc_taxi.vw_pipeline_health
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC
# MAGIC     -- ========================================================
# MAGIC     -- Record counts
# MAGIC     -- ========================================================
# MAGIC
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.source_trips
# MAGIC     )
# MAGIC         AS source_records,
# MAGIC
# MAGIC
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.curated_trips
# MAGIC     )
# MAGIC         AS silver_records,
# MAGIC
# MAGIC
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.fact_taxi_trips_enriched
# MAGIC     )
# MAGIC         AS gold_records,
# MAGIC
# MAGIC
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.rejected_trips
# MAGIC     )
# MAGIC         AS rejected_records,
# MAGIC
# MAGIC
# MAGIC     -- ========================================================
# MAGIC     -- Invalid Silver
# MAGIC     -- ========================================================
# MAGIC
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.curated_trips
# MAGIC
# MAGIC         WHERE pickup_datetime IS NULL
# MAGIC
# MAGIC            OR dropoff_datetime IS NULL
# MAGIC
# MAGIC            OR dropoff_datetime <= pickup_datetime
# MAGIC
# MAGIC            OR trip_distance IS NULL
# MAGIC
# MAGIC            OR trip_distance <= 0
# MAGIC
# MAGIC            OR fare_amount IS NULL
# MAGIC
# MAGIC            OR fare_amount <= 0
# MAGIC     )
# MAGIC         AS invalid_silver_records,
# MAGIC
# MAGIC
# MAGIC     -- ========================================================
# MAGIC     -- Invalid Gold
# MAGIC     -- ========================================================
# MAGIC
# MAGIC     (
# MAGIC         SELECT COUNT(*)
# MAGIC
# MAGIC         FROM workspace.nyc_taxi.fact_taxi_trips_enriched
# MAGIC
# MAGIC         WHERE pickup_datetime IS NULL
# MAGIC
# MAGIC            OR dropoff_datetime IS NULL
# MAGIC
# MAGIC            OR dropoff_datetime <= pickup_datetime
# MAGIC
# MAGIC            OR trip_distance IS NULL
# MAGIC
# MAGIC            OR trip_distance <= 0
# MAGIC
# MAGIC            OR fare_amount IS NULL
# MAGIC
# MAGIC            OR fare_amount <= 0
# MAGIC     )
# MAGIC         AS invalid_gold_records,
# MAGIC
# MAGIC
# MAGIC     -- ========================================================
# MAGIC     -- Overall data health
# MAGIC     -- ========================================================
# MAGIC
# MAGIC     CASE
# MAGIC
# MAGIC         WHEN
# MAGIC
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.curated_trips
# MAGIC
# MAGIC             WHERE pickup_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime <= pickup_datetime
# MAGIC
# MAGIC                OR trip_distance IS NULL
# MAGIC
# MAGIC                OR trip_distance <= 0
# MAGIC
# MAGIC                OR fare_amount IS NULL
# MAGIC
# MAGIC                OR fare_amount <= 0
# MAGIC         ) = 0
# MAGIC
# MAGIC
# MAGIC         AND
# MAGIC
# MAGIC
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.fact_taxi_trips_enriched
# MAGIC
# MAGIC             WHERE pickup_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime IS NULL
# MAGIC
# MAGIC                OR dropoff_datetime <= pickup_datetime
# MAGIC
# MAGIC                OR trip_distance IS NULL
# MAGIC
# MAGIC                OR trip_distance <= 0
# MAGIC
# MAGIC                OR fare_amount IS NULL
# MAGIC
# MAGIC                OR fare_amount <= 0
# MAGIC         ) = 0
# MAGIC
# MAGIC
# MAGIC         AND
# MAGIC
# MAGIC
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.curated_trips
# MAGIC         )
# MAGIC
# MAGIC         =
# MAGIC
# MAGIC         (
# MAGIC             SELECT COUNT(*)
# MAGIC
# MAGIC             FROM workspace.nyc_taxi.fact_taxi_trips_enriched
# MAGIC         )
# MAGIC
# MAGIC
# MAGIC         THEN 'HEALTHY'
# MAGIC
# MAGIC
# MAGIC         ELSE 'UNHEALTHY'
# MAGIC
# MAGIC
# MAGIC     END AS health_status;
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- STEP 6: FINAL PIPELINE VALIDATION
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     source_records,
# MAGIC
# MAGIC     silver_records,
# MAGIC
# MAGIC     gold_records,
# MAGIC
# MAGIC     rejected_records,
# MAGIC
# MAGIC     invalid_silver_records,
# MAGIC
# MAGIC     invalid_gold_records,
# MAGIC
# MAGIC     health_status
# MAGIC
# MAGIC FROM workspace.nyc_taxi.vw_pipeline_health;