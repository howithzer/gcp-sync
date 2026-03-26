"""
golden_glue_transform.py
========================
Golden Image Glue Job — Metadata-Driven Data Factory
Handles RAW (Iceberg JSON) → STD (Staging) → CUR (Target) promotion
for a single topic per execution.

Parameters accepted via getResolvedOptions:
  --topic_name          : Topic identifier, e.g. "payments_v2"
  --batch_id            : UUID for this run (correlation ID for all logs)
  --starting_snapshot   : Iceberg snapshot ID to read FROM (exclusive)
  --ending_snapshot     : Iceberg snapshot ID to read TO (inclusive)
  --full_reload         : "true" | "false"  — triggers OVERWRITE instead of MERGE
  --target_layer        : "STD" | "CUR"     — which table layer this job writes to
  --job_run_id          : Injected by Glue automatically (used in CW metrics)

Design Principles:
  - Idempotent: skips write if batch_id already committed to target
  - Resilient:  malformed JSON rows go to quarantine side-table, never fail the job
  - Observable: structured JSON logs emit CloudWatch custom metrics
  - Correlation: batch_id propagates through every log line and metric dimension
"""

import sys
import json
import uuid
import time
import datetime
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, LongType
from pyspark.sql.utils import AnalysisException

# ─────────────────────────────────────────────
# 0. Bootstrap: args, contexts, config
# ─────────────────────────────────────────────

REQUIRED_ARGS = [
    "JOB_NAME",
    "topic_name",
    "batch_id",
    "starting_snapshot",
    "ending_snapshot",
    "target_layer",       # STD or CUR
]
OPTIONAL_DEFAULTS = {
    "full_reload": "false",
    "job_run_id": "local",
}

args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
# Merge optional args with defaults — getResolvedOptions raises if key is missing
for key, default in OPTIONAL_DEFAULTS.items():
    if f"--{key}" in sys.argv:
        args.update(getResolvedOptions(sys.argv, [key]))
    else:
        args[key] = default

TOPIC          = args["topic_name"]
BATCH_ID       = args["batch_id"]
START_SNAP     = int(args["starting_snapshot"])
END_SNAP       = int(args["ending_snapshot"])
TARGET_LAYER   = args["target_layer"].upper()        # "STD" or "CUR"
FULL_RELOAD    = args["full_reload"].lower() == "true"
JOB_NAME       = args["JOB_NAME"]

# Naming convention: all resources follow [TopicName]_[Resource]
RAW_TABLE       = f"{TOPIC}_raw"
STD_TABLE       = f"{TOPIC}_std"     # Staging
CUR_TABLE       = f"{TOPIC}_cur"     # Target/Curated
QUARANTINE_TABLE = f"{TOPIC}_quarantine"
TARGET_TABLE    = STD_TABLE if TARGET_LAYER == "STD" else CUR_TABLE
SOURCE_TABLE    = RAW_TABLE if TARGET_LAYER == "STD" else STD_TABLE

sc         = SparkContext()
glueContext = GlueContext(sc)
spark      = glueContext.spark_session
job        = Job(glueContext)
job.init(JOB_NAME, args)

cloudwatch = boto3.client("cloudwatch")
dynamodb   = boto3.resource("dynamodb")

# ─────────────────────────────────────────────
# 1. Structured logging helper
#    All log lines are JSON so CloudWatch Insights
#    can parse them without a custom parser.
# ─────────────────────────────────────────────

def log(level: str, event: str, **extra):
    """
    Emit a structured JSON log line.
    Every line includes batch_id for cross-layer correlation.
    """
    record = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "level":     level,
        "job_name":  JOB_NAME,
        "topic":     TOPIC,
        "batch_id":  BATCH_ID,
        "event":     event,
        **extra,
    }
    # Glue captures stdout to CloudWatch Logs automatically
    print(json.dumps(record))


# ─────────────────────────────────────────────
# 2. CloudWatch Custom Metrics
#    Namespace: DataFactory/<TopicName>
#    Dimensions: Topic + BatchId (for drill-down)
# ─────────────────────────────────────────────

def emit_metric(metric_name: str, value: float, unit: str = "Count"):
    """
    Publish a single custom metric to CloudWatch.
    Failures are logged but never raise — observability must not break the job.
    """
    try:
        cloudwatch.put_metric_data(
            Namespace=f"DataFactory/{TOPIC}",
            MetricData=[{
                "MetricName": metric_name,
                "Dimensions": [
                    {"Name": "Topic",   "Value": TOPIC},
                    {"Name": "BatchId", "Value": BATCH_ID},
                    {"Name": "Layer",   "Value": TARGET_LAYER},
                ],
                "Value":     value,
                "Unit":      unit,
                "Timestamp": datetime.datetime.utcnow(),
            }],
        )
    except Exception as exc:
        log("WARN", "metric_emit_failed", metric=metric_name, error=str(exc))


# ─────────────────────────────────────────────
# 3. Idempotency guard
#    Read the target Iceberg table's latest committed
#    batch_id column. If it matches, skip write.
# ─────────────────────────────────────────────

def batch_already_committed(target_table: str, batch_id: str) -> bool:
    """
    Returns True if this batch_id was already written to target_table.
    Uses Iceberg metadata — no full scan, just a filter on the lineage column.
    """
    try:
        result = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM   {target_table}
            WHERE  _batch_id = '{batch_id}'
            LIMIT  1
        """)
        count = result.collect()[0]["cnt"]
        if count > 0:
            log("INFO", "idempotency_skip",
                target_table=target_table,
                batch_id=batch_id,
                existing_records=count)
            return True
    except AnalysisException:
        # Table may not exist yet on first run — that is fine
        log("INFO", "idempotency_table_not_found", target_table=target_table)
    return False


# ─────────────────────────────────────────────
# 4. Iceberg incremental read
#    Reads ONLY the delta between two snapshot IDs.
#    This is the core of the HWM / offset mechanism.
# ─────────────────────────────────────────────

def read_incremental(source_table: str, start_snap: int, end_snap: int):
    """
    Use Iceberg's incremental scan API to return only rows
    appended between start_snap (exclusive) and end_snap (inclusive).
    """
    log("INFO", "incremental_read_start",
        source_table=source_table,
        start_snapshot=start_snap,
        end_snapshot=end_snap)

    df = spark.read \
        .format("iceberg") \
        .option("start-snapshot-id", str(start_snap)) \
        .option("end-snapshot-id",   str(end_snap)) \
        .load(source_table)

    count = df.count()
    log("INFO", "incremental_read_complete",
        source_table=source_table,
        record_count=count)
    return df, count


# ─────────────────────────────────────────────
# 5. Full reload read
#    Used when Nuclear_Option = true.
#    Reads the entire current snapshot of source.
# ─────────────────────────────────────────────

def read_full(source_table: str):
    log("WARN", "full_reload_initiated",
        source_table=source_table,
        batch_id=BATCH_ID)
    df = spark.read.format("iceberg").load(source_table)
    count = df.count()
    log("INFO", "full_reload_read_complete",
        source_table=source_table,
        record_count=count)
    return df, count


# ─────────────────────────────────────────────
# 6. Quarantine pattern
#    Any row where the JSON payload cannot be
#    parsed or schema-validated is routed here
#    instead of crashing the job.
# ─────────────────────────────────────────────

# Quarantine schema — minimal: raw payload + reason + lineage
QUARANTINE_SCHEMA = StructType([
    StructField("raw_payload",    StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("topic",          StringType(), False),
    StructField("batch_id",       StringType(), False),
    StructField("ingested_at",    StringType(), False),
])

def route_quarantine(df, payload_col: str = "payload"):
    """
    Splits df into (clean_df, quarantine_df).
    Rows where `payload_col` cannot be parsed as valid JSON are quarantined.
    Uses Spark's `from_json` with a permissive mode — nulls indicate bad rows.

    For STD layer: payload is raw JSON string from Firehose.
    For CUR layer: source is already typed — quarantine catches null primary keys.
    """
    if TARGET_LAYER == "STD":
        # Try parsing the JSON payload column
        parsed = df.withColumn(
            "_parsed_check",
            F.get_json_object(F.col(payload_col), "$")
        )
        good_df = parsed.filter(F.col("_parsed_check").isNotNull()) \
                        .drop("_parsed_check")
        bad_df  = parsed.filter(F.col("_parsed_check").isNull()) \
                        .drop("_parsed_check")

        quarantine_rows = bad_df.select(
            F.col(payload_col).cast(StringType()).alias("raw_payload"),
            F.lit("invalid_json").alias("failure_reason"),
            F.lit(TOPIC).alias("topic"),
            F.lit(BATCH_ID).alias("batch_id"),
            F.current_timestamp().cast(StringType()).alias("ingested_at"),
        )
    else:
        # CUR layer: quarantine rows missing primary key
        # In production, replace "id" with the actual PK from topic config
        pk_col = "_record_id"
        good_df = df.filter(F.col(pk_col).isNotNull())
        bad_df  = df.filter(F.col(pk_col).isNull())

        quarantine_rows = bad_df.select(
            F.to_json(F.struct("*")).alias("raw_payload"),
            F.lit("null_primary_key").alias("failure_reason"),
            F.lit(TOPIC).alias("topic"),
            F.lit(BATCH_ID).alias("batch_id"),
            F.current_timestamp().cast(StringType()).alias("ingested_at"),
        )

    quarantine_count = bad_df.count()
    if quarantine_count > 0:
        log("WARN", "quarantine_records_detected",
            quarantine_count=quarantine_count,
            target_quarantine_table=QUARANTINE_TABLE)
        emit_metric("QuarantineRecords", quarantine_count)
        # Write quarantine rows — append only, never overwrite history
        quarantine_rows.writeTo(QUARANTINE_TABLE) \
            .option("write.format.default", "parquet") \
            .append()

    return good_df, quarantine_count


# ─────────────────────────────────────────────
# 7. STD transformation
#    Parses the RAW JSON payload into typed columns.
#    Injects lineage columns (_batch_id, _topic, _ingested_at).
#    CDE columns are tagged via metadata — not masked here, handled at CUR.
# ─────────────────────────────────────────────

def transform_to_std(df):
    """
    Expand the JSON payload into a typed Spark DataFrame.
    The actual schema comes from the Iceberg table DDL (already created
    by the Stamper Lambda at deploy time).  We use from_json against the
    live target schema to stay in sync automatically.
    """
    log("INFO", "transform_std_start")

    # Read the current STD table schema as the target schema
    try:
        target_schema = spark.table(STD_TABLE).schema
    except AnalysisException:
        raise RuntimeError(
            f"STD table {STD_TABLE} does not exist. "
            "Ensure Stamper Lambda ran DDL before triggering this job."
        )

    # Expand JSON payload into typed columns
    expanded = df.withColumn(
        "_struct",
        F.from_json(F.col("payload"), target_schema)
    ).select("_struct.*")

    # Inject lineage columns
    enriched = expanded \
        .withColumn("_batch_id",     F.lit(BATCH_ID)) \
        .withColumn("_topic",        F.lit(TOPIC)) \
        .withColumn("_ingested_at",  F.current_timestamp()) \
        .withColumn("_source_layer", F.lit("RAW"))

    log("INFO", "transform_std_complete")
    return enriched


# ─────────────────────────────────────────────
# 8. CUR transformation
#    Reads from STD, applies business curation logic.
#    Partition pruning: only process the current batch slice.
#    In production, custom logic lives in a per-topic override script;
#    this golden image handles the default pass-through + dedup case.
# ─────────────────────────────────────────────

def transform_to_cur(df):
    """
    Default CUR transformation: deduplication by _record_id, keeping latest.
    Per-topic custom logic is handled by the overriding script — this function
    is never called when a custom_script_path was provided to the Stamper Lambda.
    """
    log("INFO", "transform_cur_start")

    # Dedup: keep the most recent record per natural key within the batch
    from pyspark.sql.window import Window
    w = Window.partitionBy("_record_id").orderBy(F.col("_ingested_at").desc())
    deduped = df.withColumn("_row_rank", F.row_number().over(w)) \
                .filter(F.col("_row_rank") == 1) \
                .drop("_row_rank") \
                .withColumn("_source_layer", F.lit("STD")) \
                .withColumn("_cur_batch_id", F.lit(BATCH_ID))

    log("INFO", "transform_cur_complete")
    return deduped


# ─────────────────────────────────────────────
# 9. Write: MERGE (incremental) or OVERWRITE (full reload)
#    MERGE uses _record_id as the join key.
#    OVERWRITE replaces the entire target table partition.
# ─────────────────────────────────────────────

def write_to_target(df, target_table: str):
    if FULL_RELOAD:
        log("WARN", "write_mode_overwrite",
            target_table=target_table,
            batch_id=BATCH_ID)
        df.writeTo(target_table) \
          .using("iceberg") \
          .overwritePartitions()
    else:
        log("INFO", "write_mode_merge",
            target_table=target_table,
            batch_id=BATCH_ID)
        # Iceberg MERGE via Spark SQL — handles upsert on _record_id
        df.createOrReplaceTempView("_incoming")
        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING _incoming          AS source
            ON target._record_id = source._record_id
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    log("INFO", "write_complete", target_table=target_table)


# ─────────────────────────────────────────────
# 10. Main execution
# ─────────────────────────────────────────────

def main():
    run_start = time.time()
    log("INFO", "job_start",
        source_table=SOURCE_TABLE,
        target_table=TARGET_TABLE,
        full_reload=FULL_RELOAD,
        starting_snapshot=START_SNAP,
        ending_snapshot=END_SNAP)

    # ── Idempotency: bail early if already done ──
    if not FULL_RELOAD and batch_already_committed(TARGET_TABLE, BATCH_ID):
        log("INFO", "job_skipped_idempotent")
        emit_metric("IdempotentSkip", 1)
        job.commit()
        return

    # ── Read source data ──
    if FULL_RELOAD:
        source_df, raw_count = read_full(SOURCE_TABLE)
    else:
        source_df, raw_count = read_incremental(SOURCE_TABLE, START_SNAP, END_SNAP)

    if raw_count == 0:
        log("INFO", "job_no_data", source_table=SOURCE_TABLE)
        emit_metric("RecordsProcessed", 0)
        job.commit()
        return

    # ── Quarantine split ──
    clean_df, quarantine_count = route_quarantine(source_df)
    clean_count = raw_count - quarantine_count

    if clean_count == 0:
        log("WARN", "job_all_quarantined", raw_count=raw_count)
        emit_metric("RecordsProcessed", 0)
        emit_metric("QuarantineRate",   100.0, unit="Percent")
        job.commit()
        return

    # ── Transform ──
    if TARGET_LAYER == "STD":
        transformed_df = transform_to_std(clean_df)
    else:
        transformed_df = transform_to_cur(clean_df)

    # ── Write ──
    write_to_target(transformed_df, TARGET_TABLE)

    # ── Emit metrics ──
    elapsed = time.time() - run_start
    emit_metric("RecordsProcessed", float(clean_count))
    emit_metric("ProcessingTime",   elapsed,               unit="Seconds")
    emit_metric("QuarantineRecords", float(quarantine_count))
    if raw_count > 0:
        emit_metric("QuarantineRate",
                    float(quarantine_count) / raw_count * 100,
                    unit="Percent")

    log("INFO", "job_complete",
        records_processed=clean_count,
        quarantine_count=quarantine_count,
        processing_time_seconds=round(elapsed, 2),
        target_table=TARGET_TABLE,
        write_mode="OVERWRITE" if FULL_RELOAD else "MERGE")

    job.commit()


if __name__ == "__main__":
    main()
