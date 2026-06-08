import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

AMS_TZ = ZoneInfo("Europe/Amsterdam")

VOLUME_INPUT = "/Volumes/mock_vdb/default/mock/input"
VOLUME_OUTPUT = "/Volumes/mock_vdb/default/mock/output/delta"
SCHEMA_DIR = f"{VOLUME_INPUT}/schemas"
TABLE_METADATA_PATH = f"{SCHEMA_DIR}/table_metadata.json"

LOG_TABLE = "mock_vdb.ops.silver_mock_run_log"

LOG_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("table_id", T.IntegerType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("started_at_utc", T.TimestampType(), True),
        T.StructField("finished_at_utc", T.TimestampType(), True),
        T.StructField("from_date", T.StringType(), True),
        T.StructField("to_date", T.StringType(), True),
        T.StructField("row_count", T.LongType(), True),
        T.StructField("partition_count", T.IntegerType(), True),
        T.StructField("partition_dates", T.StringType(), True),
        T.StructField("is_backfill", T.BooleanType(), True),
        T.StructField("write_mode", T.StringType(), True),
        T.StructField("write_duration_seconds", T.DoubleType(), True),
        T.StructField("error_message", T.StringType(), True),
    ]
)

_DTYPE_MAP: dict[str, T.DataType] = {
    "Int8": T.ByteType(),
    "Int16": T.ShortType(),
    "Int32": T.IntegerType(),
    "Int64": T.LongType(),
    "UInt8": T.ShortType(),
    "UInt16": T.IntegerType(),
    "UInt32": T.LongType(),
    "UInt64": T.LongType(),
    "Float32": T.FloatType(),
    "Float64": T.DoubleType(),
    "Utf8": T.StringType(),
    "String": T.StringType(),
    "Boolean": T.BooleanType(),
    "Date": T.DateType(),
    "Datetime": T.TimestampType(),
}


def _parse_dtype(name: str) -> T.DataType:
    try:
        return _DTYPE_MAP[name]
    except KeyError as exc:
        raise ValueError(f"Unsupported dtype in schema JSON: {name!r}") from exc


def _read_json_spark(spark: SparkSession, path: str) -> dict:
    text = "\n".join(row.value for row in spark.read.text(path).collect())
    return json.loads(text)


def load_table_metadata(spark: SparkSession) -> dict:
    return _read_json_spark(spark, TABLE_METADATA_PATH)


def get_default_table_ids(spark: SparkSession) -> list[int]:
    metadata = load_table_metadata(spark)
    return metadata["default_table_ids"]


def get_table_metadata(spark: SparkSession, table_id: int) -> dict:
    metadata = load_table_metadata(spark)
    return metadata["tables"].get(str(table_id), {})


def load_schema_config(spark: SparkSession, table_id: int) -> dict:
    base_raw = _read_json_spark(spark, f"{SCHEMA_DIR}/base_schema.json")
    table_raw = _read_json_spark(spark, f"{SCHEMA_DIR}/table_{table_id}.json")

    base_schema = {col: _parse_dtype(dt) for col, dt in base_raw.items()}
    table_schema = {
        col: _parse_dtype(dt) for col, dt in table_raw.get("schema", {}).items()
    }

    return {
        "schema": {**base_schema, **table_schema},
        "numeric_columns": table_raw.get("numeric_columns", []),
        "flag_columns": table_raw.get("flag_columns", []),
        "float_columns": table_raw.get("float_columns", []),
        "datetime_columns": table_raw.get("datetime_columns", []),
        "merge_keys": table_raw.get("merge_keys", []),
    }


def cast_dataframe(df: DataFrame, schema: dict[str, T.DataType]) -> DataFrame:
    existing = set(df.columns)

    for col_name, dtype in schema.items():
        if col_name not in existing:
            continue

        current_dtype = df.schema[col_name].dataType

        if isinstance(dtype, T.TimestampType) and isinstance(
            current_dtype, T.StringType
        ):
            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
                    F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                    F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
                ).cast(T.TimestampType()),
            )
        elif isinstance(dtype, T.DateType) and isinstance(current_dtype, T.StringType):
            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.to_date(F.col(col_name), "yyyy-MM-dd"),
                    F.to_date(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                ).cast(T.DateType()),
            )
        elif type(current_dtype) is not type(dtype):
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

    return df


def normalize_partition_date(df: DataFrame) -> DataFrame:
    if "PartitionDate" not in df.columns:
        if "date" in df.columns:
            df = df.withColumn("PartitionDate", F.col("date"))
        else:
            raise ValueError(
                "Expected column 'PartitionDate' not present in DataFrame."
            )

    df = df.withColumn(
        "PartitionDate",
        F.coalesce(
            F.to_date(F.col("PartitionDate"), "yyyy-MM-dd"),
            F.to_date(F.col("PartitionDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ).cast(T.StringType()),
    )

    df = df.withColumn("year", F.year(F.col("PartitionDate").cast(T.DateType())))
    df = df.withColumn("month", F.month(F.col("PartitionDate").cast(T.DateType())))

    return df.filter(F.col("PartitionDate").isNotNull())


def add_row_metadata(
    spark: SparkSession,
    df: DataFrame,
    table_id: int,
) -> DataFrame:
    table_meta = get_table_metadata(spark, table_id)
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    return (
        df.withColumn("source_table_id", F.lit(table_id))
        .withColumn("source_system", F.lit(table_meta.get("source_system", "unknown")))
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("ingested_at_utc", F.current_timestamp())
    )


def _build_replace_where(partition_dates: list[str]) -> str:
    quoted = ", ".join(f"'{d}'" for d in partition_dates)
    return f"PartitionDate IN ({quoted})"


def write_delta_merge(
    spark: SparkSession,
    df: DataFrame,
    table_id: int,
    partition_dates: list[str],
    merge_keys: list[str],
) -> None:
    """Upsert ``df`` into the Delta table using MERGE.

    Match condition:
    - ``PartitionDate`` is always included for partition pruning.
    - ``merge_keys`` (from the schema JSON) identify the row uniquely.

    If a row matches  → UPDATE all non-key columns.
    If no match found → INSERT the new row.
    """
    output_path = f"{VOLUME_OUTPUT}/table_{table_id}"
    partition_cols = ["year", "month", "PartitionDate"]

    if not merge_keys:
        raise ValueError(
            f"table_{table_id}: merge_keys is empty — cannot perform MERGE. "
            "Add merge_keys to the table schema JSON or use write_mode='overwrite'."
        )

    # Validate that all merge key columns are present in the DataFrame.
    missing = [k for k in merge_keys if k not in df.columns]
    if missing:
        raise ValueError(
            f"table_{table_id}: merge_keys column(s) {missing} not found in DataFrame."
        )

    if not DeltaTable.isDeltaTable(spark, output_path):
        # First write — plain overwrite to create the table.
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .partitionBy(*partition_cols)
            .save(output_path)
        )
        print(
            f"  [OK-INIT] table_{table_id} → {output_path}  "
            f"({df.count():,} rows, {len(partition_dates)} partitions) [initial write]"
        )
        return

    delta_table = DeltaTable.forPath(spark, output_path)

    # Build match condition: partition pruning first, then business key.
    key_cols = ["PartitionDate"] + merge_keys
    match_condition = " AND ".join(f"target.{col} = source.{col}" for col in key_cols)

    # All non-key, non-partition columns are updated on match.
    partition_meta_cols = {"year", "month", "PartitionDate"}
    update_cols = {
        col: F.col(f"source.{col}")
        for col in df.columns
        if col not in set(merge_keys) | partition_meta_cols
    }

    (
        delta_table.alias("target")
        .merge(df.alias("source"), match_condition)
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        f"  [OK-MERGE] table_{table_id} → {output_path}  "
        f"({df.count():,} rows, {len(partition_dates)} partitions)"
    )


def write_delta_replace_partitions(
    spark: SparkSession,
    df: DataFrame,
    table_id: int,
    partition_dates: list[str],
) -> None:
    output_path = f"{VOLUME_OUTPUT}/table_{table_id}"
    partition_cols = ["year", "month", "PartitionDate"]

    if DeltaTable.isDeltaTable(spark, output_path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", _build_replace_where(partition_dates))
            .option("mergeSchema", "true")
            .partitionBy(*partition_cols)
            .save(output_path)
        )
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .partitionBy(*partition_cols)
            .save(output_path)
        )

    print(
        f"  [OK] table_{table_id} → {output_path}  ({df.count():,} rows, {len(partition_dates)} partitions)"
    )


def log_run_event(
    spark: SparkSession,
    log_table: str,
    records: list[dict],
) -> None:
    log_df = spark.createDataFrame(records, schema=LOG_SCHEMA)
    (log_df.write.format("delta").mode("append").saveAsTable(log_table))


def _generate_date_range(from_date: date, to_date: date) -> list[str]:
    """Return every calendar date in [from_date, to_date] as ISO-8601 strings."""
    result: list[str] = []
    current = from_date
    while current <= to_date:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def get_existing_partition_dates(spark: SparkSession, table_id: int) -> set[str]:
    """Return the set of PartitionDate values already written to the Delta table."""
    output_path = f"{VOLUME_OUTPUT}/table_{table_id}"
    try:
        df = spark.read.format("delta").load(output_path)
        return {
            row[0]
            for row in df.select("PartitionDate").distinct().collect()
            if row[0] is not None
        }
    except AnalysisException:
        return set()


def get_successfully_logged_dates(spark: SparkSession, table_id: int) -> set[str]:
    """Return all partition dates that were recorded as SUCCESS in the run log.

    Strategy (in order of reliability):
    1. Parse per-date info from ``partition_dates`` column where it is populated.
    2. For SUCCESS log entries whose ``partition_dates`` is NULL (e.g. old runs
       written before the column existed), fall back to the actual Delta partitions.
       Delta writes are atomic, so an existing partition is a complete write.
    3. If the log table does not exist yet, return an empty set (nothing is safe
       to skip yet).
    """
    try:
        log_df = spark.read.table(LOG_TABLE)
    except AnalysisException:
        return set()

    success_df = log_df.filter(
        (F.col("table_id") == table_id) & (F.col("status") == "SUCCESS")
    )

    # Cheap count to avoid further work when there are no success entries at all.
    if success_df.limit(1).count() == 0:
        return set()

    successful_dates: set[str] = set()

    # -- Entries WITH partition_dates populated ---------------------------------
    with_dates_rows = (
        success_df.filter(F.col("partition_dates").isNotNull())
        .select("partition_dates")
        .collect()
    )
    for row in with_dates_rows:
        if row[0]:
            successful_dates.update(d.strip() for d in row[0].split(",") if d.strip())

    # -- Entries WITHOUT partition_dates (legacy / missing) --------------------
    # Fall back to whatever is already in Delta for this table; if the partition
    # is there, the Delta write completed atomically.
    has_legacy = (
        success_df.filter(F.col("partition_dates").isNull()).limit(1).count()
    ) > 0

    if has_legacy:
        successful_dates |= get_existing_partition_dates(spark, table_id)

    return successful_dates


def process_table(
    spark: SparkSession,
    table_id: int,
    from_date: Optional[date],
    to_date: Optional[date],
    dates_filter: Optional[list[str]] = None,
    write_mode: str = "overwrite",
) -> dict:
    print(f"\n{'=' * 72}")
    print(f"Processing table {table_id}")

    schema_config = load_schema_config(spark, table_id)
    final_schema = schema_config["schema"]
    print(f"  Schema cols  : {len(final_schema)}")

    input_path = f"{VOLUME_INPUT}/table_{table_id}"
    df = spark.read.option("basePath", input_path).parquet(input_path)
    print(f"  Raw rows     : {df.count():,}")

    if "date" in df.columns:
        if dates_filter is not None:
            df = df.filter(F.col("date").isin(dates_filter))
        else:
            if from_date:
                df = df.filter(F.col("date") >= F.lit(from_date.isoformat()))
            if to_date:
                df = df.filter(F.col("date") <= F.lit(to_date.isoformat()))

    df = cast_dataframe(df, final_schema)
    df = normalize_partition_date(df)
    df = add_row_metadata(spark, df, table_id)
    df = df.filter(F.col("PartitionDate").isNotNull())

    row_count = df.count()
    print(f"  Rows after filter : {row_count:,}")

    partition_dates = [
        row[0]
        for row in df.select("PartitionDate")
        .distinct()
        .orderBy("PartitionDate")
        .collect()
    ]

    if not partition_dates:
        print(
            f"  [SKIPPED] No partitions found for table {table_id} in the requested range."
        )
        return {
            "status": "SKIPPED",
            "row_count": 0,
            "partition_dates": [],
            "partition_count": 0,
            "write_mode": write_mode,
            "write_duration_seconds": 0.0,
        }

    print(f"  Partitions   : {partition_dates}")
    write_start = datetime.now(timezone.utc)

    if write_mode == "merge":
        merge_keys = schema_config.get("merge_keys", [])
        write_delta_merge(spark, df, table_id, partition_dates, merge_keys)
    else:
        write_delta_replace_partitions(spark, df, table_id, partition_dates)

    write_duration = (datetime.now(timezone.utc) - write_start).total_seconds()
    print(f"  Write duration : {write_duration:.1f}s  (mode={write_mode})")

    return {
        "status": "SUCCESS",
        "row_count": row_count,
        "partition_dates": partition_dates,
        "partition_count": len(partition_dates),
        "write_mode": write_mode,
        "write_duration_seconds": write_duration,
    }


def verify_outputs(spark: SparkSession, table_ids: list[int]) -> None:
    print("\n--- Verification ---")
    for table_id in table_ids:
        path = f"{VOLUME_OUTPUT}/table_{table_id}"
        try:
            df = spark.read.format("delta").load(path)
            row_count = df.count()
            dates = [
                r[0]
                for r in df.select("PartitionDate")
                .distinct()
                .orderBy("PartitionDate")
                .collect()
            ]
            print(f"  table_{table_id}: {row_count:,} rows  |  dates: {dates}")
        except AnalysisException as exc:
            print(f"  table_{table_id}: [NOT FOUND] {exc}")


def backfill(
    spark: SparkSession,
    table_ids: list[int],
    from_date: date,
    to_date: date,
    write_mode: str = "merge",
    force_table_ids: Optional[list[int]] = None,
) -> None:
    """Process only dates in [from_date, to_date] that are missing or did not succeed.

    The run log is the single source of truth: a date is skipped only when it
    appears in a SUCCESS log entry for that table.  This avoids reading Delta
    partition metadata (which can be stale after manual file deletions).

    Args:
        force_table_ids: Table IDs that bypass the skip check entirely and are
            always reprocessed — useful after manually deleting output files.
    """
    run_id = str(uuid.uuid4())
    print(f"Backfill Run ID  : {run_id}")
    print(f"Started UTC      : {datetime.now(timezone.utc).isoformat()}")
    print(f"Tables           : {table_ids}")
    print(f"Date range       : {from_date} → {to_date}")

    all_dates = _generate_date_range(from_date, to_date)
    print(f"Total dates in range : {len(all_dates)}")

    for table_id in table_ids:
        print(f"\n{'=' * 72}")
        print(f"Backfill: table {table_id}")

        # The log is the sole source of truth. If the table is in force_table_ids,
        # skip the check entirely and reprocess all dates.
        # Also force overwrite mode: MERGE reads the target table, which fails
        # when data files have been manually deleted. Overwrite (replaceWhere)
        # only writes a new Delta commit and does not read existing files.
        is_forced = bool(force_table_ids and table_id in force_table_ids)
        if is_forced:
            print(f"  [FORCED] Skipping existence check for table {table_id}. Using overwrite.")
            dates_to_process = list(all_dates)
            effective_write_mode = "overwrite"
        else:
            logged_success_dates = get_successfully_logged_dates(spark, table_id)
            dates_to_process = [d for d in all_dates if d not in logged_success_dates]
            effective_write_mode = write_mode

        print(f"  Dates in range   : {len(all_dates)}")
        print(f"  Already OK       : {len(all_dates) - len(dates_to_process)}")
        print(f"  To process       : {len(dates_to_process)}")

        if not dates_to_process:
            print(
                f"  [SKIPPED] All dates already processed successfully for table {table_id}."
            )
            continue

        started_at = datetime.now(timezone.utc)

        try:
            result = process_table(
                spark,
                table_id,
                from_date=None,
                to_date=None,
                dates_filter=dates_to_process,
                write_mode=effective_write_mode,
            )
            finished_at = datetime.now(timezone.utc)

            log_data = [
                {
                    "run_id": run_id,
                    "table_id": table_id,
                    "status": result["status"],
                    "started_at_utc": started_at,
                    "finished_at_utc": finished_at,
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat(),
                    "row_count": result["row_count"],
                    "partition_count": result["partition_count"],
                    "partition_dates": ",".join(result["partition_dates"]),
                    "is_backfill": True,
                    "write_mode": result["write_mode"],
                    "write_duration_seconds": result["write_duration_seconds"],
                    "error_message": None,
                }
            ]
            log_run_event(spark, LOG_TABLE, log_data)

        except Exception as exc:
            finished_at = datetime.now(timezone.utc)

            log_data = [
                {
                    "run_id": run_id,
                    "table_id": table_id,
                    "status": "FAILED",
                    "started_at_utc": started_at,
                    "finished_at_utc": finished_at,
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat(),
                    "row_count": None,
                    "partition_count": None,
                    "partition_dates": None,
                    "is_backfill": True,
                    "write_mode": write_mode,
                    "write_duration_seconds": None,
                    "error_message": str(exc),
                }
            ]
            log_run_event(spark, LOG_TABLE, log_data)

            print(f"\n[ERROR] table_{table_id} backfill failed: {exc}")
            raise

    print(f"\nBackfill complete: {datetime.now(timezone.utc).isoformat()}")
    verify_outputs(spark, table_ids)


def run(
    spark: SparkSession,
    table_ids: list[int],
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    write_mode: str = "overwrite",
) -> None:
    run_id = str(uuid.uuid4())
    print(f"Run ID      : {run_id}")
    print(f"Run started UTC   : {datetime.now(timezone.utc).isoformat()}")
    print(f"Run started local : {datetime.now(AMS_TZ).isoformat()}")
    print(f"Input root  : {VOLUME_INPUT}")
    print(f"Output root : {VOLUME_OUTPUT}")
    print(f"Tables      : {table_ids}")
    print(f"From date   : {from_date}")
    print(f"To date     : {to_date}")

    is_backfill = from_date is not None or to_date is not None

    for table_id in table_ids:
        started_at = datetime.now(timezone.utc)

        try:
            result = process_table(
                spark, table_id, from_date, to_date, write_mode=write_mode
            )
            finished_at = datetime.now(timezone.utc)

            log_data = [
                {
                    "run_id": run_id,
                    "table_id": table_id,
                    "status": result["status"],
                    "started_at_utc": started_at,
                    "finished_at_utc": finished_at,
                    "from_date": from_date.isoformat() if from_date else None,
                    "to_date": to_date.isoformat() if to_date else None,
                    "row_count": result["row_count"],
                    "partition_count": result["partition_count"],
                    "partition_dates": ",".join(result["partition_dates"]),
                    "is_backfill": is_backfill,
                    "write_mode": result["write_mode"],
                    "write_duration_seconds": result["write_duration_seconds"],
                    "error_message": None,
                }
            ]
            log_run_event(spark, LOG_TABLE, log_data)

        except Exception as exc:
            finished_at = datetime.now(timezone.utc)

            log_data = [
                {
                    "run_id": run_id,
                    "table_id": table_id,
                    "status": "FAILED",
                    "started_at_utc": started_at,
                    "finished_at_utc": finished_at,
                    "from_date": from_date.isoformat() if from_date else None,
                    "to_date": to_date.isoformat() if to_date else None,
                    "row_count": None,
                    "partition_count": None,
                    "partition_dates": None,
                    "is_backfill": is_backfill,
                    "write_mode": write_mode,
                    "write_duration_seconds": None,
                    "error_message": str(exc),
                }
            ]
            log_run_event(spark, LOG_TABLE, log_data)

            print(f"\n[ERROR] table_{table_id} failed: {exc}")
            raise

    print(f"\nRun complete: {datetime.now(timezone.utc).isoformat()}")
    verify_outputs(spark, table_ids)
