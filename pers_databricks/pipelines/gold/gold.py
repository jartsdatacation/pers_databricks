import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from delta.tables import DeltaTable
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pers_databricks.pipelines.silver.silver_mock import get_default_table_ids

TABLE_1_MATRIX_TABLE_ID = 1
TABLE_IDS = [1, 2, 3, 5, 12, 13, 14]

VOLUME_INPUT = "/Volumes/mock_vdb/default/mock/input"
SILVER_OUTPUT = "/Volumes/mock_vdb/default/mock/output/delta"

GOLD_BASE_PATH = "s3://databricks-pers-amzn-s3-demo/gold"

SCHEMA_DIR = f"{VOLUME_INPUT}/schemas"

ACCOUNT_CSV_PATH = f"{VOLUME_INPUT}/Results_Account_mock.csv"
HOUSE_CSV_PATH = f"{VOLUME_INPUT}/Results_House_mock.csv"
SITE_CSV_PATH = f"{VOLUME_INPUT}/Results_Site_mock.csv"

LOG_TABLE = "mock_vdb.ops.gold_run_log"

LOG_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("dataset", T.StringType(), False),
        T.StructField("table_id", T.IntegerType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("started_at_utc", T.TimestampType(), True),
        T.StructField("finished_at_utc", T.TimestampType(), True),
        T.StructField("from_date", T.StringType(), True),
        T.StructField("to_date", T.StringType(), True),
        T.StructField("row_count", T.LongType(), True),
        T.StructField("partition_count", T.IntegerType(), True),
        T.StructField("partition_dates", T.StringType(), True),
        T.StructField("write_duration_seconds", T.DoubleType(), True),
        T.StructField("error_message", T.StringType(), True),
    ]
)


def gold_path(dataset: str) -> str:
    return f"{GOLD_BASE_PATH}/{dataset}"


def _read_json_spark(spark: SparkSession, path: str) -> dict:
    text = "\n".join(row.value for row in spark.read.text(path).collect())
    return json.loads(text)


def load_schema_config(spark: SparkSession, table_id: int) -> dict:
    base_raw = _read_json_spark(spark, f"{SCHEMA_DIR}/base_schema.json")
    table_raw = _read_json_spark(spark, f"{SCHEMA_DIR}/table_{table_id}.json")

    return {
        "schema": {**base_raw, **table_raw.get("schema", {})},
        "numeric_columns": table_raw.get("numeric_columns", []),
        "datetime_columns": table_raw.get("datetime_columns", []),
        "aggregation_map": table_raw.get("aggregation_map", {}),
    }


def _date_range(from_date: date, to_date: date) -> list[str]:
    result: list[str] = []
    current = from_date
    while current <= to_date:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _build_replace_where(date_strings: list[str], column_name: str = "Date") -> str:
    quoted = ", ".join(f"'{value}'" for value in date_strings)
    return f"{column_name} IN ({quoted})"


def _extract_numeric_suffix(column_name: str) -> F.Column:
    return _cast_bigint_or_null(
        F.regexp_extract(F.col(column_name).cast("string"), r"(\d+)$", 1)
    )


def _normalize_identifier(column_name: str) -> F.Column:
    return F.regexp_replace(
        F.col(column_name).cast("string"),
        r"^[\[\]\s]+|[\[\]\s]+$",
        "",
    )


def _cast_bigint_or_null(expr: F.Column) -> F.Column:
    trimmed = F.trim(expr.cast("string"))
    return F.when(trimmed == "", F.lit(None).cast("bigint")).otherwise(
        trimmed.cast("bigint")
    )


def _read_lookup_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").option("multiLine", "false").csv(path)


def load_account_lookup(spark: SparkSession) -> DataFrame:
    df = _read_lookup_csv(spark, ACCOUNT_CSV_PATH)
    required = {"Id", "Name"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{ACCOUNT_CSV_PATH} missing required columns: {missing}")

    return (
        df.select(
            _cast_bigint_or_null(F.col("Id")).alias("AccountRowId"),
            F.col("Name").cast("string").alias("Customer"),
        )
        .dropna(subset=["AccountRowId"])
        .dropDuplicates(["AccountRowId"])
    )


def load_house_lookup(spark: SparkSession) -> DataFrame:
    df = _read_lookup_csv(spark, HOUSE_CSV_PATH)
    required = {"Id", "AccountId", "Name"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{HOUSE_CSV_PATH} missing required columns: {missing}")

    ordered = df.select(
        _cast_bigint_or_null(F.col("Id")).alias("HouseRowId"),
        _cast_bigint_or_null(F.col("AccountId")).alias("HouseAccountRowId"),
        F.col("Name").cast("string").alias("HouseName"),
        _extract_numeric_suffix("Name").alias("HouseNameOrdinalKey"),
        (
            F.col("Alias").cast("string")
            if "Alias" in df.columns
            else F.lit(None).cast("string")
        ).alias("HouseAlias"),
        (
            F.col("Town").cast("string")
            if "Town" in df.columns
            else F.lit(None).cast("string")
        ).alias("HouseTown"),
    ).dropna(subset=["HouseRowId", "HouseAccountRowId"])

    window = Window.partitionBy("HouseAccountRowId").orderBy(
        F.col("HouseNameOrdinalKey").asc_nulls_last(),
        F.col("HouseRowId").asc(),
    )

    return (
        ordered.withColumn("HouseOrdinal", F.row_number().over(window))
        .select(
            "HouseRowId",
            "HouseAccountRowId",
            "HouseName",
            "HouseNameOrdinalKey",
            "HouseOrdinal",
            "HouseAlias",
            "HouseTown",
        )
        .dropDuplicates(["HouseRowId"])
    )


def load_site_lookup(spark: SparkSession) -> DataFrame:
    df = _read_lookup_csv(spark, SITE_CSV_PATH)
    required = {"SiteId", "Name"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{SITE_CSV_PATH} missing required columns: {missing}")

    ordered = df.select(
        _cast_bigint_or_null(F.col("SiteId")).alias("SiteIdKey"),
        F.col("Name").cast("string").alias("SiteName"),
        _extract_numeric_suffix("Name").alias("SiteNameOrdinalKey"),
        (
            _cast_bigint_or_null(F.col("Id"))
            if "Id" in df.columns
            else F.lit(None).cast("bigint")
        ).alias("SiteRowId"),
        (
            F.col("Alias").cast("string")
            if "Alias" in df.columns
            else F.lit(None).cast("string")
        ).alias("SiteAlias"),
        (
            F.col("Location").cast("string")
            if "Location" in df.columns
            else F.lit(None).cast("string")
        ).alias("SiteLocation"),
    ).dropna(subset=["SiteIdKey"])

    window = Window.partitionBy("SiteIdKey").orderBy(
        F.col("SiteNameOrdinalKey").asc_nulls_last(),
        F.col("SiteRowId").asc_nulls_last(),
    )

    return (
        ordered.withColumn("SiteOrdinal", F.row_number().over(window))
        .select(
            "SiteIdKey",
            "SiteRowId",
            "SiteName",
            "SiteNameOrdinalKey",
            "SiteOrdinal",
            "SiteAlias",
            "SiteLocation",
        )
        .dropDuplicates(["SiteIdKey", "SiteRowId"])
    )


def load_silver_table(
    spark: SparkSession,
    table_id: int,
    from_date: Optional[date],
    to_date: Optional[date],
) -> DataFrame:
    path = f"{SILVER_OUTPUT}/table_{table_id}"
    df = spark.read.format("delta").load(path)

    if from_date is not None:
        df = df.filter(F.col("PartitionDate") >= F.lit(from_date.isoformat()))
    if to_date is not None:
        df = df.filter(F.col("PartitionDate") <= F.lit(to_date.isoformat()))

    return df


def build_dashboard_dataset(
    telemetry_df: DataFrame,
    house_lookup_df: DataFrame,
    account_lookup_df: DataFrame,
    site_lookup_df: DataFrame,
) -> DataFrame:
    schema_names = set(telemetry_df.columns)
    if "AccountId" not in schema_names or "SiteId" not in schema_names:
        raise ValueError("Telemetry data must contain AccountId and SiteId.")

    house_id_set_expr = (
        _normalize_identifier("HouseIdSet")
        if "HouseIdSet" in schema_names
        else F.col("SiteId").cast("string")
    )
    if "HouseIdSet" in schema_names:
        house_id_set_expr = F.when(
            house_id_set_expr == "", F.col("SiteId").cast("string")
        ).otherwise(house_id_set_expr)

    joined = (
        telemetry_df.withColumn("SiteRowIdKey", _cast_bigint_or_null(house_id_set_expr))
        .join(site_lookup_df, F.col("SiteRowIdKey") == F.col("SiteRowId"), "left")
        .join(
            house_lookup_df,
            (_cast_bigint_or_null(F.col("SiteId")) == F.col("HouseAccountRowId"))
            & (F.col("SiteOrdinal") == F.col("HouseOrdinal")),
            "left",
        )
        .join(
            account_lookup_df,
            _cast_bigint_or_null(F.col("AccountId")) == F.col("AccountRowId"),
            "left",
        )
    )

    return (
        joined.withColumn("AccountId", F.col("AccountId").cast("string"))
        .withColumn("SiteId", F.col("SiteId").cast("string"))
        .withColumn("Customer", F.col("Customer").cast("string"))
        .withColumn("HouseName", F.col("HouseName").cast("string"))
        .withColumn("HouseAlias", F.col("HouseAlias").cast("string"))
        .withColumn("SiteName", F.col("SiteName").cast("string"))
        .withColumn("SiteAlias", F.col("SiteAlias").cast("string"))
        .withColumn("HouseId", F.col("AccountId").cast("string"))
        .withColumn(
            "HouseIdSet",
            F.coalesce(
                house_id_set_expr.cast("string"),
                F.col("SiteRowIdKey").cast("string"),
            ),
        )
    )


def pick_date_column(columns: list[str]) -> str:
    for candidate in [
        "LocalDateTime",
        "BestLocalDateTime",
        "BestUtcDateTime",
        "UploadLocalDateTime",
    ]:
        if candidate in columns:
            return candidate
    raise ValueError(
        "No usable datetime column found. Expected one of: "
        "LocalDateTime, BestLocalDateTime, BestUtcDateTime, UploadLocalDateTime"
    )


def build_metric_agg_expr(metric: str, agg_type: str) -> F.Column:
    agg_type = (agg_type or "mean").lower()
    metric_col = F.col(metric)
    alias = f"{metric}_mean"

    if agg_type == "sum":
        return F.sum(metric_col).alias(alias)
    if agg_type == "max":
        return F.max(metric_col).alias(alias)
    if agg_type == "min":
        return F.min(metric_col).alias(alias)
    if agg_type == "last":
        return F.last(metric_col, ignorenulls=True).alias(alias)
    if agg_type == "delta":
        return (F.max(metric_col) - F.min(metric_col)).alias(alias)

    return F.avg(metric_col).alias(alias)


def build_daily_mean_and_null_fractions(
    df: DataFrame,
    numeric_columns: list[str],
    aggregation_map: dict[str, str],
) -> DataFrame:
    schema_names = set(df.columns)
    numeric_cols = [col for col in numeric_columns if col in schema_names]
    if not numeric_cols:
        raise ValueError("No configured numeric columns were found in the dataframe.")

    date_source_col = pick_date_column(df.columns)
    working_df = df.withColumn("Date", F.to_date(F.col(date_source_col)))

    metric_exprs = [
        build_metric_agg_expr(col, aggregation_map.get(col, "mean"))
        for col in numeric_cols
    ]
    null_count_exprs = [
        F.sum(F.when(F.col(col).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            f"{col}_null_count"
        )
        for col in numeric_cols
    ]

    grouped = (
        working_df.filter(F.col("Date").isNotNull())
        .groupBy(
            "Date",
            "AccountId",
            "SiteId",
            "Customer",
            "HouseIdSet",
            "HouseId",
            "HouseName",
            "HouseAlias",
            "SiteName",
            "SiteAlias",
        )
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            *metric_exprs,
            *null_count_exprs,
            F.max(F.col(date_source_col)).alias("last_record_local_ts"),
        )
    )

    now_utc = datetime.now(timezone.utc)
    today_date = now_utc.date().isoformat()
    elapsed_utc_hours = (
        now_utc.hour + (now_utc.minute / 60.0) + (now_utc.second / 3600.0)
    )
    day_progress_fraction = elapsed_utc_hours / 24.0

    for col in numeric_cols:
        grouped = grouped.withColumn(
            f"{col}_null_fraction",
            F.col(f"{col}_null_count") / F.col("row_count"),
        )

    return (
        grouped.withColumn(
            "elapsed_utc_hours",
            F.when(
                F.col("Date") == F.lit(today_date), F.lit(elapsed_utc_hours)
            ).otherwise(F.lit(24.0)),
        )
        .withColumn(
            "day_progress_fraction",
            F.when(
                F.col("Date") == F.lit(today_date),
                F.lit(day_progress_fraction),
            ).otherwise(F.lit(1.0)),
        )
        .orderBy("Date", "Customer", "HouseIdSet", "HouseId")
    )


def make_date_expr(df: DataFrame) -> F.Column:
    candidates: list[F.Column] = []
    schema_map = {field.name: field.dataType for field in df.schema.fields}

    for col_name in ["PartitionDate", "BestUtcDateTime", "timeutc", "datetimeutc"]:
        if col_name not in schema_map:
            continue

        dtype = schema_map[col_name]
        col_ref = F.col(col_name)

        if isinstance(dtype, T.StringType):
            if col_name == "PartitionDate":
                candidates.extend(
                    [
                        F.to_date(col_ref, "yyyy-MM-dd"),
                        F.to_date(
                            F.to_timestamp(col_ref, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
                        ),
                        F.to_date(F.to_timestamp(col_ref, "yyyy-MM-dd HH:mm:ss")),
                    ]
                )
            else:
                candidates.extend(
                    [
                        F.to_date(
                            F.to_timestamp(col_ref, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
                        ),
                        F.to_date(F.to_timestamp(col_ref, "yyyy-MM-dd HH:mm:ss")),
                        F.to_date(col_ref, "yyyy-MM-dd"),
                    ]
                )
        elif isinstance(dtype, T.DateType):
            candidates.append(col_ref.cast(T.DateType()))
        elif isinstance(dtype, T.TimestampType):
            candidates.append(F.to_date(col_ref))

    if not candidates:
        return F.lit(None).cast(T.DateType())
    return F.coalesce(*candidates)


def build_position_bin_expr(column_name: str = "distancedonepercent") -> F.Column:
    source = F.col(column_name).cast("double")
    return F.when(
        source.isNotNull(),
        F.floor(F.least(F.greatest(source, F.lit(0.0)), F.lit(99.999))).cast("int"),
    )


def build_table_1_daily_matrix_long(df: DataFrame) -> DataFrame:
    required_columns = {
        "AccountId",
        "SiteId",
        "HouseIdSet",
        "beltids",
        "linenmbr",
        "distancedonepercent",
        "eggsincrease",
    }
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns for table 1 matrix long: {missing}")

    working_df = (
        df.withColumn("Date", make_date_expr(df))
        .withColumn("position_bin", build_position_bin_expr("distancedonepercent"))
        .withColumn(
            "cell_value", F.coalesce(F.col("eggsincrease").cast("double"), F.lit(0.0))
        )
    )

    return (
        working_df.filter(
            F.col("Date").isNotNull()
            & F.col("linenmbr").isNotNull()
            & F.col("position_bin").isNotNull()
        )
        .groupBy(
            "Date",
            "AccountId",
            "SiteId",
            "HouseIdSet",
            "beltids",
            "linenmbr",
            "position_bin",
        )
        .agg(
            F.sum("cell_value").alias("eggsincrease_sum"),
            F.count(F.lit(1)).alias("sample_count"),
        )
        .orderBy(
            "Date",
            "AccountId",
            "SiteId",
            "HouseIdSet",
            "beltids",
            "linenmbr",
            "position_bin",
        )
    )


def build_table_1_daily_matrix_wide(df: DataFrame) -> DataFrame:
    long_df = build_table_1_daily_matrix_long(df).withColumn(
        "position_col",
        F.format_string("pos_%03d", F.col("position_bin")),
    )

    all_pos_cols = [f"pos_{i:03d}" for i in range(100)]
    pivoted = (
        long_df.groupBy(
            "Date",
            "AccountId",
            "SiteId",
            "HouseIdSet",
            "beltids",
            "linenmbr",
        )
        .pivot("position_col")
        .agg(F.first("eggsincrease_sum"))
    )

    select_cols = [
        "Date",
        "AccountId",
        "SiteId",
        "HouseIdSet",
        "beltids",
        "linenmbr",
    ]
    for col_name in all_pos_cols:
        if col_name not in pivoted.columns:
            pivoted = pivoted.withColumn(col_name, F.lit(None).cast("double"))
        select_cols.append(col_name)

    return pivoted.select(*select_cols).orderBy(
        "Date",
        "AccountId",
        "SiteId",
        "HouseIdSet",
        "beltids",
        "linenmbr",
    )


def _partition_dates(df: DataFrame) -> list[str]:
    return [
        row[0]
        for row in df.select(F.date_format("Date", "yyyy-MM-dd").alias("Date"))
        .distinct()
        .orderBy("Date")
        .collect()
    ]


def write_gold_dataset(
    df: DataFrame, output_path: str, partition_dates: list[str]
) -> None:
    writer = (
        df.withColumn("year", F.year("Date"))
        .withColumn("month", F.month("Date"))
        .withColumn("Date", F.date_format("Date", "yyyy-MM-dd"))
        .write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("year", "month", "Date")
    )

    if DeltaTable.isDeltaTable(df.sparkSession, output_path):
        writer = writer.option("replaceWhere", _build_replace_where(partition_dates))

    writer.save(output_path)


def _build_log_record(
    *,
    run_id: str,
    dataset: str,
    table_id: Optional[int],
    started_at: datetime,
    finished_at: datetime,
    from_date: Optional[date],
    to_date: Optional[date],
    row_count: Optional[int] = None,
    partition_dates: Optional[list[str]] = None,
    write_duration_seconds: Optional[float] = None,
    error: Optional[Exception] = None,
) -> dict:
    status = "FAILED" if error is not None else "SUCCESS"
    return {
        "run_id": run_id,
        "dataset": dataset,
        "table_id": table_id,
        "status": status,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "from_date": from_date.isoformat() if from_date else None,
        "to_date": to_date.isoformat() if to_date else None,
        "row_count": row_count,
        "partition_count": len(partition_dates or []),
        "partition_dates": ",".join(partition_dates or []),
        "write_duration_seconds": write_duration_seconds,
        "error_message": str(error) if error is not None else None,
    }


def log_run_event(spark: SparkSession, records: list[dict]) -> None:
    log_df = spark.createDataFrame(records, schema=LOG_SCHEMA)
    log_df.write.format("delta").mode("append").saveAsTable(LOG_TABLE)


def process_daily_stats_table(
    spark: SparkSession,
    table_id: int,
    from_date: Optional[date],
    to_date: Optional[date],
    house_lookup_df: DataFrame,
    account_lookup_df: DataFrame,
    site_lookup_df: DataFrame,
) -> dict:
    schema_config = load_schema_config(spark, table_id)
    silver_df = load_silver_table(spark, table_id, from_date, to_date)
    dashboard_df = build_dashboard_dataset(
        silver_df,
        house_lookup_df,
        account_lookup_df,
        site_lookup_df,
    )
    daily_stats_df = build_daily_mean_and_null_fractions(
        dashboard_df,
        numeric_columns=schema_config["numeric_columns"],
        aggregation_map=schema_config["aggregation_map"],
    )

    partition_dates = _partition_dates(daily_stats_df)
    if not partition_dates:
        print(
            f"  [SKIPPED] dashboard_daily_stats_table_{table_id} -> no rows for the requested range"
        )
        return {
            "dataset": f"dashboard_daily_stats_table_{table_id}",
            "row_count": 0,
            "partition_dates": [],
            "write_duration_seconds": 0.0,
        }

    row_count = daily_stats_df.count()
    write_start = datetime.now(timezone.utc)
    write_gold_dataset(
        daily_stats_df,
        f"{GOLD_BASE_PATH}/dashboard_daily_stats_table_{table_id}",
        partition_dates,
    )
    write_duration = (datetime.now(timezone.utc) - write_start).total_seconds()

    print(
        f"  [OK] dashboard_daily_stats_table_{table_id} -> {row_count:,} rows, "
        f"{len(partition_dates)} dates"
    )

    return {
        "dataset": f"dashboard_daily_stats_table_{table_id}",
        "row_count": row_count,
        "partition_dates": partition_dates,
        "write_duration_seconds": write_duration,
    }


def process_table_1_matrices(
    spark: SparkSession,
    from_date: Optional[date],
    to_date: Optional[date],
) -> list[dict]:
    silver_df = load_silver_table(spark, TABLE_1_MATRIX_TABLE_ID, from_date, to_date)
    long_df = build_table_1_daily_matrix_long(silver_df)
    wide_df = build_table_1_daily_matrix_wide(silver_df)

    results: list[dict] = []
    for dataset_name, dataset_df in [
        ("table_1_matrix_long", long_df),
        ("table_1_matrix_wide", wide_df),
    ]:
        partition_dates = _partition_dates(dataset_df)
        if not partition_dates:
            print(f"  [SKIPPED] {dataset_name} -> no rows for the requested range")
            results.append(
                {
                    "dataset": dataset_name,
                    "row_count": 0,
                    "partition_dates": [],
                    "write_duration_seconds": 0.0,
                }
            )
            continue

        row_count = dataset_df.count()
        write_start = datetime.now(timezone.utc)
        write_gold_dataset(
            dataset_df,
            f"{GOLD_BASE_PATH}/{dataset_name}",
            partition_dates,
        )
        write_duration = (datetime.now(timezone.utc) - write_start).total_seconds()
        print(
            f"  [OK] {dataset_name} -> {row_count:,} rows, {len(partition_dates)} dates"
        )
        results.append(
            {
                "dataset": dataset_name,
                "row_count": row_count,
                "partition_dates": partition_dates,
                "write_duration_seconds": write_duration,
            }
        )

    return results


def verify_outputs(spark: SparkSession, table_ids: list[int]) -> None:
    print("\n--- Verification ---")
    output_paths = [
        *(
            f"{GOLD_BASE_PATH}/dashboard_daily_stats_table_{table_id}"
            for table_id in table_ids
        ),
        f"{GOLD_BASE_PATH}/table_1_matrix_long",
        f"{GOLD_BASE_PATH}/table_1_matrix_wide",
    ]

    for path in output_paths:
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"  {path}: [SKIPPED] No output written for the requested range")
            continue

        try:
            df = spark.read.format("delta").load(path)
            row_count = df.count()
            dates = [
                row[0] for row in df.select("Date").distinct().orderBy("Date").collect()
            ]
            print(f"  {path}: {row_count:,} rows | dates: {dates}")
        except AnalysisException as exc:
            print(f"  {path}: [NOT FOUND] {exc}")


def run(
    spark: SparkSession,
    table_ids: list[int],
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> None:
    run_id = str(uuid.uuid4())
    print(f"Run ID      : {run_id}")
    print(f"Run started : {datetime.now(timezone.utc).isoformat()}")
    print(f"Input root  : {VOLUME_INPUT}")
    print(f"Silver root : {SILVER_OUTPUT}")
    print(f"Gold base   : {GOLD_BASE_PATH}")
    print(f"Tables      : {table_ids}")
    print(f"From date   : {from_date}")
    print(f"To date     : {to_date}")

    if not table_ids:
        table_ids = TABLE_IDS

    account_lookup_df = load_account_lookup(spark)
    house_lookup_df = load_house_lookup(spark)
    site_lookup_df = load_site_lookup(spark)

    for table_id in table_ids:
        started_at = datetime.now(timezone.utc)
        try:
            result = process_daily_stats_table(
                spark,
                table_id,
                from_date,
                to_date,
                house_lookup_df,
                account_lookup_df,
                site_lookup_df,
            )
            finished_at = datetime.now(timezone.utc)
            log_run_event(
                spark,
                [
                    _build_log_record(
                        run_id=run_id,
                        dataset=result["dataset"],
                        table_id=table_id,
                        started_at=started_at,
                        finished_at=finished_at,
                        from_date=from_date,
                        to_date=to_date,
                        row_count=result["row_count"],
                        partition_dates=result["partition_dates"],
                        write_duration_seconds=result["write_duration_seconds"],
                    )
                ],
            )
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            log_run_event(
                spark,
                [
                    _build_log_record(
                        run_id=run_id,
                        dataset=f"dashboard_daily_stats_table_{table_id}",
                        table_id=table_id,
                        started_at=started_at,
                        finished_at=finished_at,
                        from_date=from_date,
                        to_date=to_date,
                        error=exc,
                    )
                ],
            )
            print(f"\n[ERROR] table_{table_id} failed: {exc}")
            raise

    matrix_results = process_table_1_matrices(spark, from_date, to_date)
    matrix_records = []
    for result in matrix_results:
        finished_at = datetime.now(timezone.utc)
        matrix_records.append(
            _build_log_record(
                run_id=run_id,
                dataset=result["dataset"],
                table_id=TABLE_1_MATRIX_TABLE_ID,
                started_at=finished_at,
                finished_at=finished_at,
                from_date=from_date,
                to_date=to_date,
                row_count=result["row_count"],
                partition_dates=result["partition_dates"],
                write_duration_seconds=result["write_duration_seconds"],
            )
        )
    if matrix_records:
        log_run_event(spark, matrix_records)

    print(f"\nRun complete: {datetime.now(timezone.utc).isoformat()}")
    verify_outputs(spark, table_ids)


def get_default_gold_table_ids(spark: SparkSession) -> list[int]:
    try:
        return get_default_table_ids(spark)
    except Exception:
        return TABLE_IDS