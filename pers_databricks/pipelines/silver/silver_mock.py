import json
import re
from datetime import date, datetime
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

VOLUME_INPUT = "/Volumes/mock_vdb/default/mock/input"
VOLUME_OUTPUT = "/Volumes/mock_vdb/default/mock/output/delta"
SCHEMA_DIR = f"{VOLUME_INPUT}/schemas"

DEFAULT_TABLE_IDS: list[int] = [1, 2, 3, 5, 12, 13, 14]

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


def load_schema_config(spark: SparkSession, table_id: int) -> dict:
    base_raw = _read_json_spark(spark, f"{SCHEMA_DIR}/base_schema.json")
    table_raw = _read_json_spark(spark, f"{SCHEMA_DIR}/table_{table_id}.json")

    base_schema = {col: _parse_dtype(dt) for col, dt in base_raw.items()}
    table_schema = {col: _parse_dtype(dt) for col, dt in table_raw.get("schema", {}).items()}

    return {
        "schema": {**base_schema, **table_schema},
        "numeric_columns": table_raw.get("numeric_columns", []),
        "flag_columns": table_raw.get("flag_columns", []),
        "float_columns": table_raw.get("float_columns", []),
        "datetime_columns": table_raw.get("datetime_columns", []),
    }


def cast_dataframe(df: DataFrame, schema: dict[str, T.DataType]) -> DataFrame:
    existing = set(df.columns)

    for col_name, dtype in schema.items():
        if col_name not in existing:
            continue

        current_dtype = df.schema[col_name].dataType

        if isinstance(dtype, T.TimestampType) and isinstance(current_dtype, T.StringType):
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
            raise ValueError("Expected column 'PartitionDate' not present in DataFrame.")

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


def _build_replace_where(partition_dates: list[str]) -> str:
    quoted = ", ".join(f"'{d}'" for d in partition_dates)
    return f"PartitionDate IN ({quoted})"


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

    print(f"  [OK] table_{table_id} → {output_path}  ({df.count():,} rows, {len(partition_dates)} partitions)")

def process_table(
    spark: SparkSession,
    table_id: int,
    from_date: Optional[date],
    to_date: Optional[date],
) -> None:
    print(f"\n{'=' * 72}")
    print(f"Processing table {table_id}")

    schema_config = load_schema_config(spark, table_id)
    final_schema = schema_config["schema"]
    print(f"  Schema cols  : {len(final_schema)}")

    input_path = f"{VOLUME_INPUT}/table_{table_id}"
    df = spark.read.option("basePath", input_path).parquet(input_path)
    print(f"  Raw rows     : {df.count():,}")

    if "date" in df.columns:
        if from_date:
            df = df.filter(F.col("date") >= F.lit(from_date.isoformat()))
        if to_date:
            df = df.filter(F.col("date") <= F.lit(to_date.isoformat()))

    df = cast_dataframe(df, final_schema)
    df = normalize_partition_date(df)
    df = df.filter(F.col("PartitionDate").isNotNull())
    print(f"  Rows after filter : {df.count():,}")

    partition_dates = [
        row[0]
        for row in df.select("PartitionDate").distinct().orderBy("PartitionDate").collect()
    ]

    if not partition_dates:
        print(f"  [SKIP] No partitions found for table {table_id} in the requested range.")
        return

    print(f"  Partitions   : {partition_dates}")
    write_delta_replace_partitions(spark, df, table_id, partition_dates)


def verify_outputs(spark: SparkSession, table_ids: list[int]) -> None:
    print("\n--- Verification ---")
    for table_id in table_ids:
        path = f"{VOLUME_OUTPUT}/table_{table_id}"
        try:
            df = spark.read.format("delta").load(path)
            row_count = df.count()
            dates = [
                r[0]
                for r in df.select("PartitionDate").distinct().orderBy("PartitionDate").collect()
            ]
            print(f"  table_{table_id}: {row_count:,} rows  |  dates: {dates}")
        except AnalysisException as exc:
            print(f"  table_{table_id}: [NOT FOUND] {exc}")


def run(
    spark: SparkSession,
    table_ids: list[int],
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> None:
    print(f"Run started : {datetime.utcnow().isoformat()}Z")
    print(f"Input root  : {VOLUME_INPUT}")
    print(f"Output root : {VOLUME_OUTPUT}")
    print(f"Tables      : {table_ids}")
    print(f"From date   : {from_date}")
    print(f"To date     : {to_date}")

    for table_id in table_ids:
        try:
            process_table(spark, table_id, from_date, to_date)
        except Exception as exc:
            print(f"\n[ERROR] table_{table_id} failed: {exc}")
            raise

    print(f"\nRun complete: {datetime.utcnow().isoformat()}Z")
    verify_outputs(spark, table_ids)