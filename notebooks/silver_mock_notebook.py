from datetime import date
from pyspark.sql import SparkSession

from pers_databricks.pipelines.silver.silver_mock import DEFAULT_TABLE_IDS, run

dbutils.widgets.text("tables", ",".join(str(t) for t in DEFAULT_TABLE_IDS), "Table IDs")
dbutils.widgets.text("from_date", "", "From date (YYYY-MM-DD, inclusive)")
dbutils.widgets.text("to_date", "", "To date (YYYY-MM-DD, inclusive)")

tables_raw = dbutils.widgets.get("tables").strip()
from_date_raw = dbutils.widgets.get("from_date").strip()
to_date_raw = dbutils.widgets.get("to_date").strip()

table_ids = [int(t) for t in tables_raw.split(",") if t]
from_date = date.fromisoformat(from_date_raw) if from_date_raw else None
to_date = date.fromisoformat(to_date_raw) if to_date_raw else None

spark = SparkSession.builder.getOrCreate()

run(
    spark=spark,
    table_ids=table_ids,
    from_date=from_date,
    to_date=to_date,
)