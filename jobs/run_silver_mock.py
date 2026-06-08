from datetime import date, timedelta

from databricks.connect import DatabricksSession

from pers_databricks.pipelines.silver.silver_mock import get_default_table_ids, run

spark = DatabricksSession.builder.serverless(True).getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")

default_table_ids = get_default_table_ids(spark)

# Process yesterday so today's data has fully arrived before ingestion.
yesterday = date.today() - timedelta(days=1)

run(
    spark=spark,
    table_ids=default_table_ids,
    from_date=yesterday,
    to_date=date.today(),
    write_mode="overwrite",
)
