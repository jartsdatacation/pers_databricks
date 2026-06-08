from datetime import date

from databricks.connect import DatabricksSession

from pers_databricks.pipelines.silver.silver_mock import get_default_table_ids, run

spark = DatabricksSession.builder.serverless(True).getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")

default_table_ids = get_default_table_ids(spark)

run(
    spark=spark,
    table_ids=default_table_ids,
    from_date=date(2026, 6, 5),
    to_date=date(2026, 6, 9),
)
