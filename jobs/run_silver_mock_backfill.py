from datetime import date

from databricks.connect import DatabricksSession

from pers_databricks.pipelines.silver.silver_mock import backfill, get_default_table_ids

spark = DatabricksSession.builder.serverless(True).getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")

default_table_ids = get_default_table_ids(spark)

# Adjust the date range as needed.
# Set force_table_ids to reprocess specific tables regardless of log state
# (e.g. after manually deleting output files).
backfill(
    spark=spark,
    table_ids=default_table_ids,
    from_date=date(2026, 5, 9),
    to_date=date(2026, 5, 9),
    write_mode="overwrite",
    force_table_ids=None,  # [1],  # e.g. [1, 3] to force specific tables
)
