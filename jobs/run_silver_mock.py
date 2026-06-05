from datetime import date

from databricks.connect import DatabricksSession

from pers_databricks.pipelines.silver.silver_mock import DEFAULT_TABLE_IDS, run

spark = (
    DatabricksSession.builder
    .serverless(True)
    .getOrCreate()
)

run(
    spark=spark,
    table_ids=DEFAULT_TABLE_IDS,
    from_date=None,
    to_date=None,
)