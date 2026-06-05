import os
from pyspark.sql import SparkSession

def get_spark_session() -> SparkSession:
    """Get a Spark session, either from Databricks Connect or from the Databricks environment.

    Returns:
        SparkSession: The Spark session to use for the notebook.
    """
    spark: SparkSession
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") is None:
        # Running outside Databricks via Databricks Connect
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    else:
        # Running inside Databricks, `spark` is defined globally
        active_spark = SparkSession.getActiveSession()
        if active_spark is None:
            # Fallback if no active session exists
            spark = SparkSession.builder.getOrCreate()
        else:
            spark = active_spark
    return spark