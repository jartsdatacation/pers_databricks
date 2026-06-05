from databricks.sdk.runtime import *  # noqa: F403
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import udf as u
from pyspark.sql.context import SQLContext

udf = u
spark: SparkSession
sc = spark.sparkContext
sql_context: SQLContext
sql = sql_context.sql
table = sql_context.table

def display(input=None, *args, **kwargs): ...
