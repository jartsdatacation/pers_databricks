from databricks.connect import DatabricksSession

def run() -> None:
    spark = (
        DatabricksSession.builder
        .serverless(True)
        .getOrCreate()
    )

    df = spark.read.table("mock_vdb.serving.v_table_1")
    df.show(10)

if __name__ == "__main__":
    run()