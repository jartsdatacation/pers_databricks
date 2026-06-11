# pers_databricks/cli/run_silver_mock.py
from datetime import date, datetime, timedelta
from typing import Optional

import typer
from databricks.connect import DatabricksSession

from pers_databricks.pipelines.silver.silver_mock import (
    get_default_table_ids,
    run,
)

app = typer.Typer(help="Run the silver mock pipeline.")


def _parse_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Expected string date, got {type(value).__name__}: {value!r}")
    return datetime.strptime(value, "%Y-%m-%d").date()


def _build_spark():
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")
    return spark


def run_pipeline(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    write_mode: str = "overwrite",
    table_ids: Optional[list[int]] = None,
):
    spark = _build_spark()
    ids = table_ids or get_default_table_ids(spark)

    if from_date is None and to_date is None:
        from_d = date.today() - timedelta(days=1)
        to_d = date.today()
    else:
        from_d = _parse_date(from_date)
        to_d = _parse_date(to_date)

    run(
        spark=spark,
        table_ids=ids,
        from_date=from_d,
        to_date=to_d,
        write_mode=write_mode,
    )


# CLI wrapper for local use only
@app.command()
def main(
    from_date: Optional[str] = typer.Option(None, help="YYYY-MM-DD"),
    to_date: Optional[str] = typer.Option(None, help="YYYY-MM-DD"),
    write_mode: str = typer.Option("overwrite", help="overwrite or merge"),
    table_ids: list[int] = typer.Option([], "--table-id", "-t"),
):
    run_pipeline(
        from_date=from_date,
        to_date=to_date,
        write_mode=write_mode,
        table_ids=table_ids,
    )


if __name__ == "__main__":
    app()