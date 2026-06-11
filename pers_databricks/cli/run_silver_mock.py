from datetime import date, timedelta, datetime
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
    return datetime.strptime(value, "%Y-%m-%d").date()


def _build_spark():
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")
    return spark


@app.command()
def main(
    from_date: Optional[str] = typer.Option(
        None, help="Start date in YYYY-MM-DD format."
    ),
    to_date: Optional[str] = typer.Option(
        None, help="End date in YYYY-MM-DD format."
    ),
    write_mode: str = typer.Option(
        "overwrite",
        help="Write mode: overwrite or merge.",
        case_sensitive=False,
    ),
    table_ids: Optional[list[int]] = typer.Option(
        None,
        "--table-id",
        "-t",
        help="Table ID to process. Can be supplied multiple times.",
    ),
):
    spark = _build_spark()

    default_table_ids = table_ids or get_default_table_ids(spark)

    # Process yesterday so today's data has fully arrived before ingestion.
    if from_date is None and to_date is None:
        yesterday = date.today() - timedelta(days=1)
        parsed_from_date = yesterday
        parsed_to_date = date.today()
    else:
        parsed_from_date = _parse_date(from_date)
        parsed_to_date = _parse_date(to_date)

    run(
        spark=spark,
        table_ids=default_table_ids,
        from_date=parsed_from_date,
        to_date=parsed_to_date,
        write_mode=write_mode,
    )


if __name__ == "__main__":
    app()