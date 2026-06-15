import argparse
import sys
from datetime import date, datetime, timedelta
from typing import Optional

import typer

from pers_databricks.pipelines.silver.silver_mock import backfill, get_default_table_ids
from pers_databricks.utils.spark import get_spark_session


def _parse_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Expected string date, got {type(value).__name__}: {value!r}")
    return datetime.strptime(value, "%Y-%m-%d").date()


def run_pipeline(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    write_mode: str = "merge",
    table_ids: Optional[list[int]] = None,
    force_table_ids: Optional[list[int]] = None,
):
    spark = get_spark_session()
    spark.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")

    ids = table_ids or get_default_table_ids(spark)
    forced_ids = force_table_ids or None

    if from_date is None and to_date is None:
        from_d = date.today() - timedelta(days=1)
        to_d = date.today()
    else:
        from_d = _parse_date(from_date)
        to_d = _parse_date(to_date)

    if from_d is None or to_d is None:
        raise ValueError("Both from_date and to_date must be provided together.")

    backfill(
        spark=spark,
        table_ids=ids,
        from_date=from_d,
        to_date=to_d,
        write_mode=write_mode,
        force_table_ids=forced_ids,
    )


def main(
    from_date: Optional[str] = typer.Option(None, help="YYYY-MM-DD"),
    to_date: Optional[str] = typer.Option(None, help="YYYY-MM-DD"),
    write_mode: str = typer.Option("merge", help="merge or overwrite"),
    table_ids: list[int] = typer.Option([], "--table-id", "-t"),
    force_table_ids: list[int] = typer.Option([], "--force-table-id", "-f"),
):
    run_pipeline(
        from_date=from_date,
        to_date=to_date,
        write_mode=write_mode,
        table_ids=table_ids,
        force_table_ids=force_table_ids,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run-silver-backfill",
        description="Run the silver mock backfill pipeline.",
    )
    parser.add_argument("--from-date", dest="from_date")
    parser.add_argument("--to-date", dest="to_date")
    parser.add_argument("--write-mode", dest="write_mode", default="merge")
    parser.add_argument("--table-id", dest="table_ids", action="append", type=int)
    parser.add_argument(
        "--force-table-id",
        dest="force_table_ids",
        action="append",
        type=int,
    )
    return parser


def cli(argv: Optional[list[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_pipeline(
        from_date=args.from_date,
        to_date=args.to_date,
        write_mode=args.write_mode,
        table_ids=args.table_ids,
        force_table_ids=args.force_table_ids,
    )


if __name__ == "__main__":
    cli(sys.argv[1:])
