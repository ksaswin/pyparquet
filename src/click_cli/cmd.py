from datetime import datetime as dt

import click

from src.parquet_duck.parquet import ParquetDuck


# """Interactively read & parse parquet files"""
# @click.command()
# @click.argument("file")
# def read(file: str) -> None:
#     print(file)
#     pd = ParquetDuck(file)
#
#     print(pd)


"""Show the entire table"""
@click.command()
@click.argument("file")
@click.option("--shape", "-s", flag_value=True, help="Display the shape of the table")
@click.option("--rows", "-r", type=int, help="Max number of rows to display")
@click.option("--offset", "-o", type=int, help="Offset the starting point")
def cat(file: str, shape: bool, rows: int | None, offset: int | None) -> None:
    pd = ParquetDuck(file)

    row_offset = 0

    if offset is not None:
        row_offset = offset

    try:
        if shape:
            rows = pd.total_row_count
            columns = pd.total_column_count

            click.echo(f" Columns: {columns}\n Rows:    {rows}")
            return

        pd.show_table(rows, row_offset)
    except Exception as e:
        raise e


"""Render the first specified number of rows in the table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", default=10, type=int, show_default=True, help="Number of rows to print")
def head(file: str, rows: int) -> None:
    pd = ParquetDuck(file)

    try:
        pd.show_head(rows)
    except Exception as e:
        raise e


"""Render the last specified number of rows in the table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", default=10, show_default=True, type=int, help="Number of rows to print")
def tail(file: str, rows: int) -> None:
    pd = ParquetDuck(file)

    try:
        pd.show_tail(rows)
    except Exception as e:
        raise e


"""Convert Parquet file to CSV file"""
@click.command()
@click.argument("file")
@click.option("--csvfile", help="CSV file name to write the data")
def csv(file: str, csvfile: str | None) -> None:
    pd = ParquetDuck(file)

    if csvfile is None:
        dt_iso = dt.now().isoformat()
        dt_iso = dt_iso[:dt_iso.find('.')]

        csvfile = f'pyparquet_csv_{dt_iso}.csv'

    try:
        pd.write_to_csv(csvfile)
    except Exception as e:
        raise e

    click.echo(f"Saved file to: {csvfile}")


# """Transform Parquet file to desired format"""
# @click.command()
# @click.argument("file")
# @click.option("--csv", help="Save in CSV format")
# @click.option("--excel", help="Save in CSV format")
# def transform(file: str, csv: str | None) -> None:
#     pd = ParquetDuck(file)
#     pass

