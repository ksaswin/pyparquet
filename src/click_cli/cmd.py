from datetime import datetime as dt

import click

from src.parquet_duck.utils import get_parquet_reader


# """Interactively read & parse parquet files"""
# @click.command()
# @click.argument("file")
# def read(file: str) -> None:
#     print(file)
#     pd = get_parquet_reader(file)
#
#     print(pd)


"""Show the entire table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", type=int, help="Max number of rows to display")
@click.option("--offset", "-o", type=int, help="Offset the starting point")
def cat(file: str, rows: int | None, offset: int | None) -> None:
    pd = get_parquet_reader(file)

    row_offset = 0

    if offset is not None:
        row_offset = offset

    try:
        pd.show_table(rows, row_offset)
    except Exception as e:
        raise e
    finally:
        pd.destroy()


"""Render the first specified number of rows in the table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", default=10, type=int, show_default=True, help="Number of rows to print")
def head(file: str, rows: int) -> None:
    pd = get_parquet_reader(file)

    try:
        pd.show_table(rows)
    except Exception as e:
        raise e
    finally:
        pd.destroy()


"""Render the last specified number of rows in the table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", default=10, show_default=True, type=int, help="Number of rows to print")
def tail(file: str, rows: int) -> None:
    pd = get_parquet_reader(file)

    try:
        pd.show_table(rows, show_tail=True)
    except Exception as e:
        raise e
    finally:
        pd.destroy()


"""Convert Parquet file to CSV file"""
@click.command()
@click.argument("file")
@click.option("--csvfile", help="CSV file name to write the data")
def csv(file: str, csvfile: str | None) -> None:
    pd = get_parquet_reader(file)

    if csvfile is None:
        dt_iso = dt.now().isoformat()
        dt_iso = dt_iso[:dt_iso.find('.')]

        csvfile = f'pyparquet_csv_{dt_iso}.csv'

    try:
        pd.write_to_csv(csvfile)
    except Exception as e:
        raise e
    finally:
        pd.destroy()

    click.echo(f"Saved file to: {csvfile}")


# """Transform Parquet file to desired format"""
# @click.command()
# @click.argument("file")
# @click.option("--csv", help="Save in CSV format")
# @click.option("--excel", help="Save in CSV format")
# def transform(file: str, csv: str | None) -> None:
#     pd = get_parquet_reader(file)
#     pass

