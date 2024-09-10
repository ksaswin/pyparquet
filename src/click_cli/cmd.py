from datetime import datetime as dt

import click

from src.parquet_duck.utils import get_parquet_reader


"""Show the entire table"""
@click.command()
@click.argument("file")
def cat(file: str) -> None:
    pd = get_parquet_reader(file)

    try:
        pd.show_table()
    except Exception as e:
        raise e
    finally:
        pd.destroy()


"""Render the first specified number of rows in the table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", default=10, type=int, help="Number of rows to print (Default 10)")
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
@click.option("--rows", "-r", default=10, type=int, help="Number of rows to print (Default 10)")
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

    print(f"Saved file to: {csvfile}")

