from datetime import datetime as dt
from os import remove as rmfile

import click

from src.parquet_duck.parquet import ParquetDuck
from src.click_cli.utils import Colors, TransformFileFormat, get_file_extension



TRANSFORM_CHOICES = click.Choice(
    [ TransformFileFormat.CSV.filetype, TransformFileFormat.EXCEL.filetype, TransformFileFormat.JSON.filetype ],
    case_sensitive=False
)


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
    row_offset = 0 if offset is None else offset

    try:
        pd = ParquetDuck(file)

        if shape:
            pd.show_shape()
            return

        pd.show_table(rows, row_offset)
    except Exception as e:
        click.secho(e, fg=Colors.ERROR.value)


"""Render the first specified number of rows in the table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", default=10, type=int, show_default=True, help="Number of rows to print")
def head(file: str, rows: int) -> None:
    try:
        pd = ParquetDuck(file)

        pd.show_head(rows)
    except Exception as e:
        click.secho(e, fg=Colors.ERROR.value)


"""Render the last specified number of rows in the table"""
@click.command()
@click.argument("file")
@click.option("--rows", "-r", default=10, show_default=True, type=int, help="Number of rows to print")
def tail(file: str, rows: int) -> None:
    try:
        pd = ParquetDuck(file)

        pd.show_tail(rows)
    except Exception as e:
        click.secho(e, fg=Colors.ERROR.value)


"""Transform Parquet file to desired format"""
@click.command()
@click.argument("file")
@click.option("--ftype", "-t", type=TRANSFORM_CHOICES, default="csv", show_default=True, help="Transform to specified file format")
@click.option("--fname", "-n", help="Filename for the transformed file")
@click.option("--delete", "-d", flag_value=True, help="Delete the original Parquet file")
def transform(file: str, ftype: str, fname: str | None, delete: bool) -> None:
    filename = fname

    if filename is None:
        dt_iso = dt.now().isoformat()
        dt_iso = dt_iso[:dt_iso.find('.')]

        file_extension = get_file_extension(ftype)

        filename = f"pyparquet_{ftype}_{dt_iso}{file_extension}"

    try:
        pd = ParquetDuck(file)

        if ftype == TransformFileFormat.CSV.filetype:
            pd.write_to_csv(filename)
        elif ftype == TransformFileFormat.EXCEL.filetype:
            pd.write_to_excel(filename)
        elif ftype == TransformFileFormat.JSON.filetype:
            pd.write_to_json(filename)

        if delete:
            rmfile(file)
    except Exception as e:
        click.secho(e, fg=Colors.ERROR.value)

    click.secho(f"Saved file to: {filename}", fg=Colors.SUCCESS.value)

