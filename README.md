## PyParquet

CLI tool to work with Parquet files


### Installation

Dependency manager: [poetry](https://python-poetry.org/)

Installing dependencies:
```shell
poetry install
```

Create a CLI command:
```shell
poetry shell

pip install --editable .
```
This allows you to use `pyparquet` as a regular CLI command.

Dependencies:
- [click](https://click.palletsprojects.com/en/8.1.x/): Command Line Interface Creation Kit
- [duckdb](https://duckdb.org/): Fast in-process analytical Database
- [pandas](https://pandas.pydata.org/): Data-analysis & manipulation tool
