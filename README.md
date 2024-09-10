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


### Usage

```shell
pyparquet [OPTIONS] COMMAND [ARGS]...
```

**Available Commands**
```shell
USAGE: pyparquet [OPTIONS] COMMAND [ARGS]...

OPTIONS
  --help  Show this message and exit.

COMMANDS:
  cat
        Description:
            Print the table
        Usage:
            pyparquet cat FILE
        Options:
            -r, --rows   INTEGER    Max number of rows to display
            -o, --offset INTEGER    Offset the starting point
  csv
        Description:
            Transform & Save file in CSV format
        Usage:
            pyparquet csv [OPTIONS] FILE
        Options:
            --csvfile TEXT  CSV file name to write the data
  head
        Description:
            Print the first 'n' rows of the table
        Usage:
            pyparquet head [OPTIONS] FILE
        Options:
            -r, --rows INTEGER  Number of rows to print [default: 10]
  tail
        Description:
            Print the last 'n' rows of the table
        Usage:
            pyparquet tail [OPTIONS] FILE
        Options:
            -r, --rows INTEGER  Number of rows to print [default: 10]
```
