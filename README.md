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


### Usage

```shell
pyparquet [OPTIONS] COMMAND [ARGS]...
```

**Available Commands**
```shell
USAGE: pyparquet [OPTIONS] COMMAND [ARGS]...

OPTIONS
  -- version Show the version and exit.
  --help     Show this message and exit.

COMMANDS:
  cat
        Description:
            Print the table
        Usage:
            pyparquet cat FILE
        Options:
            -s, --shape                   Display the shape of the table
            -r, --rows   INTEGER          Max number of rows to display
            -o, --offset INTEGER          Offset the starting point
  head
        Description:
            Print the first 'r' rows of the table
        Usage:
            pyparquet head [OPTIONS] FILE
        Options:
            -r, --rows   INTEGER          Number of rows to print [default: 10]
  tail
        Description:
            Print the last 'r' rows of the table
        Usage:
            pyparquet tail [OPTIONS] FILE
        Options:
            -r, --rows   INTEGER          Number of rows to print [default: 10]
  transform
        Description:
            Transform the Parquet file to [csv|excel|json] format
        Usage:
            pyparquet transform [OPTIONS] FILE
        Options:
            -n, --fname TEXT              Filename for the transformed file
            -d, --delete                  Delete the original Parquet file
```
