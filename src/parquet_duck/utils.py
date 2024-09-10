import os.path as filepath

from src.parquet_duck.parquet import ParquetDuck


def get_parquet_reader(file: str) -> ParquetDuck:
    if not filepath.isfile(file):
        raise Exception(f'Could not find {file}')

    pd = ParquetDuck(file)
    pd.create_view()

    return pd

