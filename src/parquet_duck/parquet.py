import os.path as filepath

import duckdb


class ParquetDuck:
    def __init__(self, file):
        self.file = file

        self._table = self._initialise


    @property
    def _initialise(self) -> duckdb.DuckDBPyRelation:
        if not filepath.isfile(self.file):
            raise Exception(f"Could not find {self.file}")

        return duckdb.read_parquet(self.file)


    @property
    def _shape(self):
        return self._table.shape


    @property
    def total_row_count(self) -> int:
        return self._shape[0]


    @property
    def total_column_count(self) -> int:
        return self._shape[1]


    @property
    def _all_rows(self) -> duckdb.DuckDBPyRelation:
        return self._table.select("*")


    def show_shape(self) -> None:
        table_name = "tmp_table_pyparquet"

        duckdb.sql(f"CREATE TABLE {table_name} (row_count int64, column_count int64);")
        duckdb.table(table_name).insert(self._shape)
        duckdb.table(table_name).show()
        duckdb.sql(f"DROP TABLE {table_name};")


    def show_table(self, rows: int | None, offset: int | None) -> None:
        display_rows = rows if rows is not None else self.total_row_count
        display_offset = offset if offset is not None else 0

        table = self._all_rows.limit(display_rows, display_offset)
        table.show()


    def show_head(self, rows: int) -> None:
        self._all_rows.limit(rows).show()


    def show_tail(self, rows: int) -> None:
        if self.total_row_count is None:
            raise Exception("Error while trying to read the table")

        offset = self.total_row_count - rows

        self._all_rows.limit(rows, offset).show()


    def write_to_csv(self, csv_file: str) -> None:
        self._table.to_csv(csv_file)


    def write_to_excel(self) -> None:
        pass
