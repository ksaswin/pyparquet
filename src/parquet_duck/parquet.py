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


    # TODO: Idea: Maybe cache this?
    @property
    def total_row_count(self) -> int | None:
        row_count = self._table.count("*").fetchone()

        if row_count is None:
            return None

        return row_count[0]


    @property
    def total_column_count(self) -> int:
        return len(self._table.columns)


    @property
    def _all_rows(self) -> duckdb.DuckDBPyRelation:
        return self._table.select("*")


    def _render_table(self, table: duckdb.DuckDBPyRelation) -> None:
        table.show()


    def show_table(self, rows: int | None, offset: int | None) -> None:
        table = self._all_rows

        display_offset = offset if offset is not None else 0

        if display_offset and rows is None:
            total_rows = self.total_row_count

            if total_rows is not None:
                table = table.limit(total_rows, display_offset)

        if rows is not None:
            table = table.limit(rows, display_offset)

        self._render_table(table)


    def show_head(self, rows: int) -> None:
        table = self._all_rows.limit(rows)
        self._render_table(table)


    def show_tail(self, rows: int) -> None:
        if self.total_row_count is None:
            raise Exception("Error while trying to read the table")

        offset = self.total_row_count - rows

        table = self._all_rows.limit(rows, offset)
        self._render_table(table)


    def write_to_csv(self, csv_file: str) -> None:
        self._table.to_csv(csv_file)


    def write_to_excel(self) -> None:
        pass
