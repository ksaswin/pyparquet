import duckdb


class ParquetDuck:
    def __init__(self, file):
        self.VIEW = 'py_parquet_cli_view'

        self.file = file
        self.connection = duckdb.connect(":default:")


    def create_view(self) -> None:
        self.connection.read_parquet(self.file).create_view(self.VIEW)


    # TODO: Idea: Maybe cache this?
    @property
    def total_row_count(self) -> int | None:
        row_count = duckdb.view(self.VIEW).count("*").fetchone()

        if row_count is None:
            return None

        return row_count[0]


    @property
    def total_column_count(self) -> int:
        return len(duckdb.view(self.VIEW).columns)


    @property
    def _all_rows(self) -> duckdb.DuckDBPyRelation:
        return duckdb.view(self.VIEW).select("*")


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
        duckdb.view(self.VIEW).to_csv(csv_file)


    def write_to_excel(self) -> None:
        pass


    def destroy(self) -> None:
        self.connection.execute(f"DROP VIEW IF EXISTS {self.VIEW}")
        self.connection.close()
