import duckdb


class ParquetDuck:
    def __init__(self, file):
        self.VIEW = 'py_parquet_cli_view'

        self.file = file
        self.connection = duckdb.connect(":default:")


    def create_view(self) -> None:
        self.connection.read_parquet(self.file).create_view(self.VIEW)


    def show_table(self, rows: int | None=None, show_tail: bool=False) -> None:
        offset = 0

        if show_tail:
            row_count = duckdb.view(self.VIEW).count("*").fetchone()

            if row_count is None:
                raise Exception("Error while trying to read rows")

            row_count = row_count[0]
            offset = row_count - rows

        sql = duckdb.view(self.VIEW).select("*")

        if rows is not None:
            sql = sql.limit(rows, offset)

        sql.show()


    def write_to_csv(self, csv_file: str) -> None:
        duckdb.view(self.VIEW).to_csv(csv_file)


    def write_to_excel(self) -> None:
        pass


    def destroy(self) -> None:
        self.connection.execute(f"DROP VIEW IF EXISTS {self.VIEW}")
        self.connection.close()
