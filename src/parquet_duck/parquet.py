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
    def get_total_row_count(self) -> int | None:
        row_count = duckdb.view(self.VIEW).count("*").fetchone()

        if row_count is None:
            return None

        return row_count[0]


    def show_table(self, rows: int | None=None, offset: int=0, show_tail: bool=False) -> None: #FIXME: Refactor this! Maybe split to head, tail and cat?
        if show_tail:
            if rows is None:
                raise Exception("Mandatory row argument is missing!")

            row_count = self.get_total_row_count

            if row_count is None:
                raise Exception("Error while trying to read rows")

            offset = row_count - rows

        if rows is None and offset != 0:
            row_count = self.get_total_row_count

            if row_count is None:
                raise Exception("Error while trying to read rows")

            rows = row_count

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
