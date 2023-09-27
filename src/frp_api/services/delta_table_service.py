from pathlib import Path
from typing import Dict, List

from deltalake import DeltaTable

from frp_api.models.table import Table, TableRow
from frp_api.utils.thread_safe_singleton import ThreadSafeSingleton
from frp_api.utils.utils import serialize_pyarrow_dict, get_logger_for_class

logger=get_logger_for_class(__name__,'DeltaTableService')
class DeltaTableService(metaclass=ThreadSafeSingleton):
    def __init__(self, table_path: str):
        table_path_ = Path(table_path)
        self.delta_table = DeltaTable(table_path_)
        self.schema = self.delta_table.schema()
        self.field_names = list(map(lambda x: x.name, self.schema.fields))

    def _column_in_schema(self, column_name: str) -> bool:
        return column_name in self.field_names

    def get_table_schema(self) -> Dict:
        return {field.name: field.type.type for field in self.schema.fields}

    def get_table(
        self, sort_key: str, sort_dir: str, page_size: int, offset: int
    ) -> Table:
        assert (
            sort_key in self.field_names
        ), f"Sort key: {sort_key} does not exist as a column in the table"
        slice: List[Dict] = (
            self.delta_table.to_pyarrow_dataset()
            .sort_by([(sort_key, sort_dir)])
            .to_table()
            .slice(offset, page_size)
            .to_pylist()
        )

        rows: List[TableRow] = []
        for i, row in enumerate(slice):
            serialized_row = serialize_pyarrow_dict(row)
            rows.append(TableRow(id_=str(i), rownum=str(i), describes=[serialized_row]))

        table = Table(
            id_="",
            url="",
            row=rows,
            title="frp_data",
            tableSchema=self.get_table_schema(),
        )
        return table

    def post_data(self, rows):
        pass
