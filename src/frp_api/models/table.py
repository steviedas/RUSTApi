from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel


class DataType(BaseModel):
    id: Optional[str] = None
    base: str
    format: str
    length: Optional[int] = None
    minimum_length: Optional[int] = None
    maximum_length: Optional[int] = None
    minimum: Optional[Union[str, int, datetime]] = None
    maximum: Optional[Union[str, int, datetime]] = None
    minimum_exclusive: Optional[Union[str, int, datetime]] = None
    maximum_exclusive: Optional[Union[str, int, datetime]] = None


class TableCell(BaseModel):
    columnname: str
    value: str
    data_type: Union[str, DataType]


class TableColumn(BaseModel):
    name: str
    data_type: Union[str, DataType]
    titles: List[str]
    values: List[TableCell]


class TableRow(BaseModel):
    id_: Optional[str] = None
    rownum: str
    url: Optional[str] = None
    titles: Optional[List[str]] = None
    describes: List[Dict[str, Optional[Union[str, List[str], Dict, List[Dict]]]]]


class Table(BaseModel):
    id_: Optional[str] = None
    url: Optional[str] = None
    row: List[TableRow]
    title: Optional[str] = None
    tableSchema: Optional[Dict] = None
