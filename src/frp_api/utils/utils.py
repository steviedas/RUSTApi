import datetime
import json
from logging import getLogger
from typing import Dict, Any


def get_logger_for_class(path: str, class_name: str):
    return getLogger(f"api.{path}.{class_name}")


def convert_to_string(item: Any) -> Any:
    if isinstance(item, dict):
        return {k: convert_to_string(v) for k, v in item.items()}
    elif isinstance(item, (list, tuple)):
        return [convert_to_string(i) for i in item]
    elif isinstance(item, (datetime.date, datetime.datetime)):
        return item.isoformat()
    elif item is None:
        return ""
    elif isinstance(item, bool):
        return str(item).lower()
    else:
        return str(item)


def serialize_pyarrow_dict(data: Dict) -> Dict:
    converted_data = convert_to_string(data)
    json_string = json.dumps(converted_data)
    return json.loads(json_string)
