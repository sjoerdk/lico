"""Operations take a single table row and give back a new row"""

from typing import Dict

from lico.core import Operation


class Concatenate(Operation):
    """A simple operation. Concatenate two columns"""

    def __init__(self, columns):
        self.columns = columns

    def apply(self, row):
        return {"concatenated": "".join(row[x] for x in self.columns)}

    def has_previous_result(self, row: Dict[str, str]):
        return bool(row.get("concatenated", None))


class FetchResult(Operation):
    """Testing with server func that might malfunction"""

    def __init__(self, id_column: str, server_func):
        self.id_column = id_column
        self.server_func = server_func

    def apply(self, row):
        id_value = row[self.id_column]
        return {"server_result": self.server_func(id_value)}
