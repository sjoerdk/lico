"""Working with lico requires subclassing lico.Operation. This file contains examples
of this.
"""
from typing import Dict

from lico.lico import Operation


class Concatenate(Operation):
    """An example of a subclass with an init parameter"""
    def __init__(self, columns):
        self.columns = columns

    def apply(self, row):
        return {'concatenated': ''.join(row[x] for x in self.columns)}

    def can_be_skipped(self, row: Dict):
        return bool(row.get('concatenated', None))


class FetchResult(Operation):
    """Testing with server func that might malfunction"""
    def __init__(self, id_column: str, server_func):
        self.id_column = id_column
        self.server_func = server_func

    def apply(self, row):
        id_value = row[self.id_column]
        return {'server_result': self.server_func(id_value)}

