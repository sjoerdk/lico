import string
import random

from lico.core import Table
from lico.io import CSVFile


def randomword(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def generate_table(fieldnames=None, size=10):
    if not fieldnames:
        fieldnames = ["field1", "field2"]
    content = []
    for i in range(size):
        content.append({field: randomword(10) for field in fieldnames})
    return Table(content, column_order=fieldnames)


def generate_csv_file(fieldnames=None, size=10):
    if not fieldnames:
        fieldnames = ["field1", "field2"]
    return CSVFile.init_from_table(path=None, table=generate_table(fieldnames, size))
