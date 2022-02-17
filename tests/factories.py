import string
import random

from lico.lico import Table


def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def generate_table(fieldnames=['field1', 'field2'], size=10):
    content = []
    for i in range(size):
        content.append({field: randomword(10) for field in fieldnames})
    return Table(content, column_order=fieldnames)


