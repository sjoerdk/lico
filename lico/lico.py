"""Works with csv files, allows running operations on each and skipping existing

Design notes
------------
* All csv values are text. No interpreting things as ints. Too many operations
  have been messed up by truncating leading zeros etc.
* CSV IO is done via python stdlib csv.DictReader and csv.DictWriter. Pandas is
  too complex and has too many bells and whistles.
* A 'row' is a python dict, a csv file is a list of such dicts
* csv row headers are required and are considered unique keys
* All data is read into memory. Not designed for huge gb+ files


"""
import csv
from collections import OrderedDict

from typing import Dict, Iterator, List, Optional


class Table:
    """A table of text data, can be used like List[Dict].

    Can be sparse, containing different keys for different rows"""
    def __init__(self, content: List[Dict], column_order: List[str] = None):
        """
        Parameters
        ----------
        content:
            Each row of the table as a dict
        column_order:
            For remembering the order of the columns. If not given, internal dict
            processing will put columns in a random order

        """

        self.content = content
        if not column_order:
            column_order = []
        self.column_order = column_order

    def __iter__(self):
        return iter(self.content)

    def __len__(self):
        return len(self.content)

    def append(self, val):
        """Append a row to this table"""
        return self.content.append(val)

    def concat(self, other: 'Table'):
        """Adds all rows of other table to this one"""
        all_fieldnames = self.get_fieldnames() + other.get_fieldnames()
        unique = OrderedDict()
        for field in all_fieldnames:
            unique[field] = True
        self.column_order = list(unique.keys())
        self.content = self.content + other.content

    @classmethod
    def init_from_path(cls, path):
        with open(path) as f:
            reader = csv.DictReader(f)
            return cls(content=[row for row in reader],
                       column_order=reader.fieldnames)

    def get_fieldnames(self):
        """All unique header names. Maintains original column order if possible"""
        fieldnames = self.column_order
        from_content = set().union(*[x.keys() for x in self.content])
        extra = from_content.difference(set(fieldnames))
        return fieldnames + list(extra)

    def save(self, handle):
        writer = csv.DictWriter(handle, fieldnames=self.get_fieldnames())
        writer.writeheader()
        for row in self.content:
            writer.writerow(row)

    def save_to_path(self, path):
        with open(path, 'w') as f:
            self.save(f)


class Operation:
    """Takes in a row, does something, optionally returns a row.

    Handles common exceptions.
    """
    inputs: List[str]
    outputs: Optional[List[str]]

    def apply_safe(self, row, skip=True) -> Dict:
        """Run this operation on given row, handle exceptions

        Parameters
        ----------
        row: Dict
            input row
        skip: Bool, optional
            If True, skip rows that contain previous answers, if False, overwrite
            defaults to True

        Raises
        ------
        MissingInputColumn:
            If row misses any of the inputs for this operation
        """
        if skip and self.can_be_skipped(row):
            return row
        else:
            try:
                row.update(self.apply(row))
                return row
            except KeyError as e:
                raise MissingInputColumn(f' Missing column {e}. columns in row:'
                                         f'{[str(x) for x in row.keys()]}') from e

    def apply(self, row: Dict) -> Dict:
        """Run this operation on given row. Overwrite this in child classes.

        Parameters
        ----------
        row: Dict
            Row to process. Use dict access like row['param1'] to access parameters
            lico catches KeyErrors

        Returns
        -------
        Dict
            The result of the operation. If no result can be empty dict

        Raises
        ------
        RowProcessError
            When processing this row fails. Lico will skip this row and continue
        """
        return {}

    def can_be_skipped(self, row: Dict):
        """True if the given row contain a result from this operation"""
        return False

    def apply_to_table(self, table: Table) -> Iterator:
        return OperationIterator(input_list=table.content, operation=self)


class OperationIterator:
    """For iteratively performing an operation while having a len() property"""

    def __init__(self, input_list: Table, operation: Operation,
                 skip_failing_rows=True):
        self.input_list = input_list
        self.operation = operation
        self.skip_failing_rows = skip_failing_rows

    def __len__(self):
        return len(self.input_list)

    def __iter__(self):
        for result in self.all_results():
            yield result

    def all_results(self):
        """Apply operation to each rows in input. Returns original row + results

          Raises
          ------
          RowProcessError:
              If skip_failing_rows=False and a single row cannot be processed

          LicoError:
              If something else goes wrong

          """
        for idx, row in enumerate(self.input_list):
            try:
                yield self.operation.apply_safe(row)
            except RowProcessError as e:
                if self.skip_failing_rows:
                    print(f'Error processing line {idx}: {e}')
                    yield row
                else:
                    raise


def process_each_row(input_list: Table, operation: Operation,
                     skip_failing_rows=True) -> Iterator[Dict]:
    """Apply operation to each rows in input. Returns original row + results

    Raises
    ------
    RowProcessError:
        If skip_failing_rows=False and a single row cannot be processed

    LicoError:
        If something else goes wrong

    """
    for idx, row in enumerate(input_list):
        try:
            yield operation.apply_safe(row)
        except RowProcessError as e:
            if skip_failing_rows:
                print(f'Error processing line {idx}: {e}')
                yield row
            else:
                raise


def process(input_list: Table, operation: Operation, skip_failing_rows=True):
    output_list = Table(content=[], column_order=input_list.column_order)
    for result in process_each_row(input_list, operation, skip_failing_rows):
        output_list.append(result)
    return output_list


class Task:

    def __init__(self, input_path, operation, output_path):
        self.input_path = input_path
        self.operation = operation
        self.output_path = output_path

    def process(self, skip_failing_rows=True):
        print(f'Reading {self.input_path}')
        input_list = Table.init_from_path(self.input_path)

        try:
            output = process_each_row(input_list=input_list, skip_failing_rows=skip_failing_rows)
        except LicoError as e:
            print(f'Unhandled exception {e} stopping processing')


class LicoError(Exception):
    pass


class RowProcessError(LicoError):
    pass


class MissingInputColumn(RowProcessError):
    pass

