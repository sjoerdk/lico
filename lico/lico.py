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
from pathlib import Path

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
        """The number of rows in this table"""
        return len(self.content)

    def __getitem__(self, item):
        return Table(content=self.content[item], column_order=self.column_order)

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
    def init_from_path(cls, path, column_names=None):
        """Parse csv table from given path

        Parameters
        ----------
        path: str
            path to file
        column_names: Optional[List[str]]
            names to use for each column in input

        Returns
        -------
        Table

        Raises
        ------
        ValueError
            If number of column_names given do not correspond to structure of table
        """
        with open(path) as f:
            reader = csv.DictReader(f)
            if column_names:
                if len(column_names) != len(reader.fieldnames):
                    raise ValueError(f"{len(column_names)} fieldname(s) given ({column_names}), "
                                     f"but {len(reader.fieldnames)} columns found "
                                     f"in file. I can't be sure which column is which")
            else:
                column_names = reader.fieldnames
            return cls(content=[row for row in reader],
                       column_order=column_names)

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
    """Takes in a row, optionally adds columns based on that row.

    Warning
    -------
    An operation should always leave the original row values unchanged. Violating
    this assumption will break resuming half-finished tasks
    """

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
        if skip and self.has_previous_result(row):
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

    def has_previous_result(self, row: Dict):
        """True if the given row contain a result from this operation"""
        return False

    def apply_to_table(self, table: Table) -> Iterator:
        return OperationIterator(input_list=table.content, operation=self)


class OperationIterator:
    """Modifies each row in input by running operation on it.

    Separate iterator to have a len() attribute, which gives nice tqdm progress bars
    """

    def __init__(self, input_list: List[Dict], operation: Operation,
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
        """Apply operation to each row in input. Returns original row + results

          Raises
          ------
          RowProcessError:
              If skip_failing_rows=False and a single row cannot be processed

          LicoError:
              If something else goes wrong

          Notes
          -----
          Modifies input list!

          """
        for idx, row in enumerate(self.input_list):
            try:
                #  update row in place and return updated version
                updated = self.operation.apply_safe(row)
                self.input_list[idx] = updated
                yield updated
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


def process_iter(input_list: Table, operation: Operation,
                 skip_failing_rows=True) -> Iterator[Dict]:
    output_list = Table(content=[], column_order=input_list.column_order)
    return process_each_row(input_list, operation, skip_failing_rows)


class Task:
    """Input file, Operation, output file. Facilitates iterative processing, skipping
    existing results

    Notes
    -----
    A task run will always write an output file, even if processing is halted
    halfway through. A second run will then load the output file and skip any row
    for which results are already computed

    """

    def __init__(self, input_path, operation, output_path):
        self.input_path = input_path
        self.operation = operation
        self.output_path = output_path

    def all_results(self, raise_exceptions=False) -> Iterator[Dict]:
        """Apply operation to each row in input, skip rows with previous answers.
        Will always save output, even with user-cancel or exceptions

        Parameters
        ----------
        raise_exceptions:
            If true, re-raise any exception raised during processing.
        """
        # if output is there, load that
        if Path(self.output_path).exists():
            print(f'Output file {self.output_path} already exists. Augmenting '
                  f'results')
            input_list = Table.init_from_path(self.output_path)
        else:
            print(f'Output file did not exist. Reading {self.input_path}')
            input_list = Table.init_from_path(self.input_path)

        try:
            for result in self.operation.apply_to_table(input_list):
                yield result
        #TODO: arg get this straight. too many layers are catching errors. Move
        # to single layer
        except RowProcessError as e:
            if raise_exceptions:
                print(f'Unhandled exception {e} stopping processing')
                self.save_to_output(input_list)
                raise e
            else:
                print(f'Error occurred: {e}')

        except Exception as e:
            print(f'Unhandled exception {e} stopping processing')
            self.save_to_output(input_list)
            raise e

    def save_to_output(self, table):

        print(f'Saving to {self.output_path}')
        table.save_to_path(self.output_path)


class LicoError(Exception):
    pass


class RowProcessError(LicoError):
    pass


class MissingInputColumn(RowProcessError):
    pass

