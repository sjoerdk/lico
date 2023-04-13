"""Anything to do with loading from and saving to files"""
import csv
from pathlib import Path
from typing import Union

from lico.core import Operation, RunStatistics, Table, apply_to_each
from lico.logging import get_module_logger

logger = get_module_logger('io')


class CSVFile(Table):
    """A comma seperated file

    Can be used directly to iterate over rows:
    rows = [row for row in CSVFile]

    Notes
    -----
    * All csv values are text. No interpreting things as ints. Too many operations
      have been messed up by truncating leading zeros etc.
    * CSV IO is done via python stdlib csv.DictReader and csv.DictWriter. Pandas is
      too complex and has too many bells and whistles.
    * A 'row' is a python dict, a csv file is a list of such dicts
    * csv row headers are required and are considered unique keys
    * All data is read into memory. Not designed for huge gb+ files

    """

    def __init__(self, path, content, column_order=None):
        super().__init__(content, column_order)
        self.path = path

    @classmethod
    def init_from_table(cls, path, table):
        return cls(path=path, content=table.content, column_order=table.column_order)

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
                    raise ValueError(f"{len(column_names)} fieldname(s) given "
                                     f"({column_names}), "
                                     f"but {len(reader.fieldnames)} columns found "
                                     f"in file. I can't be sure which column is "
                                     f"which")
            else:
                column_names = reader.fieldnames
            return cls(path=path,
                       content=[row for row in reader],
                       column_order=column_names)

    def save_to_handle(self, handle):
        writer = csv.DictWriter(handle, fieldnames=self.get_fieldnames())
        writer.writeheader()
        for row in self:
            writer.writerow(row)

    def save_to_path(self, path):
        with open(path, 'w') as f:
            self.save_to_handle(f)

    def save(self):
        self.save_to_path(self.path)


class RowIterator:
    """Iterator that will yield rows, but also allow retrieval of unprocessed rows

    Helps recovery from exceptions during row processing
    """
    def __init__(self, rows):
        self.rows_left = rows
        self.rows_returned = []

    def __iter__(self):
        return self

    def __next__(self):
        if not self.rows_left:
            raise StopIteration()
        row = self.rows_left.pop()
        self.rows_returned.append(row)
        return row


class Task:
    """Defines input file, operation, output file. Runs robustly, skipping errors
    and previous results where possible.
    """

    def __init__(self, input_file: Union[CSVFile, Path], output_path: Path,
                 operation: Operation):
        """

        Parameters
        ----------
        input_file:
            Input file name or CSVFile object
        output_path:
            Write output here
        operation:
            Run this on each row in input
        """
        if isinstance(input_file, Path):
            self.input_file = CSVFile.init_from_path(input_file)
        else:
            self.input_file = input_file
        self.output_path = output_path
        self.operation = operation

    def run(self) -> RunStatistics:
        output = CSVFile(path=self.output_path,
                         column_order=self.input_file.column_order,
                         content=[])

        statistics = RunStatistics()
        row_iter = RowIterator(rows=self.input_file.content)
        try:

            for result in apply_to_each(input_rows=row_iter,
                                   operation=self.operation,
                                   skip_failing_rows=True,
                                   skip_previous_results=True,
                                   statistics=statistics):
                output.content.append(result)

        except Exception as e:
            # whatever happens, write all input rows

            logger.error(f'Unhandled exception {e}. Writing unprocessed rows to output')
            # Exception was raised before last returned row could be processed
            unprocessed = [row_iter.rows_returned[-1]] + row_iter.rows_left
            # Add unprocessed to output to not loose any rows in output
            output.content = output.content + unprocessed
            statistics.skipped += len(unprocessed) - 1
            statistics.failed += 1
            statistics.errors.append(e)
            logger.info(statistics)
            output.save()
            raise e

        output.save()
        return statistics



