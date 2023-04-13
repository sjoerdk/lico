"""In-memory representations of a table and operations on table rows"""
from collections import OrderedDict
from dataclasses import dataclass, field

from typing import Dict, Iterable, Iterator, List

from lico.exceptions import RowProcessError
from lico.logging import get_module_logger

logger = get_module_logger('core')


class Table:
    """A table of text data, can be used like List[Dict].

    * Can be sparse, containing different keys for different rows
    * Can be used as an iterator over its rows Iterator[Dict]
    * Can be sliced: Table[2:6] will be a Table again with only the given rows

    """
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

    def get_fieldnames(self):
        """All unique header names. Maintains original column order if possible"""
        fieldnames = self.column_order
        from_content = set().union(*[x.keys() for x in self.content])
        extra = from_content.difference(set(fieldnames))
        return fieldnames + list(extra)


class Operation:
    """Takes in a row, optionally adds columns.

    Base class that can be subclassed to create any operation on a row.
    To do so, Overwrite .apply() and optionally .has_previous_result()

    Notes
    -----
    * Operations never removes values from a row, only rewrite or add new.
    * An operation can raise RowProcessError to signal it cannot operate on this row
      In many contexts RowProcessingErrors will be skipped
    * An operation can define has_previous_result


    """

    def apply_safe(self, row: Dict) -> Dict:
        """Apply operation, handle KeyErrors. Do not overwrite in child classes

        KeyError is raised in apply() when accessing any dict item that does
        not exist. For example trying row['name'] when there is no value for
        'name' in that row. As this happens often with sparse CSV files and
        you usually want to skip past these rows, this is handled here

        Returns
        -------
        Dict
            The row processed by this operation

        Raises
        ------
        RowProcessingError
            When apply() tries to access any value that does not exist in row

        """
        try:
            return self.apply(row)
        except KeyError as e:
            raise(RowProcessError(f'Missing column {str(e)}')) from e

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
            When processing this row fails. Lico will skip_previous this row and
            continue
        """
        return {}

    def has_previous_result(self, row: Dict):
        """True if the given row contains a result from this operation"""
        return False


@dataclass
class RunStatistics:
    """Statistics on a task run"""
    completed: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list = field(default_factory=lambda: [])

    def __str__(self):
        total = self.completed + self.skipped + self.failed
        return f'{total} rows total, {self.completed} completed, ' \
               f'{self.skipped} skipped, {self.failed} failed'


def apply_to_each(input_rows: Iterable[Dict], operation: Operation,
                  skip_failing_rows=True,
                  skip_previous_results=True,
                  statistics: RunStatistics=None
                  ) -> Iterator[Dict]:
    """Run operation on each row in input. Returns original row + operation results.

    Guarantees return value for each input row. If processing fails, the original
    row will be returned

    Parameters
    ----------
    input_rows:
        Iterable yielding each row as a dict
    operation:
        Operation to perform on each row
    skip_failing_rows:
        Ignore RowProcessErrors while processing
    skip_previous_results:
        Ignore rows for which Operation.has_previous_result() = True
    statistics:
        Statistics object to record rows processed, skipped etc to.
        Optional.

    """
    if not statistics:
        statistics = RunStatistics()

    def should_skip(row):
        return skip_previous_results and operation.has_previous_result(row)

    for count, input_row in enumerate(input_rows):
        if should_skip(input_row):
            logger.debug(f'skipping row {count} for previous result')
            statistics.skipped += 1
            yield input_row
        else:
            try:
                # overwrite input row with results from operation
                result = operation.apply_safe(input_row)
                statistics.completed += 1
                yield {**input_row, **result}
            except RowProcessError as e:
                if skip_failing_rows:
                    logger.debug(f'Error processing row {count}: {e}')
                    statistics.failed += 1
                    statistics.errors.append(e)
                    yield input_row
                else:
                    raise e


def process(table: Table, operation: Operation, skip_failing_rows=True,
            skip_previous_results=True) -> Table:
    """Run the given operation on each row in table

    Higher-level function on the level of Tables. If you need more control use
    apply_to_each() instead

    Parameters
    ----------
    table:
        The table read rows from
    operation:
        Operation to perform on each row
    skip_failing_rows:
        Ignore RowProcessErrors while processing
    skip_previous_results:
        Ignore rows for which Operation.has_previous_result() = True
    """
    return Table(content=list(apply_to_each(table, operation, skip_failing_rows,
                                            skip_previous_results)),
                 column_order=table.column_order)
