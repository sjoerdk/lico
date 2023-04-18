"""Tests for `lico` package"""
import logging
from io import StringIO
from itertools import cycle
from unittest.mock import Mock

import pytest

from lico.exceptions import RowProcessError
from lico.io import CSVFile, Task
from lico.operations import Concatenate, FetchResult
from lico.core import Operation, Table, apply_to_each, process
from tests import RESOURCE_PATH
from tests.factories import generate_csv_file, generate_table


def test_lico():
    """Example basic usage where everything works"""

    input_list = CSVFile.init_from_path(RESOURCE_PATH / "a_csv_table_file")
    output_list = CSVFile.init_from_table(
        table=process(input_list, Concatenate(columns=["patient", "date"])), path=""
    )
    file = StringIO()
    output_list.save_to_handle(file)
    file.seek(0)
    content = file.read()
    assert "patient,date,concatenated" in content  # result column should be last


def test_save_load(tmp_path):
    """Make sure saving and then loading yields the same table values

    Also test actual disk io here to cover that code too
    """
    filename = tmp_path / "testsaveload.csv"
    fieldnames = ["fieldB", "fieldA", "fieldC"]
    table1 = generate_csv_file(fieldnames=fieldnames)
    table1.save_to_path(filename)
    loaded = CSVFile.init_from_path(filename)
    assert len(table1) == len(loaded)
    assert table1.column_order == loaded.column_order
    for saved_row, loaded_row in zip(table1.content, loaded.content):
        assert saved_row == loaded_row


def test_missing_parameter():
    """Test when missing parameter, is error message clear?"""
    with pytest.raises(RowProcessError) as e:
        process(
            generate_table(fieldnames=["patient", "missingdate"]),
            Concatenate(columns=["patient", "date"]),
            skip_failing_rows=False,
        )
    assert "Missing column 'date'" in str(e.value)


def test_saving_sparse_table():
    """When a table has no values for certain fields, saving should still work"""
    table1 = generate_csv_file(fieldnames=["fieldA1", "fieldA2"], size=4)
    table2 = generate_csv_file(fieldnames=["fieldA1", "fieldB1"], size=4)
    table1.concat(table2)
    file = StringIO()
    table1.save_to_handle(file)
    file.seek(0)
    content = file.read()
    assert "fieldA1,fieldA2,fieldB1" in content  # maintains original column order


def test_apply_to_all():
    input = generate_table(fieldnames=["col1", "col2"])
    operation = Concatenate(columns=["col1", "col2"])

    results = list(apply_to_each(input, operation))
    assert len(results) == 10


def test_read_table_set_column_names():
    """You can set column names"""
    column_names = ["col1", "col2"]
    assert (
        CSVFile.init_from_path(
            RESOURCE_PATH / "a_csv_table_file", column_names=column_names
        ).column_order
        == column_names
    )

    # but setting the wrong number will raise an error
    with pytest.raises(ValueError):
        CSVFile.init_from_path(
            RESOURCE_PATH / "a_csv_table_file", column_names=["only_one"]
        )


@pytest.fixture
def a_csv_table_file(tmp_path) -> CSVFile:
    """A csv file on disk"""
    file = CSVFile.init_from_table(
        path=tmp_path / "a_csv_table_file.csv",
        table=generate_table(fieldnames=["field1", "field2"]),
    )
    file.save()
    return file


def test_task(a_csv_table_file, tmp_path, caplog):
    """A task should handle exceptions well, writing what can be written"""
    caplog.set_level(logging.DEBUG)

    unstable_server_func = Mock(
        side_effect=cycle(["one", "two", ValueError("Horrible error")])
    )
    output_path = tmp_path / "a_result_file.csv"
    task = Task(
        input_file=a_csv_table_file,
        operation=FetchResult(id_column="field1", server_func=unstable_server_func),
        output_path=output_path,
    )

    with pytest.raises(ValueError):
        task.run()

    # output should still have been written
    output = CSVFile.init_from_path(output_path)
    assert len(output.content) == 10
    assert output.content[0]["server_result"] == "one"
    assert output.content[1]["server_result"] == "two"
    assert output.content[2]["server_result"] == ""


def test_table_slicing():
    table = generate_table(fieldnames=["columnA", "ColumnB"])
    # you can slice rows
    assert table[3].content == table.content[3]
    assert table[3:6].content == table.content[3:6]


def test_slicing_maintain_type():
    """Slicing a Table should yield a Table"""
    table = generate_table(fieldnames=["columnA", "ColumnB"])
    sliced = table[2:4]
    assert sliced.column_order == ["columnA", "ColumnB"]


def test_table_file_iterating_slicing(a_csv_table_file):
    """You should be able to iterate and slice rows in a CSV file directly"""
    rows = [x for x in a_csv_table_file]
    assert len(rows) == 10
    assert len(a_csv_table_file[3:6]) == 3


def test_process_fail_return_org():
    """If row processing fails, the original row should be in result"""
    data = [{"name": "name1"}, {"name": "name2"}, {"name": "name3"}]

    class TestOperation(Operation):
        """Operation that fails on one of the rows, and skips one other"""

        def apply(self, row):
            if row["name"] == "name2":
                raise RowProcessError("name2 is very bad")
            return {"result": row["name"] + "processed"}

        def has_previous_result(self, row):
            """Skip row 1"""
            return row["name"] == "name1"

    results = list(apply_to_each(input_rows=Table(data), operation=TestOperation()))
    # First row should have been skipped, second skipped due to error, third processed
    assert results == [
        {"name": "name1"},
        {"name": "name2"},
        {"name": "name3", "result": "name3processed"},
    ]
