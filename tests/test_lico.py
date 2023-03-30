"""Tests for `lico` package"""

from io import StringIO
from itertools import cycle
from unittest.mock import Mock

import pytest
from lico import lico
from lico.example_classes import Concatenate, FetchResult
from lico.lico import MissingInputColumn, Table, Task
from tests import RESOURCE_PATH
from tests.factories import generate_table


def concat(patientid, date):
    return {'concatenated': patientid + date}


def test_lico():
    """Example basic usage where everything works"""

    input_list = Table.init_from_path(RESOURCE_PATH / "a_csv_file")
    output_list = lico.process(input_list, Concatenate(columns=['patient', 'date']))
    file = StringIO()
    output_list.save(file)
    file.seek(0)
    content = file.read()
    assert "patient,date,concatenated" in content   # result column should be last


def test_save_load(tmp_path):
    """Make sure saving and then loading yields the same table values

    Also test actual disk io here to cover that code too"""
    filename = tmp_path / 'testsaveload.csv'
    fieldnames = ['fieldB', 'fieldA', 'fieldC']
    table1 = generate_table(fieldnames=fieldnames)
    table1.save_to_path(filename)
    loaded = Table.init_from_path(filename)
    assert len(table1) == len(loaded)
    assert table1.column_order == loaded.column_order
    for saved, loaded in zip(table1.content, loaded.content):
        assert saved == loaded


def test_missing_parameter():
    """test when missing parameter, is error message clear?"""
    with pytest.raises(MissingInputColumn) as e:
        lico.process(generate_table(fieldnames=['patient', 'missingdate']),
                              Concatenate(columns=['patient', 'date']),
                              skip_failing_rows=False)
    assert "Missing column 'date'" in str(e)


def test_saving_sparse_table():
    """When a table has no values for certain fields, saving should still work"""
    table1 = generate_table(fieldnames=['fieldA1', 'fieldA2'], size=4)
    table2 = generate_table(fieldnames=['fieldA1', 'fieldB1'], size=4)
    table1.concat(table2)
    file = StringIO()
    table1.save(file)
    file.seek(0)
    content = file.read()
    assert "fieldA1,fieldA2,fieldB1" in content  # maintains original column order


def test_apply_to_all():
    input = generate_table(fieldnames=['col1', 'col2'])
    operation = Concatenate(columns=['col1', 'col2'])
    results = [x for x in operation.apply_to_table(input)]
    assert len(results) == 10


def test_read_table_set_column_names():
    """You can set column names"""
    column_names = ['col1', 'col2']
    assert Table.init_from_path(
        RESOURCE_PATH / "a_csv_file", column_names=column_names
    ).column_order == column_names

    # but setting the wrong number will raise an error
    with pytest.raises(ValueError):
        Table.init_from_path(RESOURCE_PATH / "a_csv_file", column_names=['only_one'])


@pytest.fixture
def a_table_file(tmp_path):
    """A csv file on disk"""
    path = tmp_path / "a_table_file.csv"
    generate_table(fieldnames=['field1', 'field2']).save_to_path(path)
    return path


def test_task(a_table_file, tmp_path):
    """A task should handle exceptions well, writing what can be written"""
    unstable_server_func = Mock(side_effect=cycle(['one', 'two',
                                                   Exception('Horrible error')]))
    output_path = tmp_path / 'a_result_file.csv'
    task = Task(input_path=a_table_file,
                operation=FetchResult(id_column='field1',
                                      server_func=unstable_server_func),
                output_path=output_path)
    with pytest.raises(Exception):
        results = [x for x in task.all_results(raise_exceptions=True)]

    # output should still have been written, with the two results that were there
    output = Table.init_from_path(output_path)
    assert output.content[0]['server_result'] == 'one'
    assert output.content[1]['server_result'] == 'two'
    assert output.content[2]['server_result'] == ''


def test_table_slicing():
    table = generate_table(fieldnames=['columnA', 'ColumnB'])
    # you can slice rows
    assert table[3].content == table.content[3]
    assert table[3:6].content == table.content[3:6]

