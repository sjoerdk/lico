"""Tests for `lico` package."""
from io import StringIO

import pytest
from lico import lico
from lico.lico import MissingInputColumn, Table
from tests import RESOURCE_PATH
from tests.example_classes import Concatenate
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


def test_missing_parameter():
    """test when missing parameter, is error message clear?"""
    with pytest.raises(MissingInputColumn) as e:
        lico.process(generate_table(fieldnames=['patient', 'missingdate']),
                     Concatenate(columns=['patient', 'date']),
                     catch_exceptions=False)
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





