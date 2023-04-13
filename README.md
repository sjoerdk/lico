# lico


[![CI](https://github.com/sjoerdk/lico/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/sjoerdk/lico/actions/workflows/build.yml?query=branch%3Amaster)
[![PyPI](https://img.shields.io/pypi/v/dicomtrolley)](https://pypi.org/project/dicomtrolley/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dicomtrolley)](https://pypi.org/project/dicomtrolley/)
[![Code Climate](https://codeclimate.com/github/sjoerdk/lico/badges/gpa.svg)](https://codeclimate.com/github/sjoerdk/lico)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

List comb. For quick-and-dirty operations on each row of a csv file.
Handles boiler-plate code for IO, error handling printing progress. 
Optimized for single-use operations on smaller (< millions) csv files in noisy environments.

## features 

* Free software: MIT license
* Read and write CSV files
* Run custom operations for each row
* Handles errors and existing results

## Installation 

```
pip install lico
```

## Usage

### Basic example
```
from lico.io import Task
from lico.operations import Concatenate

# concatenate column 1 and 2 in input.csv, write to output
Task(input='input.csv', 
     operation=Concatenate(['col1', 'col2']),
     output='output.csv').run()
``` 

### Defining operations
```
from lico.core import Operation

# first of all, subclass lico.core.Operation
class MyOperation(Operation):         
    def apply(self, row):
        """This method gets called on each row"""
        old_value = row['column1']           # access values like dict 
        new_value = any_function(old_value)
        return {'new_column': new_value}     # new value(s)
        # 'new_column' is appended to existing columns in output
```
### Skipping rows
There are two ways to tell lico to skip a row.`Operation.has_previous_result()` and raising `RowProcessError`
```
from lico.core import Operation
from lico.exceptions import RowProcessError

class MyOperation(Operation):         
    def apply(self, row):
        if row['col1'] == '0':          
          raise RowProcessError  # Lico will skip current row   
        return {'result':'a_result'}
        
    def has_previous_result(self, row):
      """# If the column 'result' contains anything, skip this"""      
      if row.get('result', None):
        return True   
      else:
        return False
```
## Built-in error handling
Beyond skipping lines with previous results or `RowProcessingErrors` there are ways in which lico
makes processing more robust:

* Trying to access a non-existent column in Operation.apply() will yield an error and automatically skip that row
* Output of `Task.run()` will always have the same number of rows as the input. If an unhandled exception occurs during `Task.run()`, lico will stop processing but still write all results obtained
  so far. The unprocessed rows will be in the output unmodified. 


## CSV structure

The idea is to keep CSVs as simple and unambiguous as possible. Therefore:

* All csv values are text. No interpreting things as ints. Too many operations
  have been messed up by truncating leading zeros etc.
* csv row headers are required and are considered unique keys

## Why?

Situations in which lico might speed up your work:

* I've got a Here is a csv file of (~1000) rows including `legacy id`
* Can we find `new id` for each of these legacy ids and also add `datapoint` based on `new id`?
* We don't know whether `legacy id` is valid in all cases. Or at all.
* This whole procedure is just to 'get an idea'. Just for exploration

There are many ways to approach this. Mine is usually to get rid of excel by parsing the data into a flat
csv file and then using a combination of a text editor and bash magic for merging, sorting. Intermediate
steps are saved for auditing.

However, for certain operations such as interacting with servers this is not enough. I then tend to use python.
This is more powerful but also creates overhead. Many of these tasks are single-use. Each time I have to slighty
modify the same code: read in csv, do something, handle errors, write output.

lico tries to get rid of that boiler plate code as much as possible.

