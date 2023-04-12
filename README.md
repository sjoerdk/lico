# lico


[![CI](https://github.com/sjoerdk/lico/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/sjoerdk/lico/actions/workflows/build.yml?query=branch%3Amaster)
[![PyPI](https://img.shields.io/pypi/v/dicomtrolley)](https://pypi.org/project/dicomtrolley/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dicomtrolley)](https://pypi.org/project/dicomtrolley/)
[![Code Climate](https://codeclimate.com/github/sjoerdk/lico/badges/gpa.svg)](https://codeclimate.com/github/sjoerdk/lico)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

List comb. For quick-and-dirty operations on each row of a csv file. Handles boiler-plate code for IO, error handling printing progress. Optimized for single-use operations on smaller (< millions) csv files in noisy environments.

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
    from lico import Table, process
    from lico.tests.example_classes import Concatenate

    input_list = Table.init_from_path("/tmp/input.csv")
    output_list = lico.process(input_list,
                               Concatenate(columns=['patient', 'date']))  # adds a column
    output_list.save_to_path("/tmp/output.csv")
```

### CSV structure

The idea is to keep CSVs as simple and unambiguous as possible. Therefore:

* All csv values are text. No interpreting things as ints. Too many operations
  have been messed up by truncating leading zeros etc.
* csv row headers are required and are considered unique keys


### Working with CSV
```
# Run a task 


input_list = Table.init_from_path("/tmp/input.csv")
```

###
###


## Why?

Situations in which lico might speed up your work:

* I've got a Here is an excel file of (~1000) rows including `legacy id`
* Can we find `new id` for each of these legacy ids and also add `datapoint` based on `new id`?
* This tasks never repeats in exactly this way.

There are many ways to approach this. Mine is usually to get rid of excel by parsing the data into a flat
csv file and then using a combination of a text editor and bash magic for merging, sorting. Intermediate
steps are saved for auditing.

However, for certain operations such as interacting with servers this is not enough. I then tend to use python.
This is more powerful but also creates overhead. Many of these tasks are single-use. Each time I have to slighty
modify the same code: read in csv, do something, handle errors, write output.

lico tries to get rid of that boiler plate code as much as possible.

