====
lico
====

.. image:: https://github.com/sjoerdk/lico/workflows/build/badge.svg
        :target: https://github.com/sjoerdk/lico/actions?query=workflow%3Abuild
        :alt: Build Status


.. image:: https://img.shields.io/pypi/v/lico.svg
        :target: https://pypi.python.org/pypi/lico

.. image:: https://readthedocs.org/projects/lico/badge/?version=latest
        :target: https://lico.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/sjoerdk/lico/shield.svg
     :target: https://pyup.io/repos/github/sjoerdk/lico/
     :alt: Updates



List comb. Read Write and Augment csv files iteratively. For performing tasks on each row of a csv file and saving the
results. Takes care of boiler plate code for opening, parsing, saving, printing progress. Optimized for single-use
operations on smaller (< millions) csv files where operations are expensive and unreliable.


* Free software: MIT license
* Documentation: https://lico.readthedocs.io.

Example::

    from lico import Table, process
    from lico.tests.example_classes import Concatenate

    input_list = Table.init_from_path("/tmp/input.csv")
    output_list = lico.process(input_list,
                               Concatenate(columns=['patient', 'date']))  # adds a column
    output_list.save("/tmp/output.csv")


Why?
----
To make the following type of task easier:
* Here is an excel file of (~1000) rows including `legacy id`
* Can we find `new id` for each of these legacy ids and also add `datapoint` based on `new id`?
* This tasks never repeats in exactly this way.

There are many ways to approach this. Mine is usually to get rid of excel by parsing the data into a flat
csv file and then using a combination of a text editor and bash magic for merging, sorting. Intermediate
steps are saved for auditing.

However, for certain operations such as interacting with servers this is not enough. I then tend to use python.
This is more powerful but also creates overhead. Many of these tasks are single-use. Each time I have to slighty
modify the same code: read in csv, do something, handle errors, write output.

lico tries to get rid of that boiler plate code as much as possible.

Features
--------

* Read and write csv files with headers
* Framework for performing operation on each row
* Handles errors gracefully
* Allows skipping already completed operations

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
