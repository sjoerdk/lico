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

# add example code here


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
