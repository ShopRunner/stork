.. apparate documentation master file, created by
   sphinx-quickstart on Fri Jun 29 16:33:02 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to apparate's documentation!
====================================

Make your libraries magically appear in Databricks.

What is apparate?
-----------------

Apparate is a tool to manage libraries in Databricks in an automated fashion. It allows you to move away from the point-and-click interface for your development work and for deploying production-level libraries for use in scheduled Databricks jobs. To learn more, check out :ref:`why`.


.. _install:

Installation
------------

Apparate is hosted on PyPi, so to get the latest version simply install via ``pip``::

    pip install apparate

You can also install from source, by cloning the git repository ``https://github.com/ShopRunner/apparate.git`` and installing via ``easy_install``::

    git clone https://github.com/ShopRunner/apparate.git
    cd apparate
    easy_install .

.. _start:

Quickstart
----------

To get started, first run ``apparate configure`` and answer the questions. 

Then you are ready to upload libraries to Databricks, using the ``apparate upload`` and ``apparate upload_and_update`` commands.

Please see :ref:`getting_started` for an introduction to the package, and :ref:`usage_details` for specifics on availible options.

Table of Contents
-----------------
.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started.rst   
   apparate.rst
   tutorial.rst
   contrib.rst

Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`
