.. _getting_started:

Getting Started
===============

.. _why:

Why did we build this?
----------------------

When our team started setting up CI/CD for the various packages we maintain, we encountered some difficulties integrating Jenkins with Databricks. 

We write a lot of Python + PySpark packages in our data science work, and we often deploy these as batch jobs run on a schedule using Databricks. However, each time we merged in a new change to one of these libraries we would have to manually create an egg, upload it using the Databricks GUI, go find all the jobs that used the library, and update each one to point to the new job. As our team and set of libraries and jobs grew, this became unsustainable (not to mention a big break from the CI/CD philosophy...). 

As we set out to automate this using Databrick's library API, we realized that this task required using two versions of the API and many dependant API calls. Instead of trying to recreate that logic in each Jenkinsfile, we wrote apparate. Now you can enjoy the magic as well!

Note: Apparate only works on Databricks accounts that run on AWS, not those that run on Azure. The V1 library API is required, and it only exists on AWS accounts.

To get started, check out :ref:`install` or :ref:`start`.

To learn more about how to use apparate, check out :ref:`tutorial` or :ref:`usage_details`.

To help improve apparate, check out :ref:`contrib`.
