.. _usage_details:

Stork
========

The stork cli is your point of contact for managing continuous delivery of
Python packages for use in Databracks.

Configure
---------

To get started, configure your Databricks account information. You'll need your Databricks account connection info, and you will also be asked to name a production folder. To learn more about how these values will be used and where to find this information, check out the :ref:`getting_started` page.

When you're ready to go, run ``stork configure``.

.. command-output:: stork configure --help

Now you're all set to start using stork! The two main commands avaliable in stork are ``upload`` and ``upload-and-update``. 

Upload
------

``upload`` can be used anytime by anyone and promises not break anything. It simply uploads an egg or jar file, and will throw an error if a file with the same name alreay exists. 

If you've set up your ``.storkcfg`` file using the ``configure`` command, you only need to provide a path to the ``.egg`` or ``.jar`` file, but can also override the default api token and destination folder if desired.

If you try to upload a library to Databricks that already exists there with the same version, a warning will be printed instructing the user to update the version if a change has been made. Without a version change the new library will not be uploaded.

This command will print out a message letting you know the name of the egg or jar that was uploaded.

.. command-output:: stork upload --help

Upload and Update
-----------------

``upload-and-update`` requires a token with admin-level permissions. It does have the capacity to delete libraries, but if used in a CI/CD system will not cause any issues. For advice on how to set this up, check out the *Gettting Started* page. 

Used with default settings, ``upload-and-update`` will start by uploading the ``.egg`` or ``.jar`` file. It will then go find all jobs that use the same major version of the library and update them to point to the new version. Finally, it will clean up outdated versions in the production library. No libraries in any other folders will ever be deleted. 

If you're nervous about deleting files, you can always use the ``--no-cleanup`` flag and no files will be deleted or overwritten. If you're confident in your CI/CD system, however, leaving the cleanup variable set to ``True`` will keep your production folder tidy, with only the most current version of each major release of each library.

This command will print out a message letting you know (1) the name of the egg or jar that was uploaded, (2) the list of jobs currently using the same major version of this library, (3) the list of jobs updated - this should match number 2, and (4) any old versions removed - if you haven't used the ``--no-cleanup`` flag.

In the same way as ``upload``, if you try to upload a library to Databricks that already exists there with the same version, a warning will be printed instructing the user to update the version if a change has been made. Without a version change the new library will not be uploaded.

.. command-output:: stork upload-and-update --help

For more info about usage, check out the :ref:`tutorial`.

Create cluster
------

``create-cluster`` can be used anytime by anyone and promises not to break anything. It simply creates a new cluster and will create a second cluster if a cluster with the same name alreay exists. 

If you've set up your ``.storkcfg`` file using the ``configure`` command, you only need to provide a job_id and optionally a cluster_name, but can also override the default api token if desired.

This command will print out a message letting you know the name of the cluster that was created.

.. command-output:: stork create-cluster --help
