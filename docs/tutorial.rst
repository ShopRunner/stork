.. _tutorial:

Tutorials
=========

For testing and development
---------------------------

When developing libraries, we often found it frustrating to frequently re-upload a library that was changing daily as we worked out a new feature. With stork, this workflow is much simpler.

When you are ready to test out changes to your library, start by deleting the current version. (Unfortunately moving or renaming the old version is insufficient, and it must be fully deleted AND removed from the trash folder before the cluster will recognize the new copy). Next restart your cluster, so it wipes the old version from its imports. 

Create a new egg file from your python package using::

    python setup.py bdist_egg

or create a new jar file from your scala package.

Upload the library to your preferred development folder using::

    stork upload -p ./dist/my_library-1.0.1-py3.6.egg -f /Users/my_email@fake_organization.com/dev_folder

    stork upload -p ./libs/my_library-1.0.1.jar -f /Users/my_email@fake_organization.com/dev_folder

Finally, attach the new library to your cluster, and you're ready to test away!

For production libraries
------------------------

While useful for testing libraries, the real reason we wrote this package involved frustrations we encountered building out our continuous integration/continuous deployment infrastructure. If you are using a CI/CD setup with tools such as Jenkins or Travis, stork works in these tools to cleanly integrate your Python packages with production jobs in Databricks. As we use Jenkins here at ShopRunner to manage CI/CD, I will continue with that example, but this should work in any similar tool.

First, you will need a Databricks token with admin permission accesible in Jenkins, here represented by the environment variable ``TOKEN``. You also need to set up the ``.storkcfg`` file. While the ``stork configure`` tool makes this easy to do locally, in an automated setup it's often easier to provide the file directly, using a command like::

   echo """[DEFAULT]
   host = https://my-organization.cloud.databricks.com
   token = ${TOKEN}
   prod_folder = /Shared/production_libraries""" > ~/.storkcfg

A standard Jenkinsfile for one of our Python packages will run a linting tool, run unittests, push the egg to our artifact store, and then use stork to push the egg to Databricks. This final steps works as follows::
  
    stork upload-and-update -p `ls dist/*.egg`

The ```ls dist/*.egg``` lists the egg files in the ``dist`` subfolder (which should just be the egg you want to upload).

We've also found it useful to redirect the printed statements to a Slack channel, so we get notifications when jobs are updated. This makes it easy to diagnose which library version caused problems if jobs ever fail.

For more details on options avaliable with these two commands, check out :ref:`usage_details`.
