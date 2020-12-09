# Stork
Command line helpers for Databricks!

![Python package](https://github.com/ShopRunner/stork/workflows/Python%20package/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/stork-library/badge/?version=latest)](https://stork-library.readthedocs.io/en/latest/?badge=latest)


## Why we built this

When our team started setting up CI/CD for the various packages we maintain, we encountered some difficulties integrating Jenkins with Databricks.

We write a lot of Python + PySpark packages in our data science work, and we often deploy these as batch jobs run on a schedule using Databricks. However, each time we merged in a new change to one of these libraries we would have to manually create an egg, upload it using the Databricks GUI, go find all the jobs that used the library, and update each one to point to the new job. As our team and set of libraries and jobs grew, this became unsustainable (not to mention a big break from the CI/CD philosophy...).

As we set out to automate this using Databrick's library API, we realized that this task required using two versions of the API and many dependant API calls. Instead of trying to recreate that logic in each Jenkinsfile, we wrote stork. Now you can enjoy the magic as well!

Stork now works for both `.egg` and `.jar` files to support Python + PySpark and Scala + Spark libaries.
Take advantage of stork's ability to update jobs, make sure you're following one of the following naming conventions:
```
new_library-1.0.0-py3.6.egg
new_library-1.0.0-SNAPSHOT-py3.6.egg
new_library-1.0.0-SNAPSHOT-my-branch-py3.6.egg
new_library-1.0.0.egg
new_library-1.0.0-SNAPSHOT.egg
new_library-1.0.0-SNAPSHOT-my-branch.egg
new_library-1.0.0.jar
new_library-1.0.0-SNAPSHOT.jar
new_library-1.0.0-SNAPSHOT-my-branch.jar
```
Where the first number in the version (in this case `1`) is a major version signaling breaking changes.

## What it does

Stork is a set of command line helpers for Databricks. Some commands are for managing libraries in Databricks in an automated fashion. This allows you to move away from the point-and-click interface for your development work and for deploying production-level libraries for use in scheduled Databricks jobs. Another command allows you to create an interactive cluster that replicates the settings used on a job cluster.

For a more detailed API and tutorials, check out the [docs](https://stork-library.readthedocs.io/en/latest/index.html).

## Installation

Note: stork requires python3, and currently only works on Databricks accounts that run AWS (not Azure)

Stork is hosted on PyPi, so to get the latest version simply install via pip:
```
pip install stork
```

You can also install from source, by cloning the git repository https://github.com/ShopRunner/stork.git and installing via easy_install:
```
git clone https://github.com/ShopRunner/stork.git
cd stork
easy_install .
```

## Setup

### Configuration

Stork uses a `.storkcfg` to store information about your Databricks account and setup. To create this file, run:
```
stork configure
```

You will be asked for your Databricks host name (the url you use to access the account - something like `https://my-organization.cloud.databricks.com`), an access token, and your production folder. This should be a folder your team creates to keep production-ready libraries. By isolating production-ready libraries in their own folder, you ensure that stork will never update a job to use a library still in development/testing.

### Databricks API token

The API tokens can be generated in Databricks under Account Settings -> Access Tokens. To upload an egg to any folder in Databricks, you can use any token. To update jobs, you will need a token with admin permissions, which can be created in the same manner by an admin on the account.

## Usage notes

While libraries can be uploaded to folders other than your specified production library, no libraries outside of this folder will ever be deleted and no jobs using libraries outside of this folder will be updated.

If you try to upload a library to Databricks that already exists there with the same version, a warning will be printed instructing the user to update the version if a change has been made. Without a version change the new library will not be uploaded.

## Contributing
See a way for stork to improve? We welcome contributions in the form of issues or pull requests!

Please check out the [contributing](https://stork-library.readthedocs.io/en/latest/contrib.html) page for more information.
