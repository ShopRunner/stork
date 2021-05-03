# Contributing

## How to Contribute

We welcome contributions in the form of issues or pull requests!

We want this to be a place where all are welcome to discuss and contribute, so please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms. Find the code of conduct in the ``CODE-OF-CONDUCT.md`` file on GitHub.

If you have a problem using stork or see a possible improvement, open an issue in the GitHub issue tracker. Please be as specific as you can.

If you see an open issue you'd like to be fixed, take a stab at it and open a PR!

### Steps for making a pull request:

1. Fork the project from GitHub

2. Clone the forked repo to your local disk and ``cd`` into it::

    git clone https://github.com/<your_github_user_name>/stork.git
    cd stork

3. Create a new branch::

    git checkout -b my_awesome_new_feature

4. Install requirements (virtualenvs always recommended!)::

    pip install -r requirements-dev.txt

5. Write some awesome useful code

6. Update unittests, docs, and CHANGELOG - to view docs locally::

     cd docs/
     make docs
     open _build/html/index.html

7. Double-check that unittests pass and the linter doesn't complain::

     pytest
     flake8 stork tests

8. Submit a PR! Once you open a PR github actions will run tests and linting. Once those pass someone will review your code and merge it into the main codebase.


Note: several of the tests rely on the ``.storkcfg`` file, so make sure to run ``stork configure`` before running tests. If you want to run tests using a different token than is in your ``.storkcfg`` file, you can also pass in the values directly, as shown in the second example. Values passed as options will override those in the config.

To run unittests using defaults in ``.storkcfg``::

   pytest

To run unittests using defaults in a ``.storkcfg`` file somewhere other than the root directory::

   pytest --cfg=/Users/my_user/other_folder/.storkcfg

To run unittests with a different token::

   pytest --token abc123

Warning: tests in ``test_token_permissions`` make actual API calls. They only make read calls, but do require an internet connection. To run only tests that are isolated, use::

    pytest --deselect tests/test_token_permissions.py

This package follows PEP8 standards, uses numpy-type docstrings, and should be tested in python3.
