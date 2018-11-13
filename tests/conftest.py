from .fixtures import *

from apparate.configure import _load_config, CFG_FILE, PROFILE


def pytest_addoption(parser):
    # for passing Databricks token as a command line parameter
    parser.addoption('--token', action='store', default=None)
    parser.addoption('--host', action='store', default=None)
    parser.addoption('--prod_folder', action='store', default=None)


def _resolve_test_config(metafunc, config, key):
    # makes value availible as a fixture with name key
    value = getattr(metafunc.config.option, key)
    if value is None and config.has_option(PROFILE, key):
        value = config.get(PROFILE, key)
    if key in metafunc.fixturenames and value is not None:
        metafunc.parametrize(key, [value])


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    config = _load_config(CFG_FILE)
    _resolve_test_config(metafunc, config, 'token')
    _resolve_test_config(metafunc, config, 'host')
    _resolve_test_config(metafunc, config, 'prod_folder')
