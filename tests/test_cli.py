from os.path import expanduser, join

import mock
import pytest
from click.testing import CliRunner
from configparser import SafeConfigParser

from apparate.configure import configure
from apparate.cli_commands import upload, upload_and_update


def test_configure_no_existing_config():
    expected_stdout = (
        'Databricks host (e.g. https://my-organization.cloud.databricks.com): '
        'https://test_host\n'
        'Databricks API token: \n'
        'Repeat for confirmation: \n'
        'Databricks folder for production libraries: test_folder\n'
    )

    filename = join(expanduser('~'), '.apparatecfg')
    expected_call_list = [
        mock.call(filename, encoding=None),
        mock.call(filename, 'w+'),
        mock.call().write('[DEFAULT]\n'),
        mock.call().write('host = https://test_host\n'),
        mock.call().write('token = test_token\n'),
        mock.call().write('prod_folder = test_folder\n'),
        mock.call().write('\n'),
    ]

    with mock.patch('builtins.open', mock.mock_open(read_data='')) as m_open:
        runner = CliRunner()
        result = runner.invoke(
            configure,
            input=(
                'https://test_host\n'
                'test_token\n'
                'test_token\n'
                'test_folder\n'
            ),
        )
        print(m_open.call_args_list)
        m_open.assert_has_calls(expected_call_list, any_order=True)

    assert not result.exception
    assert result.output == expected_stdout


def test_configure_extra_slash_in_host():
    expected_stdout = (
        'Databricks host (e.g. https://my-organization.cloud.databricks.com): '
        'https://test_host/\n'
        'Databricks API token: \n'
        'Repeat for confirmation: \n'
        'Databricks folder for production libraries: test_folder\n'
    )

    filename = join(expanduser('~'), '.apparatecfg')
    expected_call_list = [
        mock.call(filename, encoding=None),
        mock.call(filename, 'w+'),
        mock.call().write('[DEFAULT]\n'),
        mock.call().write('host = https://test_host\n'),
        mock.call().write('token = test_token\n'),
        mock.call().write('prod_folder = test_folder\n'),
        mock.call().write('\n'),
    ]

    with mock.patch('builtins.open', mock.mock_open(read_data='')) as m_open:
        runner = CliRunner()
        result = runner.invoke(
            configure,
            input=(
                'https://test_host/\n'
                'test_token\n'
                'test_token\n'
                'test_folder\n'
            ),
        )
        print(m_open.call_args_list)
        m_open.assert_has_calls(expected_call_list, any_order=True)

    assert not result.exception
    assert result.output == expected_stdout


def test_configure_extra_slash_in_folder():
    expected_stdout = (
        'Databricks host (e.g. https://my-organization.cloud.databricks.com): '
        'https://test_host\n'
        'Databricks API token: \n'
        'Repeat for confirmation: \n'
        'Databricks folder for production libraries: test_folder/\n'
    )

    filename = join(expanduser('~'), '.apparatecfg')
    expected_call_list = [
        mock.call(filename, encoding=None),
        mock.call(filename, 'w+'),
        mock.call().write('[DEFAULT]\n'),
        mock.call().write('host = https://test_host\n'),
        mock.call().write('token = test_token\n'),
        mock.call().write('prod_folder = test_folder\n'),
        mock.call().write('\n'),
    ]

    with mock.patch('builtins.open', mock.mock_open(read_data='')) as m_open:
        runner = CliRunner()
        result = runner.invoke(
            configure,
            input=(
                'https://test_host\n'
                'test_token\n'
                'test_token\n'
                'test_folder/\n'
            ),
        )
        print(m_open.call_args_list)
        m_open.assert_has_calls(expected_call_list, any_order=True)

    assert not result.exception
    assert result.output == expected_stdout


def test_configure_no_http_in_host():
    expected_stdout = (
        'Databricks host (e.g. https://my-organization.cloud.databricks.com): '
        'test_host\n'
        "looks like there's an issue - make sure the host name starts "
        'with http: https://test_host\n'
        'Databricks API token: \n'
        'Repeat for confirmation: \n'
        'Databricks folder for production libraries: test_folder\n'
    )

    filename = join(expanduser('~'), '.apparatecfg')
    expected_call_list = [
        mock.call(filename, encoding=None),
        mock.call(filename, 'w+'),
        mock.call().write('[DEFAULT]\n'),
        mock.call().write('host = https://test_host\n'),
        mock.call().write('token = test_token\n'),
        mock.call().write('prod_folder = test_folder\n'),
        mock.call().write('\n'),
    ]

    with mock.patch('builtins.open', mock.mock_open(read_data='')) as m_open:
        runner = CliRunner()
        result = runner.invoke(
            configure,
            input=(
                'test_host\n'
                'https://test_host\n'
                'test_token\n'
                'test_token\n'
                'test_folder\n'
            ),
        )
        print(m_open.call_args_list)
        m_open.assert_has_calls(expected_call_list, any_order=True)

    assert not result.exception
    assert result.output == expected_stdout


@pytest.mark.fixture('existing_config')
@mock.patch('apparate.cli_commands._load_config')
@mock.patch('apparate.cli_commands.update_databricks')
def test_upload(update_databricks_mock, config_mock, existing_config):

    config_mock.return_value = existing_config

    runner = CliRunner()
    result = runner.invoke(
        upload,
        ['--path', '/path/to/egg']
    )

    config_mock.assert_called_once()
    update_databricks_mock.assert_called_with(
        '/path/to/egg',
        'test_token',
        'test_folder',
        cleanup=False,
        update_jobs=False,
    )
    assert not result.exception


@pytest.mark.fixture('existing_config')
@mock.patch('apparate.cli_commands._load_config')
@mock.patch('apparate.cli_commands.update_databricks')
def test_upload_all_options(
    update_databricks_mock,
    config_mock,
    existing_config
):

    config_mock.return_value = existing_config

    runner = CliRunner()
    result = runner.invoke(
        upload,
        [
            '--path',
            '/path/to/egg',
            '--token',
            'new_token',
            '--folder',
            'new_folder'
        ]
    )

    config_mock.assert_called_once()
    update_databricks_mock.assert_called_with(
        '/path/to/egg',
        'new_token',
        'new_folder',
        cleanup=False,
        update_jobs=False,
    )
    assert not result.exception


@pytest.mark.fixture('empty_config')
@mock.patch('apparate.cli_commands._load_config')
def test_upload_missing_token(config_mock, empty_config):

    config_mock.return_value = empty_config

    runner = CliRunner()
    result = runner.invoke(
        upload,
        ['--path', '/path/to/egg', '--folder', 'test_folder']
    )

    assert str(result.exception) == (
        'no token found - either provide a command line argument or set up'
        ' a default by running `apparate configure`'
    )


@pytest.mark.fixture('empty_config')
@mock.patch('apparate.cli_commands._load_config')
def test_upload_missing_folder(config_mock, empty_config):

    config_mock.return_value = empty_config

    runner = CliRunner()
    result = runner.invoke(
        upload,
        ['--path', '/path/to/egg', '--token', 'test_token']
    )

    assert str(result.exception) == (
        'no folder found - either provide a command line argument or set up'
        ' a default by running `apparate configure`'
    )


@pytest.mark.fixture('existing_config')
@mock.patch('apparate.cli_commands._load_config')
@mock.patch('apparate.cli_commands.update_databricks')
def test_upload_and_update_cleanup(
    update_databricks_mock,
    config_mock,
    existing_config
):

    config_mock.return_value = existing_config

    runner = CliRunner()
    result = runner.invoke(
        upload_and_update,
        ['--path', '/path/to/egg']
    )

    config_mock.assert_called_once()
    update_databricks_mock.assert_called_with(
        '/path/to/egg',
        'test_token',
        'test_folder',
        cleanup=True,
        update_jobs=True,
    )
    assert not result.exception


@pytest.mark.fixture('existing_config')
@mock.patch('apparate.cli_commands._load_config')
@mock.patch('apparate.cli_commands.update_databricks')
def test_upload_and_update_no_cleanup(
    update_databricks_mock,
    config_mock,
    existing_config
):

    config_mock.return_value = existing_config

    runner = CliRunner()
    result = runner.invoke(
        upload_and_update,
        ['--path', '/path/to/egg', '--no-cleanup']
    )

    config_mock.assert_called_once()
    update_databricks_mock.assert_called_with(
        '/path/to/egg',
        'test_token',
        'test_folder',
        cleanup=False,
        update_jobs=True,
    )
    assert not result.exception


@mock.patch('apparate.cli_commands._load_config')
def test_upload_and_update_missing_token(config_mock):

    existing_config = SafeConfigParser()
    existing_config['DEFAULT'] = {'prod_folder': 'test_folder'}
    config_mock.return_value = existing_config

    runner = CliRunner()
    result = runner.invoke(
        upload_and_update,
        ['--path', '/path/to/egg']
    )

    config_mock.assert_called_once()
    assert str(result.exception) == (
        'no token found - either provide a command line argument or set up'
        ' a default by running `apparate configure`'
    )


@pytest.mark.fixture('empty_config')
@mock.patch('apparate.cli_commands._load_config')
def test_upload_and_update_missing_folder(config_mock, empty_config):

    config_mock.return_value = empty_config

    runner = CliRunner()
    result = runner.invoke(
        upload_and_update,
        ['-p', '/path/to/egg', '--token', 'test_token']
    )

    config_mock.assert_called_once()
    assert str(result.exception) == (
        'no folder found - either provide a command line argument or set up'
        ' a default by running `apparate configure`'
    )
