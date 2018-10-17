import json
import mock
import pytest
import responses
import requests

from .unittest_helpers import compare_multiline_strings
from apparate.update_databricks_library import (
    APIError,
    FileNameError,
    load_egg,
    get_job_list,
    get_library_mapping,
    update_job_libraries,
    delete_old_versions,
    update_databricks,
)


# helper for responses library callbacks
def request_callback(request):
    return(200, {}, request.body)


@responses.activate
def test_load_egg(host, prod_folder):
    responses.add(
        responses.POST,
        host + '/api/1.2/libraries/upload',
        status=200
    )

    with mock.patch(
        'builtins.open',
        mock.mock_open(read_data='egg file contents')
    ):
        load_egg(
            filename='test-library-1.0.3-py3.6.egg',
            library_name='test-library',
            version='1.0.3',
            folder=prod_folder,
            token='',
            host=host,
        )


@responses.activate
def test_load_egg_APIError(host, prod_folder):
    responses.add(
        responses.POST,
        host + '/api/1.2/libraries/upload',
        status=401,
    )

    with pytest.raises(APIError) as err:
        with mock.patch(
            'builtins.open',
            mock.mock_open(read_data='egg file contents')
        ):
            load_egg(
                filename='test-library-1.0.3-py3.6.egg',
                library_name='test-library',
                version='1.0.3',
                folder=prod_folder,
                token='',
                host=host,
            )
        assert err.code == 'http 401'


@pytest.mark.usefixtures('library_mapping')
@pytest.mark.usefixtures('job_list')
@pytest.mark.usefixtures('job_list_response')
@responses.activate
def test_get_job_list(library_mapping, job_list, job_list_response, host):

    responses.add(
        responses.GET,
        host + '/api/2.0/jobs/list',
        status=200,
        json=job_list_response,
    )

    job_list_actual = get_job_list(
        library_name='test-library',
        major_version='1',
        suffix='egg',
        library_mapping=library_mapping,
        token='',
        host=host,
    )

    assert len(responses.calls) == 1
    assert job_list_actual == job_list


@pytest.mark.usefixtures('library_mapping')
@pytest.mark.usefixtures('id_nums')
@pytest.mark.usefixtures('library_8')
@pytest.mark.usefixtures('library_7')
@pytest.mark.usefixtures('library_6')
@pytest.mark.usefixtures('library_5')
@pytest.mark.usefixtures('library_4')
@pytest.mark.usefixtures('library_3')
@pytest.mark.usefixtures('library_2')
@pytest.mark.usefixtures('library_1')
@pytest.mark.usefixtures('library_list_response')
@responses.activate
def test_get_library_mapping(
    library_list_response,
    library_1,
    library_2,
    library_3,
    library_4,
    library_5,
    library_6,
    library_7,
    library_8,
    id_nums,
    library_mapping,
    host,
    prod_folder,
):
    responses.add(
        responses.GET,
        host + '/api/1.2/libraries/list',
        status=200,
        json=library_list_response,
    )
    for i, lib in enumerate([
        library_1,
        library_2,
        library_3,
        library_4,
        library_5,
        library_6,
        library_7,
        library_8
    ]):
        responses.add(
            responses.GET,
            host + '/api/1.2/libraries/status?libraryId={}'.format(i+1),
            status=200,
            json=lib,
        )
    print(host)

    library_map_actual, id_nums_actual = get_library_mapping(
        token='',
        host=host,
        prod_folder=prod_folder
    )

    assert len(responses.calls) == 9
    assert id_nums == id_nums_actual
    assert library_mapping == library_map_actual


@pytest.mark.usefixtures('job_list')
@pytest.mark.usefixtures('job_update_response_list_old')
@pytest.mark.usefixtures('job_update_response_list_new')
@responses.activate
def test_update_job_libraries(
    job_list,
    job_update_response_list_old,
    job_update_response_list_new,
    host
):
    for job in job_update_response_list_old:
        responses.add(
            responses.GET,
            host + '/api/2.0/jobs/get?job_id={}'.format(job['job_id']),
            status=200,
            json=job,
        )
        responses.add_callback(
            responses.POST,
            host + '/api/2.0/jobs/reset',
            callback=request_callback,
        )
    update_job_libraries(
        job_list,
        'egg',
        'dbfs:/FileStore/jars/some_library_uri',
        '',
        host
    )

    assert len(responses.calls) == 2
    assert (
        json.loads(responses.calls[1].response.text) ==
        job_update_response_list_new[0]
    )


@pytest.mark.usefixtures('id_nums')
@responses.activate
def test_delete_old_versions(id_nums, host, prod_folder):
    for i in range(2):
        responses.add_callback(
            responses.POST,
            host + '/api/1.2/libraries/delete',
            callback=request_callback,
        )
    actual_deleted_libraries = delete_old_versions(
        library_name='test-library',
        major_version='1',
        minor_version='0.3',
        id_nums=id_nums,
        token='',
        prod_folder=prod_folder,
        host=host,
    )
    assert len(responses.calls) == 2
    actual_responses = [res.response.text for res in responses.calls]
    assert set(actual_responses) == {'libraryId=6', 'libraryId=7'}
    assert (
        set(actual_deleted_libraries) ==
        {'test-library-1.0.1', 'test-library-1.0.2'}
    )


@mock.patch('apparate.update_databricks_library.load_egg')
@responses.activate
def test_update_databricks_already_exists(
        load_mock,
        capsys,
        prod_folder,
        host,
):
    responses.add(
        responses.GET,
        'https://test-api',
        status=500,
        content_type='text/plain',
        json={
            'error_code': 'http 500',
            'message': (
                'NameConflictException: '
                'Node named "test-library" already exists'
            )
        }
    )
    res = requests.get('https://test-api')
    load_mock.side_effect = APIError(res)
    update_databricks(
        path='some/path/to/test-library-1.0.1-py3.6.egg',
        token='',
        folder='/other/folder',
        update_jobs=False,
        cleanup=False,
    )
    out, _ = capsys.readouterr()
    expected_out = (
        'this version (1.0.1) already exists:' +
        'if a change has been made please update your version number'
    )
    compare_multiline_strings(out, expected_out)
    load_mock.assert_called_with(
        'some/path/to/test-library-1.0.1-py3.6.egg',
        'test-library',
        '1.0.1',
        '/other/folder',
        '',
        host,
    )


@pytest.mark.usefixtures('job_list')
@pytest.mark.usefixtures('id_nums')
@pytest.mark.usefixtures('library_mapping')
@mock.patch('apparate.update_databricks_library.load_egg')
@mock.patch('apparate.update_databricks_library.get_job_list')
@mock.patch('apparate.update_databricks_library.get_library_mapping')
@mock.patch('apparate.update_databricks_library.update_job_libraries')
@mock.patch('apparate.update_databricks_library.delete_old_versions')
def test_update_databricks_update_jobs(
    delete_mock,
    update_mock,
    lib_mock,
    job_mock,
    load_mock,
    library_mapping,
    id_nums,
    job_list,
    capsys,
    prod_folder,
    host,
):
    path = 'some/path/to/test-library-1.0.3-py3.6.egg'
    delete_mock.return_value = ['test-library-1.0.1', 'test-library-1.0.2']
    job_mock.return_value = job_list
    lib_mock.return_value = (library_mapping, id_nums)

    update_databricks(
        path=path,
        token='',
        folder=prod_folder,
        update_jobs=True,
        cleanup=True,
    )

    out, _ = capsys.readouterr()
    expected_out = (
        'new egg test-library-1.0.3 loaded to Databricks\n'
        'current major version of library used by jobs: job_3\n'
        'updated jobs: job_3\n'
        'removed old versions: test-library-1.0.1, test-library-1.0.2\n'
    )

    compare_multiline_strings(out, expected_out)
    load_mock.assert_called_with(
        path, 'test-library', '1.0.3', prod_folder, '', host,
    )
    job_mock.assert_called_with(
        'test-library', '1', library_mapping, '', host,
    )
    lib_mock.assert_called_with(prod_folder, '', host)
    update_mock.assert_called_with(
        job_list,
        'dbfs:/FileStore/jars/47fb08a7-test-library_1_0_3_py3_6-e5f8c.egg',
        '',
        host,
    )
    delete_mock.assert_called_with(
        library_name='test-library',
        major_version='1',
        minor_version='0.3',
        id_nums=id_nums,
        token='',
        prod_folder=prod_folder,
        host=host,
    )


@pytest.mark.usefixtures('job_list')
@pytest.mark.usefixtures('id_nums')
@pytest.mark.usefixtures('library_mapping')
@mock.patch('apparate.update_databricks_library.load_egg')
@mock.patch('apparate.update_databricks_library.get_job_list')
@mock.patch('apparate.update_databricks_library.get_library_mapping')
@mock.patch('apparate.update_databricks_library.update_job_libraries')
def test_update_databricks_update_jobs_no_cleanup(
    update_mock,
    lib_mock,
    job_mock,
    load_mock,
    library_mapping,
    id_nums,
    job_list,
    capsys,
    prod_folder,
    host,
):
    path = 'some/path/to/test-library-1.0.3-py3.6.egg'
    job_mock.return_value = job_list
    lib_mock.return_value = (library_mapping, id_nums)
    update_databricks(
        path=path,
        token='',
        folder=prod_folder,
        update_jobs=True,
        cleanup=False,
    )
    out, _ = capsys.readouterr()
    expected_out = (
        'new egg test-library-1.0.3 loaded to Databricks\n'
        'current major version of library used by jobs: job_3\n'
        'updated jobs: job_3\n'
    )
    compare_multiline_strings(out, expected_out)
    load_mock.assert_called_with(
        path, 'test-library', '1.0.3', prod_folder, '', host,
    )
    job_mock.assert_called_with(
        'test-library', '1', library_mapping, '', host,
    )
    lib_mock.assert_called_with(prod_folder, '', host)
    update_mock.assert_called_with(
        job_list,
        'dbfs:/FileStore/jars/47fb08a7-test-library_1_0_3_py3_6-e5f8c.egg',
        '',
        host,
    )


@mock.patch('apparate.update_databricks_library.load_egg')
def test_update_databricks_only_upload(load_mock, capsys, prod_folder, host):
    update_databricks(
        path='some/path/to/test-library-1.0.3-py3.6.egg',
        token='',
        folder=prod_folder,
        update_jobs=False,
        cleanup=False,
    )
    out, _ = capsys.readouterr()
    expected_out = 'new egg test-library-1.0.3 loaded to Databricks'
    compare_multiline_strings(out, expected_out)
    load_mock.assert_called_with(
        'some/path/to/test-library-1.0.3-py3.6.egg',
        'test-library',
        '1.0.3',
        prod_folder,
        '',
        host,
    )


@mock.patch('apparate.update_databricks_library.load_egg')
def test_update_databricks_wrong_folder(load_mock, capsys, host):
    update_databricks(
        path='some/path/to/test-library-1.0.3-py3.6.egg',
        token='',
        folder='/other/folder',
        update_jobs=True,
        cleanup=True,
    )
    out, err = capsys.readouterr()
    expected_out = 'new egg test-library-1.0.3 loaded to Databricks'
    compare_multiline_strings(out, expected_out)
    load_mock.assert_called_with(
        'some/path/to/test-library-1.0.3-py3.6.egg',
        'test-library',
        '1.0.3',
        '/other/folder',
        '',
        host,
    )


@mock.patch('apparate.update_databricks_library.load_jar')
def test_update_databricks_with_jar_only_upload(load_mock, capsys, prod_folder, host):
    update_databricks(
        path='some/path/to/test-library-1.0.3.jar',
        token='',
        folder=prod_folder,
        update_jobs=False,
        cleanup=False,
    )
    out, _ = capsys.readouterr()
    expected_out = 'new jar test-library-1.0.3 loaded to Databricks'
    compare_multiline_strings(out, expected_out)
    load_mock.assert_called_with(
        'some/path/to/test-library-1.0.3.jar',
        'test-library',
        '1.0.3',
        prod_folder,
        '',
        host,
    )


def test_update_databricks_filename_not_match(load_mock, capsys, prod_folder, host):
    with pytest.raises(FileNameError) as err:
        update_databricks(
            path='some/path/to/test-library-1.0.3.zip',
            token='',
            folder=prod_folder,
            update_jobs=False,
            cleanup=False,
        )
        assert err.filename == 'test-library-1.0.3.zip'
