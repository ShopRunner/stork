# flake8: noqa E501

import pytest
from configparser import ConfigParser
from apparate.update_databricks_library import FileNameMatch


@pytest.fixture
def delete_library_response_list():
    return [{'libraryId': '6'}, {'libraryId': '7'}]


@pytest.fixture
def id_nums():
    id_nums = {
        'test-library-1.0.3': {
            'name_match': FileNameMatch('test-library-1.0.3.egg'),
            'id_num': '8',
        },
        'test-library-1.0.2': {
            'name_match': FileNameMatch('test-library-1.0.2.egg'),
            'id_num': '7',
        },
        'test-library-1.0.1': {
            'name_match': FileNameMatch('test-library-1.0.1.egg'),
            'id_num': '6',
        },
        'test-library-plus-stuff-0.0.0': {
            'name_match': FileNameMatch('test-library-plus-stuff-0.0.0.egg'),
            'id_num': '4',
        },
        'test-library-0.0.0': {
            'name_match': FileNameMatch('test-library-0.0.0.egg'),
            'id_num': '3',
        },
        'awesome_library_a-0.10.1': {
            'name_match': FileNameMatch('awesome_library_a-0.10.1.egg'),
            'id_num': '2',
        },
        'awesome_library_b-4.2.3': {
            'name_match': FileNameMatch('awesome_library_b-4.2.3.egg'),
            'id_num': '1',
        },
    }
    return id_nums


@pytest.fixture
def job_list():
    job_list = [
        {
            'job_id': 3,
            'job_name': 'job_3',
            'library_path': 'dbfs:/FileStore/jars/47fb08a7-test-library_1_0_1_py3_6-e5f8c.egg',
        },
    ]
    return job_list


@pytest.fixture
def job_list_response():
    job_list_response = {
        'jobs': [
            {
                'job_id': 1,
                'settings': {
                    'name': 'job_1',
                    'new_cluster': {'cluster_attributes': 'attrs'},
                    'libraries': [
                        {'pypi': {'package': 'boto3'}},
                        {'maven': {'coordinates': 'maven_library'}},
                        {'egg': 'dbfs:/FileStore/jars/996c949b-awesome_library_a_0_10_1_py3_6-266f.egg'},
                        {'egg': 'dbfs:/FileStore/jars/47fb08a7-awesome_library_b_4_2_3_py3_6-e5f8c.egg'},
                        {'egg': 'dbfs:/FileStore/jars/47fb08a7-test-library_0_0_0_py3_6-e5f8c.egg'},
                    ],
                },
                'creator_user_name': 'tests@shoprunner'
            },
            {
                'job_id': 2,
                'settings': {
                    'name': 'job_2',
                    'new_cluster': {'cluster_attributes': 'attrs'},
                    'libraries': [
                        {'egg': 'dbfs:/FileStore/jars/47fb08a7-test-library_1_0_0_py3_6-e5f8c.egg'},
                        {'egg': 'dbfs:/FileStore/jars/01832402-test-library-plus-stuff_0_0_0_py3_6-e5f8c.egg'},
                    ],
                },
                'creator_user_name': 'tests@shoprunner'
            },
            {
                'job_id': 3,
                'settings': {
                    'name': 'job_3',
                    'new_cluster': {'cluster_attributes': 'attrs'},
                    'libraries': [
                        {'egg': 'dbfs:/FileStore/jars/47fb08a7-test-library_1_0_1_py3_6-e5f8c.egg'},
                        {'egg': 'dbfs:/FileStore/jars/01832402-test-library-plus-stuff_0_0_0_py3_6-e5f8c.egg'},
                    ],
                },
                'creator_user_name': 'tests@shoprunner'
            },
            {
                'job_id': 4,
                'settings': {
                    'name': 'job_4',
                    'new_cluster': {'cluster_attributes': 'attrs'},
                    'libraries': [
                        {'egg': 'dbfs:/FileStore/jars/01832402-test-library-plus-stuff_0_0_0_py3_6-e5f8c.egg'}
                    ],
                },
                'creator_user_name': 'tests@shoprunner'
            },
        ]
    }
    return job_list_response


@pytest.fixture
def job_update_response_list_new():
    job_update_response_list = [
        {
            'job_id': 3,
            'new_settings': {
                'name': 'job_3',
                'new_cluster': {
                    'cluster_attributes': 'attrs'
                },
                'libraries': [
                    {'egg': 'dbfs:/FileStore/jars/47fb08a7-test-library_1_0_3_py3_6-e5f8c.egg'},
                    {'egg': 'dbfs:/FileStore/jars/01832402-test-library-plus-stuff_0_0_0_py3_6-e5f8c.egg'}
                ]
            },
            'creator_user_name': 'tests@shoprunner'
        },
    ]
    return job_update_response_list


@pytest.fixture
def job_update_response_list_old():
    job_update_response_list = [
        {
            'job_id': 3,
            'settings': {
                'name': 'job_3',
                'new_cluster': {
                    'cluster_attributes': 'attrs'
                },
                'libraries': [
                    {'egg': 'dbfs:/FileStore/jars/47fb08a7-test-library_1_0_3_py3_6-e5f8c.egg'},
                    {'egg': 'dbfs:/FileStore/jars/01832402-test-library-plus-stuff_0_0_0_py3_6-e5f8c.egg'}
                ]
            },
            'creator_user_name': 'tests@shoprunner'
        },
    ]
    return job_update_response_list


@pytest.fixture
def library_1(prod_folder):
    return {
        'id': '1',
        'name': 'awesome_library_b-4.2.3',
        'folder': prod_folder,
        'libType': 'python-egg',
        'files': ['47fb08a7-awesome_library_b_4_2_3_py3_6-e5f8c.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_2(prod_folder):
    return {
        'id': '2',
        'name': 'awesome_library_a-0.10.1',
        'folder': prod_folder,
        'libType': 'python-egg',
        'files': ['996c949b-awesome_library_a_0_10_1_py3_6-266f.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_3(prod_folder):
    return {
        'id': '3',
        'name': 'test-library-0.0.0',
        'folder': prod_folder,
        'libType': 'python-egg',
        'files': ['47fb08a7-test-library_0_0_0_py3_6-e5f8c.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_4(prod_folder):
    return {
        'id': '4',
        'name': 'test-library-plus-stuff-0.0.0',
        'folder': prod_folder,
        'libType': 'python-egg',
        'files': ['01832402-test-library-plus-stuff_0_0_0_py3_6-e5f8c.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_5(prod_folder):
    return {
        'id': '5',
        'name': 'test-library-1.0.0',
        'folder': '/Users/my_email@fake_organization.com/libraries',
        'libType': 'python-egg',
        'files': ['47fb08a7-test-library_1_0_0_py3_6-e5f8c.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_6(prod_folder):
    return {
        'id': '6',
        'name': 'test-library-1.0.1',
        'folder': prod_folder,
        'libType': 'python-egg',
        'files': ['47fb08a7-test-library_1_0_1_py3_6-e5f8c.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_7(prod_folder):
    return {
        'id': '7',
        'name': 'test-library-1.0.2',
        'folder': prod_folder,
        'libType': 'python-egg',
        'files': ['47fb08a7-test-library_1_0_2_py3_6-e5f8c.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_8(prod_folder):
    return {
        'id': '8',
        'name': 'test-library-1.0.3',
        'folder': prod_folder,
        'libType': 'python-egg',
        'files': ['47fb08a7-test-library_1_0_3_py3_6-e5f8c.egg'],
        'attachAllClusters': False,
        'statuses': [],
    }


@pytest.fixture
def library_list_response(prod_folder):
    library_list_response = [
        {
            'id': '1',
            'name': 'awesome_library_b-4.2.3',
            'folder': prod_folder,
        },
        {
            'id': '2',
            'name': 'awesome_library_a-0.10.1',
            'folder': prod_folder,
        },
        {
            'id': '3',
            'name': 'test-library-0.0.0',
            'folder': prod_folder,
        },
        {
            'id': '4',
            'name': 'test-library-plus-stuff-0.0.0',
            'folder': prod_folder,
        },
        {
            'id': '5',
            'name': 'test-library-1.0.0',
            'folder': '/Users/my_email@fake_organization.com/libraries',
        },
        {
            'id': '6',
            'name': 'test-library-1.0.1',
            'folder': prod_folder,
        },
        {
            'id': '7',
            'name': 'test-library-1.0.2',
            'folder': prod_folder,
        },
        {
            'id': '8',
            'name': 'test-library-1.0.3',
            'folder': prod_folder,
        },
    ]

    return library_list_response


@pytest.fixture
def library_mapping():
    library_mapping = {
        '47fb08a7-test-library_1_0_2_py3_6-e5f8c.egg': FileNameMatch('test-library-1.0.2.egg'),
        '47fb08a7-test-library_1_0_1_py3_6-e5f8c.egg': FileNameMatch('test-library-1.0.1.egg'),
        '01832402-test-library-plus-stuff_0_0_0_py3_6-e5f8c.egg': FileNameMatch('test-library-plus-stuff-0.0.0.egg'),
        '47fb08a7-test-library_0_0_0_py3_6-e5f8c.egg': FileNameMatch('test-library-0.0.0.egg'),
        '996c949b-awesome_library_a_0_10_1_py3_6-266f.egg': FileNameMatch('awesome_library_a-0.10.1.egg'),
        '47fb08a7-awesome_library_b_4_2_3_py3_6-e5f8c.egg': FileNameMatch('awesome_library_b-4.2.3.egg'),
        '47fb08a7-test-library_1_0_3_py3_6-e5f8c.egg': FileNameMatch('test-library-1.0.3.egg'),
    }
    return library_mapping


@pytest.fixture
def existing_config():
    existing_config = ConfigParser()
    existing_config['DEFAULT'] = {
        'host': 'test_host',
        'token': 'test_token',
        'prod_folder': 'test_folder',
    }
    return existing_config

@pytest.fixture
def empty_config():
    empty_config = ConfigParser()
    empty_config['DEFAULT'] = {}
    return empty_config
