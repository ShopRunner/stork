"""
The core of the program, update_databricks_library handles all the logic and
 API calls involved in creating continuous deployment for Python packages
 in Databricks.
"""
import json

import requests
from configparser import NoOptionError
from simplejson.errors import JSONDecodeError

from .configure import _load_config, CFG_FILE, PROFILE


class APIError(Exception):
    """
    exception to handle unifying two generations of API error responses
     from Databricks
    """
    def __init__(self, response):
        Exception.__init__(self, response)
        try:
            res_body = response.json()
        except JSONDecodeError:
            self.code = 'http {}'.format(response.status_code)
            # non-json error message, didn't bother parsing neatly
            self.message = response.text
        else:
            if 'error_code' in res_body.keys():
                self.code = res_body['error_code']
                self.message = res_body['message']
            else:
                self.code = 'http {}'.format(response.status_code)
                self.message = res_body['error']

    def __str__(self):
        return '{}: {}'.format(self.code, self.message)


def load_egg(filename, library_name, version, folder, token, host):
    """
    upload an egg to the Databricks filesystem.

    Parameters
    ----------
    filename: string
        local location of file to upload
    library_name: string
        base name of library (e.g. 'test_library')
    version: string
        version of library (e.g. '1.0.0')
    folder: string
        Databricks folder to upload to
        (e.g. '/Users/htorrence@shoprunner.com/')
    token: string
        Databricks API key
    host: string
        Databricks host (e.g. https://my-organization.cloud.databricks.com)

    Side Effects
    ------------
    uploads egg to Databricks
    """
    res = requests.post(
        host + '/api/1.2/libraries/upload',
        auth=('token', token),
        data={
            'libType': 'python-egg',
            'name': '{0}-{1}'.format(library_name, version),
            'folder': folder,
        },
        files={'uri': open(filename, 'rb')}
    )

    if res.status_code != 200:
        raise APIError(res)


def get_job_list(library_name, major_version, library_mapping, token, host):
    """
    get a list of jobs using the major version of the given library

    Parameters
    ----------
    library_name: string
        base name of library (e.g. 'test_library')
    major_version: string
        major version of library (e.g. '1')
    library_mapping: dict
        first element of get_library_mapping output
    token: string
        Databricks API key
    host: string
        Databricks host (e.g. https://my-organization.cloud.databricks.com)

    Returns
    -------
    list of dictionaries containing the job id, job name, and library path
     for each job
    """
    res = requests.get(
        host + '/api/2.0/jobs/list',
        auth=('token', token),
    )
    if res.status_code == 200:
        job_list = []
        if len(res.json()['jobs']) == 0:
            return []
        for job in res.json()['jobs']:
            if 'libraries' in job['settings'].keys():
                for library in job['settings']['libraries']:
                    if 'egg' in library.keys():
                        try:  # if in prod_folder, mapping turns uri into name
                            job_library_uri = library['egg'].split('/')[-1]
                            job_library = library_mapping[job_library_uri]
                        except KeyError:
                            pass
                        else:
                            try:  # if we can parse a version, otherwise ignore
                                job_library_name = (
                                    '-'.join(job_library.split('-')[:-1])
                                )
                                job_major_version = (
                                    job_library.split('-')[-1].split('.')[0]
                                )
                            except IndexError:
                                pass
                            else:
                                if (
                                    (library_name == job_library_name)
                                    and (job_major_version == major_version)
                                ):
                                    # append jobs we want to update later
                                    job_list.append({
                                        'job_id': job['job_id'],
                                        'job_name': job['settings']['name'],
                                        'library_path': library['egg'],
                                    })
        return job_list
    else:
        raise APIError(res)


def get_library_mapping(prod_folder, token, host):
    """
    returns a pair of library mappings, the first mapping library uri to a
     library name for all libraries in the production folder, and the second
     mapping library name to info for libraries in the production folder with
     parsable versions

    Parameters
    ----------
    prod_folder: string
        name of folder in Databricks UI containing production libraries
    token: string
        Databricks API key
    host: string
        Databricks account url
        (e.g. https://fake-organization.cloud.databricks.com)

    Returns
    -------
    dictionary mapping a library uri to a library name
    dictionary mapping library UI path to base name, major version,
        minor version, and id number
    """
    res = requests.get(
        host + '/api/1.2/libraries/list',
        auth=('token', token),
    )
    if res.status_code == 200:
        library_list = res.json()
        library_map = {}
        id_nums = {}
        for library in library_list:
            status_res = (
                requests
                .get(
                    host + '/api/1.2/libraries/status?libraryId={}'
                    .format(library['id']),
                    auth=('token', token),
                )
            )
            if status_res.status_code == 200:
                library_info = status_res.json()
                # only do any of this for libraries in the ds folder
                if library_info['folder'] != prod_folder:
                    continue
                library_map[library_info['files'][0]] = library_info['name']
                try:
                    name_split = library_info['name'].split('-')
                    name_split = (
                        ['-'.join(name_split[:-1])] + name_split[-1].split('.')
                    )
                    # map name to info
                    # we'll need the id number to clean up old libraries
                    id_nums[library_info['name']] = {
                        'name': name_split[0],
                        'major_version': name_split[1],
                        'minor_version': '.'.join(name_split[2:]),
                        'id_num': library_info['id'],
                    }
                except IndexError:
                    pass
            else:
                raise APIError(status_res)
        return library_map, id_nums
    else:
        raise APIError(res)


def update_job_libraries(job_list, new_library_path, token, host):
    """
    update libraries on jobs using same major version

    Parameters
    ----------
    job_list: list of strings
        output of get_job_list
    new_library_path: string
        path to library in dbfs (including uri)
    token: string
        Databricks API key with admin permissions
    host: string
        Databricks account url
        (e.g. https://fake-organization.cloud.databricks.com)


    Side Effects
    ------------
    jobs now require updated version of library
    """

    for job in job_list:
        get_res = requests.get(
            host + '/api/2.0/jobs/get?job_id={}'.format(job['job_id']),
            auth=('token', token),
        )
        if get_res.status_code == 200:
            job_specs = get_res.json()  # copy current job specs
            settings = job_specs['settings']
            job_specs.pop('settings')
            new_libraries = []
            for lib in settings['libraries']:
                if 'egg' in lib.keys() and lib['egg'] == job['library_path']:
                    # replace entry for old library path with new one
                    new_libraries.append({'egg': new_library_path})
                else:
                    new_libraries.append(lib)
            settings['libraries'] = new_libraries
            job_specs['new_settings'] = settings
            post_res = requests.post(
                host + '/api/2.0/jobs/reset',
                auth=('token', token),
                data=json.dumps(job_specs)
            )
            if post_res.status_code != 200:
                raise APIError(post_res)
        else:
            raise APIError(get_res)


def delete_old_versions(
    library_name,
    major_version,
    minor_version,
    id_nums,
    token,
    prod_folder,
    host
):
    """
    delete any other versions of the same library where:
        it has the same major version
        it has a smaller minor version
        it lives in prod_folder

    Parameters
    ----------
    library_name: string
        base name of library (e.g. 'test_library')
    major_version: string
        major version of library (e.g. '1')
    minor_version: string
        minor version of library (e.g. '1.0')
    id_nums: dict
        second output of get_library_mapping
    token: string
        Databricks API key with admin permissions
    prod_folder: string
        name of folder in Databricks UI containing production libraries
    host: string
        Databricks account url
        (e.g. https://fake-organization.cloud.databricks.com)

    Side Effects
    ------------
    delete any other versions of the same library with the same major version
        and smaller minor versions
    """

    old_versions = []
    for name, lib in id_nums.items():
        if (
            lib['name'] == library_name
            and int(lib['major_version']) == int(major_version)
            and float(lib['minor_version']) < float(minor_version)
        ):
            old_versions.append(name)
            res = requests.post(
                host + '/api/1.2/libraries/delete',
                auth=('token', token),
                data={'libraryId': lib['id_num']},
            )
            if res.status_code != 200:
                raise APIError(res)
    return old_versions


def update_databricks(path, token, folder, update_jobs, cleanup):
    """
    upload library, update jobs using the same major version,
    and delete libraries with the same major and lower minor versions
    (depending on update_jobs and cleanup flags)

    Parameters
    ----------
    path: string
        path with name of egg as output from setuptools
        (e.g. dist/new_library-1.0.0-py3.6.egg)
    token: string
        Databricks API key
    folder: string
        Databricks folder to upload to
        (e.g. '/Users/my_email@fake_organization.com/')
    update_jobs: bool
        if true, jobs using this library will be updated to point to the
            new version
        if false, will not touch jobs or other library versions at all
    cleanup: bool
        if true, outdated libraries will be deleted
        if false, nothing will be deleted

    Side Effects
    ------------
    new library in Databricks
    if update_jobs is true, then updated jobs
    if update_jobs and cleanup are true, removed outdated libraries
    """

    config = _load_config(CFG_FILE)
    try:
        host = config.get(PROFILE, 'host')
    except NoOptionError:
        raise ValueError('no host provided: please run `apparate configure`'
                         ' to get set up')
    try:
        prod_folder = config.get(PROFILE, 'prod_folder')
    except NoOptionError:
        raise ValueError('no prod_folder provided: please run '
                         '`apparate configure` to get set up')

    # strip off prepended folders
    filename = path.split('/')[-1]
    # strip off version and '-py3.6.egg'
    library_name = '-'.join(filename.split('-')[:-2])
    # get version
    version = filename.split('-')[-2]
    # get major version (e.g. 1) and minor version (e.g. 0.0)
    major_version, minor_version = version.split('.', 1)

    try:
        load_egg(path, library_name, version, folder, token, host)
        print(
            'new egg {}-{} loaded to Databricks'
            .format(library_name, version)
        )
    except APIError as err:
        if err.code == 'http 500' and 'already exists' in err.message:
            print(
                'this version ({}) already exists:'.format(version) +
                'if a change has been made please update your version number'
            )
            return
        else:
            raise err

    if update_jobs and folder == prod_folder:
        library_map, id_nums = get_library_mapping(prod_folder, token, host)
        library_uri = [
            uri for uri, name in library_map.items()
            if name == '{}-{}'.format(library_name, version)
        ][0]
        library_path = 'dbfs:/FileStore/jars/' + library_uri

        job_list = get_job_list(
            library_name,
            major_version,
            library_map,
            token,
            host
        )
        print(
            'current major version of library used by jobs: {}'
            .format(', '.join([i['job_name'] for i in job_list]))
        )

        if len(job_list) != 0:
            update_job_libraries(job_list, library_path, token, host)
            print(
                'updated jobs: {}'
                .format(', '.join([i['job_name'] for i in job_list]))
            )

        if cleanup:
            old_versions = delete_old_versions(
                library_name=library_name,
                major_version=major_version,
                minor_version=minor_version,
                id_nums=id_nums,
                token=token,
                prod_folder=prod_folder,
                host=host,
            )
            print('removed old versions: {}'.format(', '.join(old_versions)))
