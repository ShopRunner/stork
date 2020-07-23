"""
The core of the program, update_databricks_library handles all the logic and
 API calls involved in creating continuous deployment for Python packages
 in Databricks.
"""
import json
import re
from os.path import basename

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


class FileNameError(Exception):
    """
    exception to handle when filename is not of correct pattern
    """
    def __init__(self, filename):
        Exception.__init__(
            self,
            'Filename \'{}\' was not correct pattern'.format(filename)
        )
        self.filename = filename


class FileNameMatch(object):
    """
    Matches eggs or jars for both released and snapshot versions

    Supported Patterns:
      new_library-1.0.0-py3.6.egg
      new_library-1.0.0-SNAPSHOT-py3.6.egg
      new_library-1.0.0-SNAPSHOT-my-branch-py3.6.egg

      new_library-1.0.0.egg
      new_library-1.0.0-SNAPSHOT.egg
      new_library-1.0.0-SNAPSHOT-my-branch.egg

      new_library-1.0.0.jar
      new_library-1.0.0-SNAPSHOT.jar
      new_library-1.0.0-SNAPSHOT-my-branch.jar

    library_name: string
        base name of library (e.g. 'test_library')
    version: string
        version of library (e.g. '1.0.0')

    """
    file_pattern = (
        r'([a-zA-Z0-9-\._]+)-((\d+)\.(\d+\.\d+)'
        r'(?:-SNAPSHOT(?:[a-zA-Z_\-\.]+)?)?)(?:-py.+)?\.(egg|jar)'
    )

    def __init__(self, filename):
        match = re.match(FileNameMatch.file_pattern, filename)
        try:
            self.filename = filename
            self.library_name = match.group(1)
            self.version = match.group(2)
            self.major_version = match.group(3)
            self.minor_version = match.group(4)
            self.suffix = match.group(5)
            if self.suffix == 'jar':
                self.lib_type = 'java-jar'
            elif self.suffix == 'egg':
                self.lib_type = 'python-egg'
        except (IndexError, AttributeError):
            raise FileNameError(filename)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            self_attrs = {k: v for k, v in vars(self).items()}
            self_attrs.pop('filename')
            other_attrs = {k: v for k, v in vars(other).items()}
            other_attrs.pop('filename')
            return self_attrs == other_attrs
        else:
            return False

    def replace_version(self, other, logger):
        """
        True if self can safely replace other

        based on version numbers only - snapshot and branch tags are ignored
        """

        if other.library_name != self.library_name:
            logger.debug(
                'not replacable: {} != {} ()'
                .format(other.library_name, self.library_name)
            )
            return False
        elif int(other.major_version) != int(self.major_version):
            logger.debug(
                'not replacable: {} != {} ({})'
                .format(
                    int(self.major_version),
                    int(other.major_version),
                    other.filename,
                )
            )
            return False
        elif float(other.minor_version) >= float(self.minor_version):
            logger.debug(
                'not replacable: {} >= {} ({})'
                .format(
                    other.minor_version,
                    self.minor_version,
                    other.filename,
                )
            )
            return False
        else:
            return True


def load_library(filename, match, folder, token, host):
    """
    upload an egg to the Databricks filesystem.

    Parameters
    ----------
    filename: string
        local location of file to upload
    match: FilenameMatch object
        match object with library_type, library_name, and version
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
    with open(filename, 'rb') as file_obj:
        res = requests.post(
            host + '/api/1.2/libraries/upload',
            auth=('token', token),
            data={
                'libType': match.lib_type,
                'name': '{0}-{1}'.format(match.library_name, match.version),
                'folder': folder,
            },
            files={'uri': file_obj}
        )

    if res.status_code != 200:
        raise APIError(res)


def get_job_list(logger, match, library_mapping, token, host):
    """
    get a list of jobs using the major version of the given library

    Parameters
    ----------
    logger: logging object
        configured in cli_commands.py
    match: FilenameMatch object
        match object with suffix
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
            logger.debug('job: {}'.format(job['settings']['name']))
            if 'libraries' in job['settings'].keys():
                for library in job['settings']['libraries']:
                    if match.suffix in library.keys():
                        try:  # if in prod_folder, mapping turns uri into name
                            job_library_uri = basename(library[match.suffix])
                            job_match = library_mapping[job_library_uri]
                        except KeyError:
                            logger.debug(
                                'not in library map: {}'
                                .format(job_library_uri)
                            )
                            pass
                        else:
                            if match.replace_version(job_match, logger):
                                job_list.append({
                                    'job_id': job['job_id'],
                                    'job_name': job['settings']['name'],
                                    'library_path': library[match.suffix],
                                })
                            else:
                                logger.debug(
                                    'not replacable: {}'
                                    .format(job_match.filename)
                                )
                    else:
                        logger.debug(
                            'no matching suffix: looking for {}, found {}'
                            .format(match.suffix, str(library.keys()))
                        )
        return job_list
    else:
        raise APIError(res)


def get_library_mapping(logger, prod_folder, token, host):
    """
    returns a pair of library mappings, the first mapping library uri to a
     library name for all libraries in the production folder, and the second
     mapping library name to info for libraries in the production folder with
     parsable versions

    Parameters
    ----------
    logger: logging object
        configured in cli_commands.py
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
                # only do any of this for libraries in the production folder
                if library_info['folder'] != prod_folder:
                    logger.debug(
                        'excluded folder: {} in {}, not prod folder ({})'
                        .format(
                            library_info['name'],
                            library_info['folder'],
                            prod_folder,
                        )
                    )
                    continue
                if library_info['libType'] == 'python-egg':
                    full_name = library_info['name'] + '.egg'
                elif library_info['libType'] == 'java-jar':
                    full_name = library_info['name'] + '.jar'
                else:
                    logger.debug(
                        'excluded library type: {} is of libType {}, '
                        'not jar or egg'
                        .format(
                            library_info['name'],
                            library_info['libType'],
                        )
                    )
                    continue
                try:
                    name_match = FileNameMatch(full_name)
                    # map uri to name match object
                    library_map[library_info['files'][0]] = name_match
                    # map name to name match object and id number
                    # we'll need the id number to clean up old libraries
                    id_nums[library_info['name']] = {
                        'name_match': name_match,
                        'id_num': library_info['id'],
                    }
                except FileNameError:
                    logger.debug(
                        'FileNameError: {} file name is not parsable'
                        .format(full_name)
                    )
                    pass
            else:
                raise APIError(status_res)
        return library_map, id_nums
    else:
        raise APIError(res)


def update_job_libraries(
    logger,
    job_list,
    match,
    new_library_path,
    token,
    host,
):
    """
    update libraries on jobs using same major version

    Parameters
    ----------
    logger: logging object
        configured in cli_commands.py
    job_list: list of strings
        output of get_job_list
    match: FilenameMatch object
        match object with suffix
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
                if (
                    match.suffix in lib.keys()
                    and lib[match.suffix] == job['library_path']
                ):
                    # replace entry for old library path with new one
                    new_libraries.append({match.suffix: new_library_path})
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
    logger,
    new_library_match,
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
    logger: logging object
        configured in cli_commands.py
    match: FilenameMatch object
        match object with library_name_, major_version, minor_version
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
        if new_library_match.replace_version(lib['name_match'], logger):
            old_versions.append(lib['name_match'].filename)
            res = requests.post(
                host + '/api/1.2/libraries/delete',
                auth=('token', token),
                data={'libraryId': lib['id_num']},
            )
            if res.status_code != 200:
                raise APIError(res)
    return old_versions


def update_databricks(logger, path, token, folder, update_jobs, cleanup):
    """
    upload library, update jobs using the same major version,
    and delete libraries with the same major and lower minor versions
    (depending on update_jobs and cleanup flags)

    Parameters
    ----------
    logger: logging object
        configured in cli_commands.py
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

    match = FileNameMatch(basename(path))

    try:
        load_library(path, match, folder, token, host)
        logger.info(
            'new library {}-{} loaded to Databricks'
            .format(match.library_name, match.version)
        )
    except APIError as err:
        if err.code == 'http 500' and 'already exists' in err.message:
            logger.info(
                'This version ({}) already exists: '.format(match.version) +
                'if a change has been made please update your version number. '
                'Note this error can also occur if you are uploading a jar '
                'and an egg already exists with the same name and version, '
                'or vice versa. In this case you will need to choose a '
                'different library name or a different folder for either the '
                'egg or the jar.'
            )
            return
        else:
            raise err

    if update_jobs and folder == prod_folder:
        library_map, id_nums = get_library_mapping(
            logger,
            prod_folder,
            token,
            host,
        )
        library_uri = [
            uri for uri, tmp_match in library_map.items()
            if (
                match.library_name == tmp_match.library_name
                and match.version == tmp_match.version
            )
        ][0]
        library_path = 'dbfs:/FileStore/jars/' + library_uri

        job_list = get_job_list(logger, match, library_map, token, host)
        logger.info(
            'current major version of library used by jobs: {}'
            .format(', '.join([i['job_name'] for i in job_list]))
        )

        if len(job_list) != 0:
            update_job_libraries(
                logger,
                job_list,
                match,
                library_path,
                token,
                host,
            )
            logger.info(
                'updated jobs: {}'
                .format(', '.join([i['job_name'] for i in job_list]))
            )

        if cleanup:
            old_versions = delete_old_versions(
                logger,
                match,
                id_nums=id_nums,
                token=token,
                prod_folder=prod_folder,
                host=host,
            )
            logger.info(
                'removed old versions: {}'.format(', '.join(old_versions))
            )
