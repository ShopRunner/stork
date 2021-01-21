"""
This file handles all the logic and API calls involved in creating
an interactive Databricks cluster configured as a specific job cluster.
"""
import json
import time

import requests
from configparser import NoOptionError

from .api_error import APIError
from .configure import _load_config, CFG_FILE, PROFILE


def get_job_cluster_config(job_id, token, host):
    """
    Get cluster config for a job.

    Parameters
    ----------
    job_id: int
        id of the job you are trying to debug
    token: string
        Databricks API key
    host: string
        Databricks host (e.g. https://my-organization.cloud.databricks.com)
    """
    res = requests.get(
        host + f'/api/2.0/jobs/get/?job_id={job_id}',
        auth=('token', token),
    )

    if res.status_code != 200:
        raise APIError(res)
    else:
        cluster_config = res.json()['settings']
        if 'existing_cluster_id' in cluster_config.keys():
            raise Exception(f"""
                This job uses an interactive cluster:
                {cluster_config['existing_cluster_id']}.
            """)
        return cluster_config


def create_new_cluster(job_id, cluster_name, cluster_config, token, host):
    """
    Creat a new cluster based on a job cluster config.

    Parameters
    ----------
    job_id: int
        id of the job you are trying to debug
    cluster_name: string
        Name for your cluster, will be default if None
    cluster_config: dict
        dict containing the config details of the job cluster
    token: string
        Databricks API key
    host: string
        Databricks host (e.g. https://my-organization.cloud.databricks.com)

    Side Effects
    ------------
    Creates a new cluster on Databricks
    """
    if cluster_name is None:
        current_time = time.gmtime()

        current_time_formatted = time.strftime('%Y%m%d_%H%M%S', current_time)

        cluster_name = f'private-debug-job-{job_id}-{current_time_formatted}'

    data = {
        'cluster_name': cluster_name,
        'spark_version': cluster_config['new_cluster']['spark_version'],
        'node_type_id': cluster_config['new_cluster']['node_type_id'],
        'aws_attributes': cluster_config['new_cluster']['aws_attributes'],
        'autotermination_minutes': 120
    }

    if 'autoscale' in cluster_config['new_cluster'].keys():
        data['autoscale'] = cluster_config['new_cluster']['autoscale']

    if 'driver_node_type_id' in cluster_config['new_cluster'].keys():
        data['driver_node_type_id'] = (
            cluster_config['new_cluster']['driver_node_type_id']
        )

    if 'num_workers' in cluster_config['new_cluster'].keys():
        data['num_workers'] = cluster_config['new_cluster']['num_workers']

    if 'spark_conf' in cluster_config['new_cluster'].keys():
        data['spark_conf'] = cluster_config['new_cluster']['spark_conf']

    res = requests.post(
        host + '/api/2.0/clusters/create',
        auth=('token', token),
        data=json.dumps(data)
    )

    if res.status_code != 200:
        raise APIError(res)
    else:
        cluster_id = res.json()['cluster_id']
        return cluster_id, cluster_name


def attach_job_libraries_to_cluster(cluster_id, cluster_config, token, host):
    """
    Attach job libraries to cluster

    Parameters
    ----------
    cluster_id: int
        id of the cluster you want to attach libraries to
    cluster_config: dict
        dict containing the config details of the job cluster
    token: string
        Databricks API key
    host: string
        Databricks host (e.g. https://my-organization.cloud.databricks.com)

    Side Effects
    ------------
    Attaches libraries to a cluster on Databricks
    """
    data = {
        'cluster_id': cluster_id,
        'libraries': cluster_config['libraries']
    }

    res = requests.post(
        host + '/api/2.0/libraries/install',
        auth=('token', token),
        data=json.dumps(data)
    )

    if res.status_code != 200:
        raise APIError(res)


def create_job_library(logger, job_id, cluster_name, token):
    """
    Pull down a job cluster config, creates a new cluster with that config,
    and attaches job libraries to cluster

    Parameters
    ----------
    logger: logging object
        configured in cli_commands.py
    job_id: int
        id of the job you are trying to debug
    cluster_name: string
        Name for your cluster, will be default if None
    token: string
        Databricks API key

    Side Effects
    ------------
    creates new cluster in Databricks
    """

    config = _load_config(CFG_FILE)
    try:
        host = config.get(PROFILE, 'host')
    except NoOptionError:
        raise ValueError('no host provided: please run `stork configure`'
                         ' to get set up')

    try:
        cluster_config = get_job_cluster_config(job_id, token, host)

        cluster_id, cluster_name = create_new_cluster(
            job_id,
            cluster_name,
            cluster_config,
            token,
            host
        )

        logger.info(
            f'Cluster {cluster_name} will come up in 20 seconds'
        )

        time.sleep(20)  # Wait for cluster to be up before attaching libraries

        attach_job_libraries_to_cluster(
            cluster_id,
            cluster_config,
            token,
            host
        )

        logger.info(
            f'New cluster {cluster_name} created on Databricks'
        )
    except APIError as err:
        raise err
