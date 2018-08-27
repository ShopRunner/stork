import pytest
import requests


@pytest.mark.unit
def test_permission_basic(token, host):
    res1 = requests.get(
        host + '/api/2.0/libraries/all-cluster-statuses',
        auth=('token', token),
    )
    assert res1.status_code == 200


@pytest.mark.unit
def test_permission_admin(token, host):
    res1 = requests.get(
        host + '/api/1.2/libraries/list',
        auth=('token', token),
    )
    assert res1.status_code == 200

    library_id = res1.json()[0]['id']
    res2 = requests.get(
        host + '/api/1.2/libraries/status?libraryId={}'
        .format(library_id),
        auth=('token', token),
    )
    assert res2.status_code == 200
