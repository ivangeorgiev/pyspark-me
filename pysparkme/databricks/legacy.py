import requests
from collections import namedtuple
import base64

from .connection import connect

DEFAULT_URL = 'https://westeurope.azuredatabricks.net/'

_bearer_token = None
_cluster_id = None
_url = None

DBFS_FileInfo = namedtuple('DBFS_FileInfo', 'path,is_dir,file_size')

def _get_headers():
    global _bearer_token
    return dict(
       Authorization = f'Bearer {_bearer_token}'
    )

def _get_url(endpoint):
    global _url
    url = f'{_url}/api/2.0/{endpoint}'
    return url

def _get_cluster_id(cluster_id=None):
    global _cluster_id
    return (cluster_id or _cluster_id)

def connect(bearer_token, url=None, cluster_id=None):
    global _bearer_token, _url, _cluster_id
    _bearer_token = bearer_token
    _url = url or DEFAULT_URL
    _cluster_id = cluster_id


def dbfs_ls(path):
    global _url, _bearer_token

    url = f'{_url}/api/2.0/dbfs/list'
    params = dict(path=path)
    headers = _get_headers()

    response_obj = requests.get(url,params=params,
            headers=headers)

    response = response_obj.json()
    if response.get('error_code', None) == 'RESOURCE_DOES_NOT_EXIST':
        raise FileNotFoundError(response['message'])
    elif response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])
    return [DBFS_FileInfo(**f) for f in response['files']]

def dbfs_mkdirs(path):
    global _url, _bearer_token

    url = f'{_url}/api/2.0/dbfs/mkdirs'
    params = dict(path=path)
    headers = _get_headers()

    response_obj = requests.post(url,params=params,
            headers=headers)

    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])

def dbfs_delete(path, recursive=False):

    url = _get_url('dbfs/delete')
    headers = _get_headers()
    params = dict(
        path=path,
        recursive=str(recursive).lower(),
        )

    response_obj = requests.post(
            url,
            params=params,
            headers=headers,
            )

    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])


def dbfs_read(path, offset=None, length=None, decoded=True):
    offset = offset or 0
    length = length or 1048576

    response_obj = requests.get(
        url=_get_url('dbfs/read'),
        params=dict(path=path,offset=offset,length=length),
        headers=_get_headers()
    )
    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])
    if decoded:
        response = base64.b64decode(response['data'])
    return response

def dbfs_read_all(path, chunk_size=None) -> bytes:
    chunk_size = chunk_size or 1048576
    content = b''
    offset = 0
    while (True):
        this_read = dbfs_read(
                path, 
                offset=offset,
                length=chunk_size,
                decoded=False)
        if not this_read['bytes_read']:
            break
        offset += this_read['bytes_read']
        content += base64.b64decode(this_read['data'])
    return content


def run_submit_notebook(path, params=None, run_name=None, cluster_id=None):
    cluster_id = _get_cluster_id(cluster_id)
    assert cluster_id, f"cluster_id not specified. Set cluster_id with connect or pass as parameter"

    params = params or {}

    run_name = None or "My Notebook Run"
    r = dict(
        run_name=run_name,
        existing_cluster_id=cluster_id,
        libraries=[],
        notebook_task=dict(
            notebook_path=path,
            base_parameters=params
        ),
    )


    response_obj = requests.post(
        url=_get_url('jobs/runs/submit'),
        headers=_get_headers(),
        json=r
    )

    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])
    return response['run_id']

def run_get(run_id):
    response_obj = requests.get(
        url=_get_url('jobs/runs/get'),
        params=dict(run_id=run_id),
        headers=_get_headers()
    )
    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])
    return response


def run_get_output(run_id):
    response_obj = requests.get(
        url=_get_url('jobs/runs/get-output'),
        params=dict(run_id=run_id),
        headers=_get_headers()
    )
    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])
    return response

def run_export(run_id):
    response_obj = requests.get(
        url=_get_url('jobs/runs/export'),
        params=dict(run_id=run_id),
        headers=_get_headers()
    )
    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])
    return response


def run_list(job_id=None, offset=None, limit=None,
        completed_only=False, active_only=False):
    assert not (completed_only and active_only), "Only one of completed_only or active_only could be True"
    params = dict()
    if job_id:
        params['job_id'] = job_id
    if offset:
        params['offset'] = offset
    if limit:
        params['limit'] = limit
    if completed_only:
        params['completed_only'] = 'true'
    if active_only:
        params['active_only'] = 'true'

    response_obj = requests.get(
        url=_get_url('jobs/runs/list'),
        params=params,
        headers=_get_headers()
    )
    response = response_obj.json()
    if response.get('error_code', None):
        raise Exception(response['error_code'] + ' ' + response['message'])
    return response
