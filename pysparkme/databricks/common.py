import bisect
import requests
import dataclasses
import uuid

DEFAULT_URL = 'https://westeurope.azuredatabricks.net'

ERR_RESOURCE_DOES_NOT_EXIST = 'RESOURCE_DOES_NOT_EXIST'

class Link:
    def __init__(self, bearer_token, url:str=None, cluster_id=None):
        url or DEFAULT_URL
        if url.endswith('/'):
            url = url[:-1]
        self._bearer_token = bearer_token
        self._url = url or DEFAULT_URL
        self._cluster_id = cluster_id

    @property
    def bearer_token(self) -> str:
        return self._bearer_token

    @property
    def url(self) -> str:
        return self._url

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    def _get_url(self, verb):
        url = f'{self.url}/api/2.0/{verb}'
        return url

    def _get_headers(self):
        return dict(
            Authorization = f'Bearer {self.bearer_token}'
        )


    def get(self, verb, params=None):
        response_obj = requests.get(
                url=self._get_url(verb),
                params=(params or {}),
                headers=self._get_headers()
            )
        response = response_obj.json()
        if response.get('error_code', None):
            raise DatabricksLinkException(response['error_code'], response['message'])
        return response

    def post(self, verb, params=None, json=None):
        args = dict(
            url=self._get_url(verb),
            headers=self._get_headers()
        )
        if params:
            args['params'] = params
        if json:
            args['json'] = json
        response_obj = requests.post(**args)
        response = response_obj.json()
        if response.get('error_code', None):
            raise DatabricksLinkException(response['error_code'], response['message'])
        return response



class Api:
    def __init__(self, link: Link, path=None):
        self._link = link
        self._path = path or ''
   
    @property
    def link(self) -> Link:
        return self._link

    def path(self, action):
        "Get path to a resource, relative to the root_path"
        action_path = "{}/{}".format(self._path, action)
        return action_path

class DatabricksLinkException(Exception):
    def __init__(self, error_code, message):
        self.error_code = error_code
        self.message = message
        super().__init__('{}: {}'.format(error_code, message))


def bite_size_str(size, format_string=None, base=1000):
    sizes = [base ** 0, base**1, base**2, base**3, base**4,
             base**5, base**6, base**7, base**8]
    prefixes = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
    i = bisect.bisect(sizes, size)
    i = 0 if (i<1) else i-1
    number_part = size/sizes[i]
    format_string = format_string or "{0:.3n} {1}B" 
    return format_string.format(size/sizes[i], prefixes[i])


def random_id(length, namespace=''):
    namespace_url = 'https://pypi.org/project/pyspark-me/#' + namespace
    return str(uuid.uuid5(uuid.NAMESPACE_URL, namespace_url)).replace('-','')[:length]

