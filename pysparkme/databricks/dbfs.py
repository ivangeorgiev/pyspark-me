import base64
from .common import Api, DatabricksLinkException, ERR_RESOURCE_DOES_NOT_EXIST
from .common import bite_size_str

class DBFS(Api):
    def __init__(self, link):
        super().__init__(link, path='dbfs')

    def list(self, path=None, humanize=False):
        get_result = self.link.get(
            self.path('list'),
            params=dict(path=(path or '/')))
        files = get_result.get('files', [])
        if (humanize):
            for f in files:
                f['bite_size'] = bite_size_str(f['file_size'])
        return files

    def ls(self, path=None, humanize=False):
        return self.list(path, humanize)

    def exists(self, path):
        try:
            self.list(path)
            result = True
        except DatabricksLinkException as exc:
            if exc.error_code == ERR_RESOURCE_DOES_NOT_EXIST:
                result = False
        return result

    def read(self, path, offset=None, length=None, decoded=True):
        offset = offset or 0
        length = length or 1048576

        response = self.link.get(
            self.path('read'),
            params=dict(path=path,offset=offset,length=length),)
        if decoded:
            response = base64.b64decode(response['data'])
        return response

    def read_all(self, path, chunk_size=None) -> bytes:
        chunk_size = chunk_size or 1048576
        content = b''
        offset = 0
        while (True):
            this_read = self.read(
                    path, 
                    offset=offset,
                    length=chunk_size,
                    decoded=False)
            if not this_read['bytes_read']:
                break
            offset += this_read['bytes_read']
            content += base64.b64decode(this_read['data'])
        return content

    def mkdirs(self, path):
        response = self.link.post(
            self.path('mkdirs'),
            params=dict(path=path))
        return response

    def delete(self, path, recursive=False):
        response = self.link.post(
            self.path('delete'),
            params=dict(path=path,
                        recursive=str(recursive).lower()))
        return response
