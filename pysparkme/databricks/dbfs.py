import base64
from dataclasses import dataclass, field
from typing import List
from .common import Api, DatabricksLinkException, ERR_RESOURCE_DOES_NOT_EXIST
from .common import bite_size_str
from .common import DataClass

@dataclass
class FileInfo(DataClass):
    path: str
    is_dir: bool
    file_size: int
    is_file: bool = None
    human_size: str = field(init=False, compare=False)

    def __post_init__(self):
        self.human_size = bite_size_str(self.file_size)
        self.is_file = not self.is_dir

class DBFS(Api):
    def __init__(self, link):
        super().__init__(link, path='dbfs')

    def list(self, path=None) -> List[FileInfo]:
        get_result = self.link.get(
            self.path('list'),
            params=dict(path=(path or '/')))
        files = get_result.get('files', [])
        result = [FileInfo(**f) for f in files]
        return result

    def info(self, path=None) -> FileInfo:
        response = self.link.get(
            self.path('get-status'),
            params=dict(path=(path or '/')))
        result = FileInfo(**response)
        return result

    def ls(self, path=None) -> List[FileInfo]:
        return self.list(path)

    def exists(self, path) -> bool:
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

    def rm(self, path, recursive=False):
        return self.delete(path, recursive)
