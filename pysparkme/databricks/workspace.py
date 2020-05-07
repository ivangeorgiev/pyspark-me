import enum
import base64
from typing import List
from dataclasses import dataclass, field, asdict
from .common import Api, Link, DatabricksLinkException, ERR_RESOURCE_DOES_NOT_EXIST

class ExportFormat(enum.Enum):
    SOURCE = 'SOURCE',
    HTML = 'HTML',
    JUPYTER = 'JUPYTER',
    DBC = 'DBC'

@dataclass
class ObjectInfo:
    object_type: str
    object_id: str
    path: str
    language: str = None
    is_directory: bool = None
    is_notebook: bool = None
    is_library: bool = None

    def __post_init__(self):
        self.is_notebook = self.object_type == 'NOTEBOOK'
        self.is_directory = self.object_type == 'DIRECTORY'
        self.is_library = self.object_type == 'LIBRARY'


    def __dict__(self):
        return asdict(self)

class Workspace(Api):
    def __init__(self, link):
        super().__init__(link, path='workspace')

    def ls(self, path=None) -> List[ObjectInfo]:
        response = self.link.get(
            self.path('list'),
            params=dict(path=(path or '/')))
        objects = [ObjectInfo(**obj) for obj in response.get('objects', [])]
        return objects

    def exists(self, path):
        try:
            self.ls(path)
            result = True
        except DatabricksLinkException as exc:
            if exc.error_code == ERR_RESOURCE_DOES_NOT_EXIST:
                result = False
        return result

    def is_directory(self, path):
        if path == '/':
            return True
        item = self.ls(path)[0]
        return item.is_directory

    def export(self, path:str, format:ExportFormat=ExportFormat.SOURCE) -> bytes:
        format = ExportFormat[format] if isinstance(format, str) else format
        assert isinstance(format, ExportFormat), 'format must be ExportFormat or str instance'
        link_response = self.link.get(
            self.path('export'),
            params=dict(path=path,
                        format=format.name))
        # return link_response
        content = base64.b64decode(link_response['content'])
        return content

