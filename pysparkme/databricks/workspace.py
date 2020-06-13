import enum
import base64
from typing import List
from .common import Api, Link, DatabricksLinkException, ERR_RESOURCE_DOES_NOT_EXIST
from .databricks_data_classes import *

class ExportFormat(enum.Enum):
    SOURCE = 'SOURCE',
    HTML = 'HTML',
    JUPYTER = 'JUPYTER',
    DBC = 'DBC'

class Workspace(Api):
    def __init__(self, link):
        super().__init__(link, path='workspace')

    def ls(self, path=None) -> List[DatabricksObjectInfo]:
        response = self.link.get(
            self.path('list'),
            params=dict(path=(path or '/')))
        objects = [DatabricksObjectInfo(**obj) for obj in response.get('objects', [])]
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
        return item.path != path

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

