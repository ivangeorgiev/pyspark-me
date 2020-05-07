from .common import Api, Link, DatabricksLinkException, ERR_RESOURCE_DOES_NOT_EXIST

class Workspace(Api):
    def __init__(self, link):
        super().__init__(link, path='workspace')

    def list(self, path=None):
        get_result = self.link.get(
            self.path('list'),
            params=dict(path=(path or '/')))
        return get_result.get('objects', [])

    def exists(self, path):
        try:
            self.list(path)
            result = True
        except DatabricksLinkException as exc:
            if exc.error_code == ERR_RESOURCE_DOES_NOT_EXIST:
                result = False
        return result
