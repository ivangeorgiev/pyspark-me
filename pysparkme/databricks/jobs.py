from .common import Api
from .runs import Runs

class Jobs(Api):
    def __init__(self, link):
        super().__init__(link, path='jobs')

    def list(self, path=None):
        list_result = self.link.get(
            self.path('list'),
            params=dict(path=(path or '/')))
        return list_result.get('jobs', [])

    @property
    def runs(self) -> Runs:
        if not getattr(self, "_runs", None):
            self._runs = Runs(self.link)
        return self._runs
