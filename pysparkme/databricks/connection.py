import requests
from .common import Link
from .workspace import Workspace
from .jobs import Jobs
from .dbfs import DBFS


class DatabricksConnection:
    def __init__(self, bearer_token, url=None, cluster_id=None):
        self._link = Link(bearer_token, url, cluster_id)

    @property
    def link(self) -> Link:
        return self._link

    @property
    def workspace(self) -> Workspace:
        if not getattr(self, "_workspace", None):
            self._workspace = Workspace(self.link)
        return self._workspace

    @property
    def jobs(self) -> Jobs:
        if not getattr(self, "_jobs", None):
            self._jobs = Jobs(self.link)
        return self._jobs

    @property
    def dbfs(self) -> DBFS:
        if not getattr(self, "_dbfs", None):
            self._dbfs = DBFS(self.link)
        return self._dbfs

def connect(bearer_token, url=None, cluster_id=None) -> DatabricksConnection:
    return DatabricksConnection(bearer_token, url, cluster_id)

