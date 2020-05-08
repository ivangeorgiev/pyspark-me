import datetime
from dataclasses import dataclass, field
from typing import List
from .common import Api
from .common import DataClass

@dataclass
class DatabricksRun(DataClass):
    job_id: int
    run_id: int
    creator_user_name: str = None
    number_in_job: int = None
    original_attempt_run_id: int = None
    state: dict = None
    schedule: dict = None
    task: dict = None
    cluster_spec: dict = None
    cluster_instance: dict = None
    overriding_parameters: dict = None
    start_time: int = None
    setup_duration: int = None
    execution_duration: int = None
    cleanup_duration: int = None
    trigger: dict = None
    run_name: str = None
    run_page_url: str = None
    run_type: str = None

@dataclass
class RunList(DataClass):
    runs: List[DatabricksRun]
    has_more: bool


class Runs(Api):
    def __init__(self, link):
        super().__init__(link, path='jobs/runs')

     def submit_notebook(self, path, params=None, run_name=None, cluster_id=None):
        cluster_id = cluster_id or self.link.cluster_id
        assert cluster_id, f"cluster_id not specified. Set cluster_id with connect or pass as parameter"

        params = params or {}
        run_name = run_name or "pyspark-me-" + str(int(datetime.datetime.now().timestamp()))

        r = dict(
            run_name=run_name,
            existing_cluster_id=cluster_id,
            libraries=[],
            notebook_task=dict(
                notebook_path=path,
                base_parameters=params
            ),
        )

        response = self.link.post(
            self.path('submit'),
            json=r
        )
        return response['run_id']

    def ls(self, job_id=None, offset=None, limit=None,
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

        response = self.link.get(
            self.path('list'),
            params=params
        )
        return RunList(
                runs=[DatabricksRun(**run) for run in response['runs']],
                has_more=response['has_more'],)
