import datetime
from .common import Api

class Runs(Api):
    def __init__(self, link):
        super().__init__(link, path='jobs/runs')

    def list(self, job_id=None, offset=None, limit=None,
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
        list_result = self.link.get(
            self.path('list'),
            params=params)
        return list_result

    def export(self, run_id):
        response = self.link.get(
            self.path('export'),
            params=dict(run_id=run_id))
        return response


    def run_submit_notebook(self, path, params=None, run_name=None, cluster_id=None):
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

