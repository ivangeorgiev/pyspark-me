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


