from dataclasses import dataclass, field, asdict
from typing import List
from .common import bite_size_str

class DataClass:
    def asdict(self) -> dict:
        return asdict(self)

    def __iter__(self):
        return iter(self.asdict().items())

    @classmethod
    def from_dict(cls, d:dict):
        return cls(**d)

@dataclass
class DatabricksObjectInfo(DataClass):
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


@dataclass
class DatabricksFileInfo(DataClass):
    path: str
    is_dir: bool
    file_size: int
    is_file: bool = None
    human_size: str = field(init=False, compare=False)

    def __post_init__(self):
        self.human_size = bite_size_str(self.file_size)
        self.is_file = not self.is_dir



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
class DatabricksRunList(DataClass):
    runs: List[DatabricksRun]
    has_more: bool

@dataclass
class DatabircksNotebookOutput(DataClass):
    result: str = None
    truncated: bool = False


@dataclass
class DatabricksRunOutput(DataClass):
    notebook_output: DatabircksNotebookOutput = None
    error: str = None
    metadata: DatabricksRun = None

    @classmethod
    def from_dict(cls, d: dict) -> 'DatabricksRunOutput':
        notebook_output = d.get('notebook_output', None)
        if notebook_output != None:
            d['notebook_output'] = DatabircksNotebookOutput.from_dict(notebook_output)
        metadata = d.get('metadata', None)
        if metadata != None:
            d['metadata'] = DatabricksRun.from_dict(metadata)
        return DatabricksRunOutput(**d)

@dataclass
class DatabricksViewItem(DataClass):
    content: str = None
    name: str = None
    type: str = None

@dataclass
class DatabricksViewList(DataClass):
    views: List[DatabricksViewItem]

    @classmethod
    def from_dict(cls, d:dict) -> 'DatabricksViewList':
        views = d.get('views', [])
        d['views'] = [DatabricksViewItem.from_dict(v) for v in views]
        return cls(**d)

    @property
    def notebooks(self) -> List[DatabricksViewItem]:
        return [v for v in self.views if v.type == 'NOTEBOOK']
