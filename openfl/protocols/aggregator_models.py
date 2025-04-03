from pydantic import BaseModel, field_validator, field_serializer
from typing import List, Optional, Dict
import base64


class MessageHeader(BaseModel):
    sender: str
    receiver: str
    federation_uuid: str
    single_col_cert_common_name: str


class MetadataProto(BaseModel):
    int_to_float: Dict[int, float]
    int_list: List[int]
    bool_list: List[bool]


class NamedTensor(BaseModel):
    name: str
    round_number: int
    lossless: bool
    report: bool
    tags: List[str]
    transformer_metadata: List[MetadataProto]
    data_bytes: str


class ModelProto(BaseModel):
    tensors: List[NamedTensor]


class DataStream(BaseModel):
    size: int
    npbytes: bytes


class CollaboratorDescription(BaseModel):
    name: str
    status: str
    progress: float
    round: int
    current_task: str
    next_task: str


class TaskDescription(BaseModel):
    name: str
    description: str


class DownloadStatus(BaseModel):
    name: str
    status: str


class DownloadStatuses(BaseModel):
    models: List[DownloadStatus]
    logs: List[DownloadStatus]


class ExperimentDescription(BaseModel):
    name: str
    status: str
    progress: float
    total_rounds: int
    current_round: int
    download_statuses: DownloadStatuses
    collaborators: List[CollaboratorDescription]
    tasks: List[TaskDescription]


class GetTasksRequest(BaseModel):
    header: MessageHeader


class GetMetricStreamRequest(BaseModel):
    experiment_name: str


class GetMetricStreamResponse(BaseModel):
    metric_origin: str
    task_name: str
    metric_name: str
    metric_value: float
    round: int


class Task(BaseModel):
    name: str
    function_name: Optional[str] = None
    task_type: Optional[str] = None
    apply_local: Optional[bool] = None


class GetTasksResponse(BaseModel):
    header: MessageHeader
    round_number: int
    tasks: Optional[List[Task]] = None
    sleep_time: int
    quit: bool


class GetAggregatedTensorRequest(BaseModel):
    header: MessageHeader
    tensor_name: str
    round_number: int
    report: bool
    tags: List[str]
    require_lossless: bool


class GetAggregatedTensorResponse(BaseModel):
    header: MessageHeader
    round_number: int
    tensor: NamedTensor


class TaskResults(BaseModel):
    header: MessageHeader
    round_number: int
    task_name: str
    data_size: int
    tensors: List[NamedTensor]


class SendLocalTaskResultsResponse(BaseModel):
    header: MessageHeader


class AddCollaboratorRequest(BaseModel):
    header: MessageHeader
    collaborator_label: str
    collaborator_cn: str


class AddCollaboratorResponse(BaseModel):
    header: MessageHeader


class RemoveCollaboratorRequest(BaseModel):
    header: MessageHeader
    collaborator_label: str
    collaborator_cn: str


class RemoveCollaboratorResponse(BaseModel):
    header: MessageHeader


class GetExperimentStatusRequest(BaseModel):
    header: MessageHeader


class SetStragglerCuttoffTimeRequest(BaseModel):
    header: MessageHeader
    timeout_in_seconds: float


class TaskEndTime(BaseModel):
    task_name: str
    end_time: float


class CollaboratorProgress(BaseModel):
    col_name: str
    start_time: Optional[float] = None
    tasks_end_time: List[TaskEndTime]


class ExperimentStatus(BaseModel):
    round: int
    round_start: Optional[float] = None
    collaborators_progress: List[CollaboratorProgress]
    stragglers: List[str]
    to_add_next_round: List[str]
    to_remove_next_round: List[str]
    available_collaborators: List[str]
    assigned_collaborators: List[str]
    metrics: List[GetMetricStreamResponse]


class GetExperimentStatusResponse(BaseModel):
    header: MessageHeader
    rounds: List[ExperimentStatus]


class SetStragglerCuttoffTimeResponse(BaseModel):
    header: MessageHeader


class ConnectivityCheckRequest(BaseModel):
    header: MessageHeader


class ConnectivityCheckResponse(BaseModel):
    header: MessageHeader


class SetDynamicTaskArgRequest(BaseModel):
    header: MessageHeader
    task_name: str
    arg_name: str
    value: float


class SetDynamicTaskArgResponse(BaseModel):
    header: MessageHeader


class GetDynamicTaskArgRequest(BaseModel):
    header: MessageHeader
    task_name: str
    arg_name: str


class GetDynamicTaskArgResponse(BaseModel):
    header: MessageHeader
    current_value: float
    next_value: float
