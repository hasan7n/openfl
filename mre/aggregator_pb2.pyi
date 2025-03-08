from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MessageHeader(_message.Message):
    __slots__ = ("sender", "receiver", "federation_uuid", "single_col_cert_common_name")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    RECEIVER_FIELD_NUMBER: _ClassVar[int]
    FEDERATION_UUID_FIELD_NUMBER: _ClassVar[int]
    SINGLE_COL_CERT_COMMON_NAME_FIELD_NUMBER: _ClassVar[int]
    sender: str
    receiver: str
    federation_uuid: str
    single_col_cert_common_name: str
    def __init__(self, sender: _Optional[str] = ..., receiver: _Optional[str] = ..., federation_uuid: _Optional[str] = ..., single_col_cert_common_name: _Optional[str] = ...) -> None: ...

class Task(_message.Message):
    __slots__ = ("name", "function_name", "task_type", "apply_local")
    NAME_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    APPLY_LOCAL_FIELD_NUMBER: _ClassVar[int]
    name: str
    function_name: str
    task_type: str
    apply_local: bool
    def __init__(self, name: _Optional[str] = ..., function_name: _Optional[str] = ..., task_type: _Optional[str] = ..., apply_local: bool = ...) -> None: ...

class MetadataProto(_message.Message):
    __slots__ = ("int_to_float", "int_list", "bool_list")
    class IntToFloatEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: float
        def __init__(self, key: _Optional[int] = ..., value: _Optional[float] = ...) -> None: ...
    INT_TO_FLOAT_FIELD_NUMBER: _ClassVar[int]
    INT_LIST_FIELD_NUMBER: _ClassVar[int]
    BOOL_LIST_FIELD_NUMBER: _ClassVar[int]
    int_to_float: _containers.ScalarMap[int, float]
    int_list: _containers.RepeatedScalarFieldContainer[int]
    bool_list: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, int_to_float: _Optional[_Mapping[int, float]] = ..., int_list: _Optional[_Iterable[int]] = ..., bool_list: _Optional[_Iterable[bool]] = ...) -> None: ...

class NamedTensor(_message.Message):
    __slots__ = ("name", "round_number", "lossless", "report", "tags", "transformer_metadata", "data_bytes")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ROUND_NUMBER_FIELD_NUMBER: _ClassVar[int]
    LOSSLESS_FIELD_NUMBER: _ClassVar[int]
    REPORT_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TRANSFORMER_METADATA_FIELD_NUMBER: _ClassVar[int]
    DATA_BYTES_FIELD_NUMBER: _ClassVar[int]
    name: str
    round_number: int
    lossless: bool
    report: bool
    tags: _containers.RepeatedScalarFieldContainer[str]
    transformer_metadata: _containers.RepeatedCompositeFieldContainer[MetadataProto]
    data_bytes: bytes
    def __init__(self, name: _Optional[str] = ..., round_number: _Optional[int] = ..., lossless: bool = ..., report: bool = ..., tags: _Optional[_Iterable[str]] = ..., transformer_metadata: _Optional[_Iterable[_Union[MetadataProto, _Mapping]]] = ..., data_bytes: _Optional[bytes] = ...) -> None: ...

class DataStream(_message.Message):
    __slots__ = ("size", "npbytes")
    SIZE_FIELD_NUMBER: _ClassVar[int]
    NPBYTES_FIELD_NUMBER: _ClassVar[int]
    size: int
    npbytes: bytes
    def __init__(self, size: _Optional[int] = ..., npbytes: _Optional[bytes] = ...) -> None: ...

class GetTasksRequest(_message.Message):
    __slots__ = ("header",)
    HEADER_FIELD_NUMBER: _ClassVar[int]
    header: MessageHeader
    def __init__(self, header: _Optional[_Union[MessageHeader, _Mapping]] = ...) -> None: ...

class GetTasksResponse(_message.Message):
    __slots__ = ("header", "round_number", "tasks", "sleep_time", "quit")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ROUND_NUMBER_FIELD_NUMBER: _ClassVar[int]
    TASKS_FIELD_NUMBER: _ClassVar[int]
    SLEEP_TIME_FIELD_NUMBER: _ClassVar[int]
    QUIT_FIELD_NUMBER: _ClassVar[int]
    header: MessageHeader
    round_number: int
    tasks: _containers.RepeatedCompositeFieldContainer[Task]
    sleep_time: int
    quit: bool
    def __init__(self, header: _Optional[_Union[MessageHeader, _Mapping]] = ..., round_number: _Optional[int] = ..., tasks: _Optional[_Iterable[_Union[Task, _Mapping]]] = ..., sleep_time: _Optional[int] = ..., quit: bool = ...) -> None: ...

class GetAggregatedTensorRequest(_message.Message):
    __slots__ = ("header", "tensor_name", "round_number", "report", "tags", "require_lossless")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    TENSOR_NAME_FIELD_NUMBER: _ClassVar[int]
    ROUND_NUMBER_FIELD_NUMBER: _ClassVar[int]
    REPORT_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_LOSSLESS_FIELD_NUMBER: _ClassVar[int]
    header: MessageHeader
    tensor_name: str
    round_number: int
    report: bool
    tags: _containers.RepeatedScalarFieldContainer[str]
    require_lossless: bool
    def __init__(self, header: _Optional[_Union[MessageHeader, _Mapping]] = ..., tensor_name: _Optional[str] = ..., round_number: _Optional[int] = ..., report: bool = ..., tags: _Optional[_Iterable[str]] = ..., require_lossless: bool = ...) -> None: ...

class GetAggregatedTensorResponse(_message.Message):
    __slots__ = ("header", "round_number", "tensor")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ROUND_NUMBER_FIELD_NUMBER: _ClassVar[int]
    TENSOR_FIELD_NUMBER: _ClassVar[int]
    header: MessageHeader
    round_number: int
    tensor: NamedTensor
    def __init__(self, header: _Optional[_Union[MessageHeader, _Mapping]] = ..., round_number: _Optional[int] = ..., tensor: _Optional[_Union[NamedTensor, _Mapping]] = ...) -> None: ...

class TaskResults(_message.Message):
    __slots__ = ("header", "round_number", "task_name", "data_size", "tensors")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ROUND_NUMBER_FIELD_NUMBER: _ClassVar[int]
    TASK_NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_SIZE_FIELD_NUMBER: _ClassVar[int]
    TENSORS_FIELD_NUMBER: _ClassVar[int]
    header: MessageHeader
    round_number: int
    task_name: str
    data_size: int
    tensors: _containers.RepeatedCompositeFieldContainer[NamedTensor]
    def __init__(self, header: _Optional[_Union[MessageHeader, _Mapping]] = ..., round_number: _Optional[int] = ..., task_name: _Optional[str] = ..., data_size: _Optional[int] = ..., tensors: _Optional[_Iterable[_Union[NamedTensor, _Mapping]]] = ...) -> None: ...

class SendLocalTaskResultsResponse(_message.Message):
    __slots__ = ("header",)
    HEADER_FIELD_NUMBER: _ClassVar[int]
    header: MessageHeader
    def __init__(self, header: _Optional[_Union[MessageHeader, _Mapping]] = ...) -> None: ...
