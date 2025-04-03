import base64
from openfl.protocols import aggregator_models as aggregator_pb2


def named_tensor_pbuf_to_pydantic(tensor):
    pydantic_transformer_metadata_list = []
    for transformer_metadata in tensor.transformer_metadata:
        pydantic_transformer_metadata = aggregator_pb2.MetadataProto(
            int_to_float=transformer_metadata.int_to_float,
            int_list=transformer_metadata.int_list,
            bool_list=transformer_metadata.bool_list,
        )
        pydantic_transformer_metadata_list.append(pydantic_transformer_metadata)

    pydantic_tensor = aggregator_pb2.NamedTensor(
        name=tensor.name,
        round_number=tensor.round_number,
        lossless=tensor.lossless,
        report=tensor.report,
        tags=tensor.tags,
        transformer_metadata=pydantic_transformer_metadata_list,
        data_bytes=base64.b64encode(tensor.data_bytes),
    )
    return pydantic_tensor
