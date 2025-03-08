# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""AggregatorGRPCClient module."""

import time
from typing import Optional
from typing import Tuple

import grpc

import aggregator_pb2
import aggregator_pb2_grpc

import secrets

channel_options = [
    ("grpc.max_metadata_size", 2**25),
    ("grpc.max_send_message_length", 2**30),
    ("grpc.max_receive_message_length", 2**30),
]


def check_equal(x, y, logger):
    """Assert `x` and `y` are equal."""
    if x != y:
        exception = ValueError(f"{x} != {y}")
        raise exception


def proto_to_datastream(proto, logger, max_buffer_size=(2 * 1024 * 1024)):
    """Convert the protobuf to the datastream for the remote connection.

    Args:
        model_proto: The protobuf of the model
        logger: The log object
        max_buffer_size: The buffer size (Default= 2*1024*1024)
    Returns:
        reply: The message for the remote connection.
    """
    npbytes = proto.SerializeToString()
    data_size = len(npbytes)
    buffer_size = data_size if max_buffer_size > data_size else max_buffer_size
    print(f"Setting stream chunks with size {buffer_size} for proto of type {type(proto)}")

    for i in range(0, data_size, buffer_size):
        chunk = npbytes[i : i + buffer_size]  # noqa
        reply = aggregator_pb2.DataStream(npbytes=chunk, size=len(chunk))
        yield reply


class ConstantBackoff:
    """Constant Backoff policy."""

    def __init__(self, reconnect_interval, logger, uri):
        """Initialize Constant Backoff."""
        self.reconnect_interval = reconnect_interval
        self.logger = logger
        self.uri = uri

    def sleep(self):
        """Sleep for specified interval."""
        print(f"Attempting to connect to aggregator at {self.uri}")
        time.sleep(self.reconnect_interval)


class RetryOnRpcErrorClientInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.StreamUnaryClientInterceptor):
    """Retry gRPC connection on failure."""

    def __init__(
        self,
        sleeping_policy,
        status_for_retry: Optional[Tuple[grpc.StatusCode]] = None,
    ):
        """Initialize function for gRPC retry."""
        self.sleeping_policy = sleeping_policy
        self.status_for_retry = status_for_retry

    def _intercept_call(self, continuation, client_call_details, request_or_iterator):  # HK-TODO: have here a list of service names that we don't want to retry on failure.
        #          This will clean the "dirty" way I introduced earlier that calls the client object
        #          with the `for_admin` flag.
        #          hint for this todo: `client_call_details.method` for example will give
        #          "/openfl.aggregator.Aggregator/ConnectivityCheck"
        """Intercept the call to the gRPC server."""
        while True:
            response = continuation(client_call_details, request_or_iterator)

            if isinstance(response, grpc.RpcError):

                # If status code is not in retryable status codes
                print(f"Response code: {response.code()}\nResponse debug error string: {response.debug_error_string()}\nResponse details: {response.details()}")
                if self.status_for_retry and response.code() not in self.status_for_retry:
                    return response

                self.sleeping_policy.sleep()
            else:
                return response

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """Wrap intercept call for unary->unary RPC."""
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        """Wrap intercept call for stream->unary RPC."""
        return self._intercept_call(continuation, client_call_details, request_iterator)


def _atomic_connection(func):
    def wrapper(self, *args, **kwargs):
        self.reconnect()
        response = func(self, *args, **kwargs)
        self.disconnect()
        return response

    return wrapper


def _resend_data_on_reconnection(func):
    def wrapper(self, *args, **kwargs):
        while True:
            try:
                response = func(self, *args, **kwargs)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNKNOWN:
                    print(f"Attempting to resend data request to aggregator at {self.uri}")
                elif e.code() in [grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.NOT_FOUND, grpc.StatusCode.DEADLINE_EXCEEDED]:
                    raise
                print(f"Sent request, got {e.code()}")
                continue
            break
        return response

    return wrapper


class AggregatorGRPCClient:
    """Client to the aggregator over gRPC-TLS."""

    def __init__(
        self,
        agg_addr,
        agg_port,
        tls,
        disable_client_auth,
        root_certificate,
        certificate,
        private_key,
        aggregator_uuid=None,
        federation_uuid=None,
        single_col_cert_common_name=None,
        for_admin=False,
        **kwargs,
    ):
        """Initialize."""
        self.uri = f"{agg_addr}:{agg_port}"
        self.tls = tls
        self.disable_client_auth = disable_client_auth
        self.root_certificate = root_certificate
        self.certificate = certificate
        self.private_key = private_key

        self.logger = None

        if not self.tls:
            self.channel = self.create_insecure_channel(self.uri)
        else:
            self.channel = self.create_tls_channel(
                self.uri,
                self.root_certificate,
                self.disable_client_auth,
                self.certificate,
                self.private_key,
            )

        self.header = None
        self.aggregator_uuid = aggregator_uuid
        self.federation_uuid = federation_uuid
        self.single_col_cert_common_name = single_col_cert_common_name

        # Adding an interceptor for RPC Errors
        self.interceptors = (
            RetryOnRpcErrorClientInterceptor(
                sleeping_policy=ConstantBackoff(
                    logger=self.logger,
                    reconnect_interval=int(kwargs.get("client_reconnect_interval", 1)),
                    uri=self.uri,
                ),
                status_for_retry=(grpc.StatusCode.UNAVAILABLE,),
            ),
        )
        self.stub = aggregator_pb2_grpc.AggregatorStub(grpc.intercept_channel(self.channel, *self.interceptors))
        self.kwargs = kwargs

    def create_insecure_channel(self, uri):
        return grpc.insecure_channel(uri, options=channel_options)

    def create_tls_channel(
        self,
        uri,
        root_certificate,
        disable_client_auth,
        certificate,
        private_key,
    ):
        with open(root_certificate, "rb") as f:
            root_certificate_b = f.read()

        if disable_client_auth:
            private_key_b = None
            certificate_b = None
        else:
            with open(private_key, "rb") as f:
                private_key_b = f.read()
            with open(certificate, "rb") as f:
                certificate_b = f.read()

        credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificate_b,
            private_key=private_key_b,
            certificate_chain=certificate_b,
        )

        return grpc.secure_channel(uri, credentials, options=channel_options)

    def _set_header(self, collaborator_name):
        self.header = aggregator_pb2.MessageHeader(
            sender=collaborator_name,
            receiver=self.aggregator_uuid,
            federation_uuid=self.federation_uuid,
            single_col_cert_common_name=self.single_col_cert_common_name or "",
        )

    def validate_response(self, reply, collaborator_name):
        """Validate the aggregator response."""
        # check that the message was intended to go to this collaborator
        check_equal(reply.header.receiver, collaborator_name, self.logger)
        check_equal(reply.header.sender, self.aggregator_uuid, self.logger)

        # check that federation id matches
        check_equal(reply.header.federation_uuid, self.federation_uuid, self.logger)

        # check that there is aggrement on the single_col_cert_common_name
        check_equal(
            reply.header.single_col_cert_common_name,
            self.single_col_cert_common_name or "",
            self.logger,
        )

    def disconnect(self):
        """Close the gRPC channel."""
        self.channel.close()

    def reconnect(self):
        """Create a new channel with the gRPC server."""
        # channel.close() is idempotent. Call again here in case it wasn't issued previously
        self.disconnect()

        if not self.tls:
            self.channel = self.create_insecure_channel(self.uri)
        else:
            self.channel = self.create_tls_channel(
                self.uri,
                self.root_certificate,
                self.disable_client_auth,
                self.certificate,
                self.private_key,
            )

        self.stub = aggregator_pb2_grpc.AggregatorStub(grpc.intercept_channel(self.channel, *self.interceptors))

    @_atomic_connection
    @_resend_data_on_reconnection
    def get_tasks(self, collaborator_name):
        """Get tasks from the aggregator."""
        self._set_header(collaborator_name)
        request = aggregator_pb2.GetTasksRequest(header=self.header)
        timeout = self.kwargs.get("GetTasksTimeout", None)
        response = self.stub.GetTasks(request, timeout=timeout)
        self.validate_response(response, collaborator_name)

        return (
            response.tasks,
            response.round_number,
            response.sleep_time,
            response.quit,
        )

    @_atomic_connection
    @_resend_data_on_reconnection
    def get_aggregated_tensor(
        self,
        collaborator_name,
        tensor_name,
        round_number,
        report,
        tags,
        require_lossless,
    ):
        """Get aggregated tensor from the aggregator."""
        print(f"Getting aggregated tensor {tensor_name}, round {round_number}")
        self._set_header(collaborator_name)

        print(
            self.header,
            tensor_name,
            round_number,
            report,
            tags,
            require_lossless,
        )
        request = aggregator_pb2.GetAggregatedTensorRequest(
            header=self.header,
            tensor_name=tensor_name,
            round_number=round_number,
            report=report,
            tags=tags,
            require_lossless=require_lossless,
        )
        timeout = self.kwargs.get("GetAggregatedTensorTimeout", None)
        ffff = f"./analysis2/{collaborator_name} GetAggregatedTensor {tensor_name.replace('/', '_')} {round_number} {time.time()} START {secrets.token_hex(20)}"
        with open(ffff, "w"):
            pass
        response = self.stub.GetAggregatedTensor(request, timeout=timeout)
        ffff = f"./analysis2/{collaborator_name} GetAggregatedTensor {tensor_name.replace('/', '_')} {round_number} {time.time()} END {secrets.token_hex(20)}"
        with open(ffff, "w"):
            pass
        # also do other validation, like on the round_number
        self.validate_response(response, collaborator_name)

        return response.tensor

    @_atomic_connection
    @_resend_data_on_reconnection
    def send_local_task_results(
        self,
        collaborator_name,
        round_number,
        task_name,
        data_size,
        named_tensors,
    ):
        """Send task results to the aggregator."""
        print(f"Sending task results {task_name}, round {round_number}")

        self._set_header(collaborator_name)
        request = aggregator_pb2.TaskResults(
            header=self.header,
            round_number=round_number,
            task_name=task_name,
            data_size=data_size,
            tensors=named_tensors,
        )

        # convert (potentially) long list of tensors into stream
        stream = []
        stream += proto_to_datastream(request, self.logger)
        timeout = self.kwargs.get("SendLocalTaskResultsTimeout", None)
        ffff = f"./analysis2/{collaborator_name} SendLocalTaskResults {round_number} {time.time()} START {secrets.token_hex(20)}"
        with open(ffff, "w"):
            pass
        response = self.stub.SendLocalTaskResults(iter(stream), timeout=timeout)
        ffff = f"./analysis2/{collaborator_name} SendLocalTaskResults {round_number} {time.time()} END {secrets.token_hex(20)}"
        with open(ffff, "w"):
            pass

        # also do other validation, like on the round_number
        self.validate_response(response, collaborator_name)
