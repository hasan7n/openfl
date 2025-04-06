# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""AggregatorGRPCClient module."""

import ssl
from logging import getLogger
import time

import httpx
import base64

from openfl.protocols import aggregator_models as aggregator_pb2
from .utils import named_tensor_pbuf_to_pydantic
from openfl.utilities import (
    check_equal,
    convert_experiment_status_proto_to_dict,
)


def _atomic_connection(func):
    def wrapper(self, *args, **kwargs):
        # self.reconnect()
        response = func(self, *args, **kwargs)
        # self.disconnect()
        return response

    return wrapper


def _handle_grpc_error(func):
    def wrapper(self, *args, **kwargs):
        response = func(self, *args, **kwargs)
        return response

    return wrapper


def _resend_data_on_reconnection(func):
    def wrapper(self, *args, **kwargs):
        # TODO: should handle status code in another way!
        while True:
            try:
                response = func(self, *args, **kwargs)
                break
            except httpx.HTTPError as e:
                self.logger.info(f"HTTP Error: {str(e)}. Retrying...")
                time.sleep(5)
                self.reconnect()
            except RuntimeError as e:
                self.logger.info(f"Runtime Error: {str(e)}. Retrying...")
                time.sleep(5)
                self.reconnect()
        return response

    return wrapper


class AggregatorRESTClient:
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
        self.tls = tls
        if tls:
            schema = "https"
        else:
            schema = "http"
        self.uri = f"{schema}://{agg_addr}:{agg_port}"
        self.disable_client_auth = disable_client_auth
        self.root_certificate = root_certificate
        self.certificate = certificate
        self.private_key = private_key

        self.logger = getLogger(__name__)

        if not self.tls:
            self.logger.warn(
                "fastapi is running on insecure channel with TLS disabled."
            )
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

        self.stub = None
        self.kwargs = kwargs

    def create_insecure_channel(self, uri):
        """
        Set an insecure gRPC channel (i.e. no TLS) if desired.

        Warns user that this is not recommended.

        Args:
            uri: The uniform resource identifier fo the insecure channel

        Returns:
            An insecure gRPC channel object

        """
        return httpx.Client()

    def create_tls_channel(
        self,
        uri,
        root_certificate,
        disable_client_auth,
        certificate,
        private_key,
    ):
        """
        Set an secure gRPC channel (i.e. TLS).

        Args:
            uri: The uniform resource identifier fo the insecure channel
            root_certificate: The Certificate Authority filename
            disable_client_auth (boolean): True disabled client-side
             authentication (not recommended, throws warning to user)
            certificate: The client certficate filename from the collaborator
             (signed by the certificate authority)

        Returns:
            An insecure gRPC channel object
        """
        limits = httpx.Limits(
            max_connections=1, max_keepalive_connections=1, keepalive_expiry=60
        )

        ctx = ssl.create_default_context(cafile=root_certificate)
        if disable_client_auth:
            self.logger.warn("Client-side authentication is disabled.")
        else:
            ctx.load_cert_chain(certfile=certificate, keyfile=private_key)

        return httpx.Client(verify=ctx, limits=limits)

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
        check_equal(
            reply.header.federation_uuid, self.federation_uuid, self.logger
        )

        # check that there is aggrement on the single_col_cert_common_name
        check_equal(
            reply.header.single_col_cert_common_name,
            self.single_col_cert_common_name or "",
            self.logger,
        )

    def disconnect(self):
        """Close the gRPC channel."""
        self.logger.debug(f"Disconnecting from gRPC server at {self.uri}")
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

        self.logger.debug(f"Connecting to gRPC at {self.uri}")

    @_atomic_connection
    @_resend_data_on_reconnection
    def get_tasks(self, collaborator_name):
        """Get tasks from the aggregator."""
        self._set_header(collaborator_name)
        request = aggregator_pb2.GetTasksRequest(header=self.header)
        timeout = self.kwargs.get("GetTasksTimeout", None)
        response = self.channel.post(
            url=f"{self.uri}/GetTasks",
            json=request.model_dump(),
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(f"{response.status_code} received")
        response = aggregator_pb2.GetTasksResponse(**response.json())
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
        self._set_header(collaborator_name)

        request = aggregator_pb2.GetAggregatedTensorRequest(
            header=self.header,
            tensor_name=tensor_name,
            round_number=round_number,
            report=report,
            tags=tags,
            require_lossless=require_lossless,
        )
        timeout = self.kwargs.get("GetAggregatedTensorTimeout", None)
        response = self.channel.post(
            url=f"{self.uri}/GetAggregatedTensor",
            json=request.model_dump(),
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(f"{response.status_code} received")
        response = aggregator_pb2.GetAggregatedTensorResponse(**response.json())

        # also do other validation, like on the round_number
        self.validate_response(response, collaborator_name)

        response.tensor.data_bytes = base64.b64decode(
            response.tensor.data_bytes.encode()
        )

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
        self._set_header(collaborator_name)
        request = aggregator_pb2.TaskResults(
            header=self.header,
            round_number=round_number,
            task_name=task_name,
            data_size=data_size,
            tensors=[
                named_tensor_pbuf_to_pydantic(tensor)
                for tensor in named_tensors
            ],
        )

        # convert (potentially) long list of tensors into stream
        timeout = self.kwargs.get("SendLocalTaskResultsTimeout", None)
        response = self.channel.post(
            url=f"{self.uri}/SendLocalTaskResults",
            json=request.model_dump(),
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(f"{response.status_code} received")
        response = aggregator_pb2.SendLocalTaskResultsResponse(
            **response.json()
        )
        # also do other validation, like on the round_number
        self.validate_response(response, collaborator_name)

    @_atomic_connection
    @_resend_data_on_reconnection
    def connectivity_check(self, collaborator_name):
        """Check if collaborator can connect to the aggregator."""
        self._set_header(collaborator_name)

        request = aggregator_pb2.ConnectivityCheckRequest(header=self.header)
        response = self.channel.post(
            url=f"{self.uri}/ConnectivityCheck", json=request.model_dump()
        )
        if response.status_code != 200:
            raise RuntimeError(f"{response.status_code} received")
        response = aggregator_pb2.ConnectivityCheckResponse(**response.json())
        # also do other validation, like on the round_number
        self.validate_response(response, collaborator_name)

    @_handle_grpc_error
    @_atomic_connection  # HK-TODO: remove this wrapper?
    def admin_add_collaborator(self, admin_name, col_label, col_cn):
        """Add collaborator RPC."""
        self._set_header(admin_name)
        request = aggregator_pb2.AddCollaboratorRequest(
            header=self.header,
            collaborator_label=col_label,
            collaborator_cn=col_cn,
        )
        response = self.channel.post(
            url=f"{self.uri}/AddCollaborator", json=request.model_dump()
        )
        response = aggregator_pb2.AddCollaboratorResponse(**response.json())
        self.validate_response(response, admin_name)

    @_handle_grpc_error
    @_atomic_connection  # HK-TODO: remove this wrapper?
    def admin_remove_collaborator(self, admin_name, col_label, col_cn):
        """Remove collaborator RPC."""
        self._set_header(admin_name)
        request = aggregator_pb2.RemoveCollaboratorRequest(
            header=self.header,
            collaborator_label=col_label,
            collaborator_cn=col_cn,
        )
        response = self.channel.post(
            url=f"{self.uri}/RemoveCollaborator", json=request.model_dump()
        )
        response = aggregator_pb2.RemoveCollaboratorResponse(**response.json())
        self.validate_response(response, admin_name)

    @_handle_grpc_error
    @_atomic_connection  # HK-TODO: remove this wrapper?
    def admin_get_experiment_status(self, admin_name):
        """Get experiment status RPC."""
        self._set_header(admin_name)
        request = aggregator_pb2.GetExperimentStatusRequest(header=self.header)

        response = self.channel.post(
            url=f"{self.uri}/GetExperimentStatus", json=request.model_dump()
        )
        response = aggregator_pb2.GetExperimentStatusResponse(**response.json())
        self.validate_response(response, admin_name)
        status_dict = convert_experiment_status_proto_to_dict(response)
        return status_dict

    @_handle_grpc_error
    @_atomic_connection  # MS-TODO: remove this wrapper?
    def admin_set_straggler_cutoff_time(self, admin_name, timeout_in_seconds):
        """SetStragglerCuttoffTime RPC."""
        self._set_header(admin_name)
        request = aggregator_pb2.SetStragglerCuttoffTimeRequest(
            header=self.header, timeout_in_seconds=timeout_in_seconds
        )
        response = self.channel.post(
            url=f"{self.uri}/SetStragglerCuttoffTime", json=request.model_dump()
        )
        response = aggregator_pb2.SetStragglerCuttoffTimeResponse(
            **response.json()
        )
        self.validate_response(response, admin_name)

    @_handle_grpc_error
    @_atomic_connection  # MS-TODO: remove this wrapper?
    def admin_get_dynamic_task_arg(self, admin_name, task_name, arg_name):
        """GetDynamicTaskArg RPC."""
        self._set_header(admin_name)
        request = aggregator_pb2.GetDynamicTaskArgRequest(
            header=self.header, task_name=task_name, arg_name=arg_name
        )
        response = self.channel.post(
            url=f"{self.uri}/GetDynamicTaskArg", json=request.model_dump()
        )
        response = aggregator_pb2.GetDynamicTaskArgResponse(**response.json())
        self.validate_response(response, admin_name)
        return response.current_value, response.next_value

    @_handle_grpc_error
    @_atomic_connection  # MS-TODO: remove this wrapper?
    def admin_set_dynamic_task_arg(
        self, admin_name, task_name, arg_name, value
    ):
        """SetDynamicTaskArg RPC."""
        self._set_header(admin_name)
        request = aggregator_pb2.SetDynamicTaskArgRequest(
            header=self.header,
            task_name=task_name,
            arg_name=arg_name,
            value=value,
        )
        response = self.channel.post(
            url=f"{self.uri}/SetDynamicTaskArg", json=request.model_dump()
        )
        response = aggregator_pb2.SetDynamicTaskArgResponse(**response.json())
        self.validate_response(response, admin_name)
