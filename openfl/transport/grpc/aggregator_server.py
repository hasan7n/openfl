# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""AggregatorGRPCServer module."""

import logging
from concurrent.futures import ThreadPoolExecutor
from random import random
from multiprocessing import cpu_count
from time import sleep

from grpc import server
from grpc import ssl_server_credentials
from grpc import StatusCode

from openfl.protocols import aggregator_pb2
from openfl.protocols import aggregator_pb2_grpc
from openfl.protocols import utils
from openfl.utilities import check_equal
from openfl.utilities import check_is_in

from .grpc_channel_options import channel_options

logger = logging.getLogger(__name__)


class AggregatorGRPCServer(aggregator_pb2_grpc.AggregatorServicer):
    """gRPC server class for the Aggregator."""

    def __init__(self,
                 aggregator,
                 agg_port,
                 tls=True,
                 disable_client_auth=False,
                 root_certificate=None,
                 certificate=None,
                 private_key=None,
                 **kwargs):
        """
        Class initializer.

        Args:
            aggregator: The aggregator
        Args:
            fltask (FLtask): The gRPC service task.
            tls (bool): To disable the TLS. (Default: True)
            disable_client_auth (bool): To disable the client side
            authentication. (Default: False)
            root_certificate (str): File path to the CA certificate.
            certificate (str): File path to the server certificate.
            private_key (str): File path to the private key.
            kwargs (dict): Additional arguments to pass into function
        """
        self.aggregator = aggregator
        self.uri = f'[::]:{agg_port}'
        self.tls = tls
        self.disable_client_auth = disable_client_auth
        self.root_certificate = root_certificate
        self.certificate = certificate
        self.private_key = private_key
        self.server = None
        self.server_credentials = None

        self.logger = logging.getLogger(__name__)

    def validate_collaborator(self, request, context):
        """
        Validate the collaborator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        Raises:
            ValueError: If the collaborator or collaborator certificate is not
             valid then raises error.

        """
        if self.tls:
            common_name = context.auth_context()[
                'x509_common_name'][0].decode('utf-8')
            collaborator_common_name = request.header.sender
            if not self.aggregator.valid_collaborator_cn_and_id(
                    common_name, collaborator_common_name):
                # Random delay in authentication failures
                sleep(5 * random())  # nosec
                context.abort(
                    StatusCode.UNAUTHENTICATED,
                    f'Invalid collaborator. CN: |{common_name}| '
                    f'collaborator_common_name: |{collaborator_common_name}|')

    def validate_admin(self, request, context, endpoint_name):
        """
        Validate the admin request.

        Args:
            request: The gRPC message request
            context: The gRPC context
            endpoint_name: The endpoint name the admin is calling

        Raises:
            ValueError: If the admin certificate is not valid
                or the endpoint is not allowed then raises error.

        """
        if self.tls:
            common_name = context.auth_context()[
                'x509_common_name'][0].decode('utf-8')
            admin_common_name = request.header.sender
            if not self.aggregator.valid_admin_cn_and_id(
                    common_name, admin_common_name):
                # Random delay in authentication failures
                sleep(5 * random())  # nosec
                context.abort(
                    StatusCode.UNAUTHENTICATED,
                    f'Invalid admin. CN: |{common_name}| '
                    f'admin_common_name: |{admin_common_name}|')

        if not self.aggregator.valid_admin_endpoint(endpoint_name):
            context.abort(
                    StatusCode.UNAVAILABLE,
                    f'This endpoint is not permitted for this federation. Endpoint: |{endpoint_name}| ')

    def get_header(self, collaborator_name):
        """
        Compose and return MessageHeader.

        Args:
            collaborator_name : str
                The collaborator the message is intended for
        """
        return aggregator_pb2.MessageHeader(
            sender=self.aggregator.uuid,
            receiver=collaborator_name,
            federation_uuid=self.aggregator.federation_uuid,
            single_col_cert_common_name=self.aggregator.single_col_cert_common_name
        )

    def check_request(self, request):
        """
        Validate request header matches expected values.

        Args:
            request : protobuf
                Request sent from a collaborator that requires validation
        """
        # TODO improve this check. the sender name could be spoofed
        check_is_in(request.header.sender, self.aggregator.authorized_cols, self.logger)

        # check that the message is for me
        check_equal(request.header.receiver, self.aggregator.uuid, self.logger)

        # check that the message is for my federation
        check_equal(
            request.header.federation_uuid, self.aggregator.federation_uuid, self.logger)

        # check that we agree on the single cert common name
        check_equal(
            request.header.single_col_cert_common_name,
            self.aggregator.single_col_cert_common_name,
            self.logger
        )

    def check_admin_request(self, request):
        """
        Validate request header matches expected values.

        Args:
            request : protobuf
                Request sent from an admin that requires validation
        """
        # TODO improve this check. the sender name could be spoofed
        check_is_in(request.header.sender, self.aggregator.admins, self.logger)

        # check that the message is for me
        check_equal(request.header.receiver, self.aggregator.uuid, self.logger)

        # check that the message is for my federation
        check_equal(
            request.header.federation_uuid, self.aggregator.federation_uuid, self.logger)

    def GetTasks(self, request, context):  # NOQA:N802
        """
        Request a job from aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_collaborator(request, context)
        self.check_request(request)
        collaborator_name = request.header.sender
        tasks, round_number, sleep_time, time_to_quit = self.aggregator.get_tasks(
            request.header.sender)
        if tasks:
            if isinstance(tasks[0], str):
                # backward compatibility
                tasks_proto = [
                    aggregator_pb2.Task(
                        name=task,
                    ) for task in tasks
                ]
            else:
                tasks_proto = [
                    aggregator_pb2.Task(
                        name=task.name,
                        function_name=task.function_name,
                        task_type=task.task_type,
                        apply_local=task.apply_local
                    ) for task in tasks
                ]
        else:
            tasks_proto = []

        return aggregator_pb2.GetTasksResponse(
            header=self.get_header(collaborator_name),
            round_number=round_number,
            tasks=tasks_proto,
            sleep_time=sleep_time,
            quit=time_to_quit
        )

    def GetAggregatedTensor(self, request, context):  # NOQA:N802
        """
        Request a job from aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_collaborator(request, context)
        self.check_request(request)
        collaborator_name = request.header.sender
        tensor_name = request.tensor_name
        require_lossless = request.require_lossless
        round_number = request.round_number
        report = request.report
        tags = tuple(request.tags)

        named_tensor = self.aggregator.get_aggregated_tensor(
            collaborator_name, tensor_name, round_number, report, tags, require_lossless)

        return aggregator_pb2.GetAggregatedTensorResponse(
            header=self.get_header(collaborator_name),
            round_number=round_number,
            tensor=named_tensor
        )

    def SendLocalTaskResults(self, request, context):  # NOQA:N802
        """
        Request a model download from aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        try:
            proto = aggregator_pb2.TaskResults()
            proto = utils.datastream_to_proto(proto, request)
        except RuntimeError:
            raise RuntimeError(
                'Empty stream message, reestablishing connection from client to resume training...'
            )

        self.validate_collaborator(proto, context)
        # all messages get sanity checked
        self.check_request(proto)

        collaborator_name = proto.header.sender
        task_name = proto.task_name
        round_number = proto.round_number
        data_size = proto.data_size
        named_tensors = proto.tensors
        self.aggregator.send_local_task_results(
            collaborator_name, round_number, task_name, data_size, named_tensors)
        # turn data stream into local model update
        return aggregator_pb2.SendLocalTaskResultsResponse(
            header=self.get_header(collaborator_name)
        )

    def AddCollaborator(self, request, context):  # NOQA:N802
        """
        Request to add a collaborator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_admin(request, context, "AddCollaborator")
        self.check_admin_request(request)
        admin_name = request.header.sender
        col_label = request.collaborator_label
        col_cn = request.collaborator_cn
        self.aggregator.add_collaborator(col_label, col_cn)
        return aggregator_pb2.AddCollaboratorResponse(
            header=self.get_header(admin_name)
        )

    def RemoveCollaborator(self, request, context):  # NOQA:N802
        """
        Request to remove a collaborator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_admin(request, context, "RemoveCollaborator")
        self.check_admin_request(request)
        admin_name = request.header.sender
        col_label = request.collaborator_label
        col_cn = request.collaborator_cn
        self.aggregator.remove_collaborator(col_label, col_cn)
        return aggregator_pb2.RemoveCollaboratorResponse(
            header=self.get_header(admin_name)
        )

    def _prepare_experiment_status_pb(self, status_dict):
        if not status_dict:
            return
        round_num = status_dict["round"]
        collaborators = status_dict["collaborators"]
        start_times = status_dict["start_times"]
        end_times = status_dict["end_times"]
        stragglers = status_dict["stragglers"]
        round_start = status_dict["round_start"]
        to_add_next_round = status_dict["to_add_next_round"]
        to_remove_next_round = status_dict["to_remove_next_round"]
        available_collaborators = status_dict["available_collaborators"]
        assigned_collaborators = status_dict["assigned_collaborators"]

        collaborators_progress = []
        for collaborator_name in collaborators:
            col_start_time = start_times.get(collaborator_name)
            col_end_times = end_times.get(collaborator_name, {})
            col_task_endtimes = []
            for task_name, end_time in col_end_times.items():
                task_endtime_pb = aggregator_pb2.TaskEndTime(task_name=task_name, end_time=end_time)
                col_task_endtimes.append(task_endtime_pb)
            collaborator_progress_pb = aggregator_pb2.CollaboratorProgress(
                col_name=collaborator_name,
                start_time=col_start_time,
                tasks_end_time=col_task_endtimes
            )
            collaborators_progress.append(collaborator_progress_pb)

        return aggregator_pb2.ExperimentStatus(
            round=round_num,
            round_start=round_start,
            collaborators_progress=collaborators_progress,
            stragglers=stragglers,
            to_add_next_round=to_add_next_round,
            to_remove_next_round=to_remove_next_round,
            available_collaborators=available_collaborators,
            assigned_collaborators=assigned_collaborators
        )

    def GetExperimentStatus(self, request, context):  # NOQA:N802
        """
        Get experiment status from the aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_admin(request, context, "GetExperimentStatus")
        self.check_admin_request(request)
        admin_name = request.header.sender
        current_round, previous_round = self.aggregator.get_experiment_status()
        return aggregator_pb2.GetExperimentStatusResponse(
            header=self.get_header(admin_name),
            current_round=self._prepare_experiment_status_pb(current_round),
            previous_round=self._prepare_experiment_status_pb(previous_round)
        )

    def get_server(self):
        """Return gRPC server."""
        self.server = server(ThreadPoolExecutor(max_workers=cpu_count()),
                             options=channel_options)

        aggregator_pb2_grpc.add_AggregatorServicer_to_server(self, self.server)

        if not self.tls:

            self.logger.warn(
                'gRPC is running on insecure channel with TLS disabled.')
            port = self.server.add_insecure_port(self.uri)
            self.logger.info(f'Insecure port: {port}')

        else:

            with open(self.private_key, 'rb') as f:
                private_key_b = f.read()
            with open(self.certificate, 'rb') as f:
                certificate_b = f.read()
            with open(self.root_certificate, 'rb') as f:
                root_certificate_b = f.read()

            if self.disable_client_auth:
                self.logger.warn('Client-side authentication is disabled.')

            self.server_credentials = ssl_server_credentials(
                ((private_key_b, certificate_b),),
                root_certificates=root_certificate_b,
                require_client_auth=not self.disable_client_auth
            )

            self.server.add_secure_port(self.uri, self.server_credentials)

        return self.server

    def serve(self):
        """Start an aggregator gRPC service."""
        self.get_server()

        self.logger.info('Starting Aggregator gRPC Server')
        self.server.start()

        try:
            while not self.aggregator.all_quit_jobs_sent():
                sleep(5)
        except KeyboardInterrupt:
            pass

        self.server.stop(0)
