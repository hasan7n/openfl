"""AggregatorRESTServer module."""

import base64
import os
from . import allow_cert_info  # noqa
import uvicorn
import logging
from fastapi import FastAPI, APIRouter, HTTPException, Request
import ssl
from random import random
from time import sleep
from openfl.protocols import aggregator_models as aggregator_pb2
from openfl.utilities import check_equal
from openfl.utilities import check_is_in
from .utils import named_tensor_pbuf_to_pydantic
import asyncio

logger = logging.getLogger(__name__)


def get_common_name(context: Request):
    return (
        context.scope["transport"]
        .get_extra_info("ssl_object")
        .getpeercert()["subject"][0][0][1]
    )


class AggregatorRESTAPI:
    """gRPC server class for the Aggregator."""

    def __init__(self, aggregator, tls):
        self.aggregator = aggregator
        self.tls = tls
        self.router = APIRouter()

        routes = [
            "GetTasks",
            "GetAggregatedTensor",
            "SendLocalTaskResults",
            "ConnectivityCheck",
            "AddCollaborator",
            "RemoveCollaborator",
            "GetExperimentStatus",
            "SetStragglerCuttoffTime",
            "SetDynamicTaskArg",
            "GetDynamicTaskArg",
        ]

        # Register routes
        for route in routes:
            self.router.add_api_route(
                f"/{route}", getattr(self, route), methods=["POST"]
            )

        self.logger = logging.getLogger(__name__)
        self.lock = asyncio.Lock()

    def validate_collaborator(self, request, context: Request):
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
            common_name = get_common_name(context)
            collaborator_common_name = request.header.sender
            if not self.aggregator.valid_collaborator_cn_and_id(
                common_name, collaborator_common_name
            ):
                # Random delay in authentication failures
                sleep(5 * random())  # nosec
                detail = (
                    f"Invalid collaborator. CN: |{common_name}| "
                    f"collaborator_common_name: |{collaborator_common_name}|"
                )
                raise HTTPException(status_code=401, detail=detail)

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
            common_name = get_common_name(context)
            admin_common_name = request.header.sender
            # Authenticate
            if not self.aggregator.valid_admin_cn_and_id(
                common_name, admin_common_name
            ):
                # Random delay in authentication failures
                sleep(5 * random())  # nosec
                detail = (
                    f"Invalid admin. CN: |{common_name}| "
                    f"admin_common_name: |{admin_common_name}|"
                )
                raise HTTPException(status_code=401, detail=detail)

            # Authorize
            if not self.aggregator.valid_admin_endpoint(
                endpoint_name, admin_common_name
            ):
                detail = (
                    f"This endpoint is not permitted for this admin. Endpoint: |{endpoint_name}| "
                    f"admin_common_name: |{admin_common_name}|"
                )
                raise HTTPException(status_code=403, detail=detail)

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
            single_col_cert_common_name=self.aggregator.single_col_cert_common_name,
        )

    def check_request(self, request):
        """
        Validate request header matches expected values.

        Args:
            request : protobuf
                Request sent from a collaborator that requires validation
        """
        # TODO improve this check. the sender name could be spoofed
        check_is_in(
            request.header.sender, self.aggregator.authorized_cols, self.logger
        )

        # check that the message is for me
        check_equal(request.header.receiver, self.aggregator.uuid, self.logger)

        # check that the message is for my federation
        check_equal(
            request.header.federation_uuid,
            self.aggregator.federation_uuid,
            self.logger,
        )

        # check that we agree on the single cert common name
        check_equal(
            request.header.single_col_cert_common_name,
            self.aggregator.single_col_cert_common_name,
            self.logger,
        )

    def check_admin_request(self, request):
        """
        Validate request header matches expected values.

        Args:
            request : protobuf
                Request sent from an admin that requires validation
        """
        # TODO improve this check. the sender name could be spoofed
        check_is_in(
            request.header.sender,
            self.aggregator.admins_endpoints_mapping.keys(),
            self.logger,
        )

        # check that the message is for me
        check_equal(request.header.receiver, self.aggregator.uuid, self.logger)

        # check that the message is for my federation
        check_equal(
            request.header.federation_uuid,
            self.aggregator.federation_uuid,
            self.logger,
        )

    async def GetTasks(
        self, request: aggregator_pb2.GetTasksRequest, context: Request
    ):  # NOQA:N802
        """
        Request a job from aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        async with self.lock:
            self.validate_collaborator(request, context)
            try:
                self.check_request(request)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            collaborator_name = request.header.sender
            tasks, round_number, sleep_time, time_to_quit = (
                self.aggregator.get_tasks(request.header.sender)
            )
            if tasks:
                if isinstance(tasks[0], str):
                    # backward compatibility
                    tasks_proto = [
                        aggregator_pb2.Task(
                            name=task,
                        )
                        for task in tasks
                    ]
                else:
                    tasks_proto = [
                        aggregator_pb2.Task(
                            name=task.name,
                            function_name=task.function_name,
                            task_type=task.task_type,
                            apply_local=task.apply_local,
                        )
                        for task in tasks
                    ]
            else:
                tasks_proto = []

            return aggregator_pb2.GetTasksResponse(
                header=self.get_header(collaborator_name),
                round_number=round_number,
                tasks=tasks_proto,
                sleep_time=sleep_time,
                quit=time_to_quit,
            )

    async def GetAggregatedTensor(
        self,
        request: aggregator_pb2.GetAggregatedTensorRequest,
        context: Request,
    ):  # NOQA:N802
        """
        Request a job from aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        async with self.lock:
            self.validate_collaborator(request, context)
            try:
                self.check_request(request)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            collaborator_name = request.header.sender
            tensor_name = request.tensor_name
            require_lossless = request.require_lossless
            round_number = request.round_number
            report = request.report
            tags = tuple(request.tags)
            try:
                named_tensor = self.aggregator.get_aggregated_tensor(
                    collaborator_name,
                    tensor_name,
                    round_number,
                    report,
                    tags,
                    require_lossless,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

            return aggregator_pb2.GetAggregatedTensorResponse(
                header=self.get_header(collaborator_name),
                round_number=round_number,
                tensor=named_tensor_pbuf_to_pydantic(named_tensor),
            )

    async def SendLocalTaskResults(
        self, proto: aggregator_pb2.TaskResults, context: Request
    ):  # NOQA:N802
        """
        Request a model download from aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        async with self.lock:
            self.validate_collaborator(proto, context)
            # all messages get sanity checked
            try:
                self.check_request(proto)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

            collaborator_name = proto.header.sender
            task_name = proto.task_name
            round_number = proto.round_number
            data_size = proto.data_size
            named_tensors = proto.tensors
            for i in range(len(named_tensors)):
                named_tensors[i].data_bytes = base64.b64decode(
                    named_tensors[i].data_bytes.encode()
                )
            self.aggregator.send_local_task_results(
                collaborator_name,
                round_number,
                task_name,
                data_size,
                named_tensors,
            )
            # turn data stream into local model update
            return aggregator_pb2.SendLocalTaskResultsResponse(
                header=self.get_header(collaborator_name)
            )

    async def ConnectivityCheck(
        self, request: aggregator_pb2.ConnectivityCheckRequest, context: Request
    ):  # NOQA:N802
        """
        Just connect to the aggregator, to check if there are connectivity issues.
        Called by collaborators

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        async with self.lock:
            self.validate_collaborator(request, context)
            try:
                self.check_request(request)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            collaborator_name = request.header.sender
            self.logger.info(
                f"{collaborator_name} checked connectivity and succeeded."
            )
            return aggregator_pb2.ConnectivityCheckResponse(
                header=self.get_header(collaborator_name)
            )

    def AddCollaborator(
        self, request: aggregator_pb2.AddCollaboratorRequest, context: Request
    ):  # NOQA:N802
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

    def RemoveCollaborator(
        self,
        request: aggregator_pb2.RemoveCollaboratorRequest,
        context: Request,
    ):  # NOQA:N802
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
        metrics = status_dict["metrics"]
        metrics = [
            aggregator_pb2.GetMetricStreamResponse(**metric)
            for metric in metrics
        ]

        collaborators_progress = []
        for collaborator_name in collaborators:
            col_start_time = start_times.get(collaborator_name)
            col_end_times = end_times.get(collaborator_name, {})
            col_task_endtimes = []
            for task_name, end_time in col_end_times.items():
                task_endtime_pb = aggregator_pb2.TaskEndTime(
                    task_name=task_name, end_time=end_time
                )
                col_task_endtimes.append(task_endtime_pb)
            collaborator_progress_pb = aggregator_pb2.CollaboratorProgress(
                col_name=collaborator_name,
                start_time=col_start_time,
                tasks_end_time=col_task_endtimes,
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
            assigned_collaborators=assigned_collaborators,
            metrics=metrics,
        )

    def GetExperimentStatus(
        self,
        request: aggregator_pb2.GetExperimentStatusRequest,
        context: Request,
    ):  # NOQA:N802
        """
        Get experiment status from the aggregator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_admin(request, context, "GetExperimentStatus")
        self.check_admin_request(request)
        admin_name = request.header.sender
        rounds = self.aggregator.get_experiment_status()
        response = aggregator_pb2.GetExperimentStatusResponse(
            header=self.get_header(admin_name),
            rounds=[
                self._prepare_experiment_status_pb(round_) for round_ in rounds
            ],
        )
        return response

    def SetStragglerCuttoffTime(
        self,
        request: aggregator_pb2.SetStragglerCuttoffTimeRequest,
        context: Request,
    ):  # NOQA:N802
        """
        Set a new straggler handler cutoff time, if it exists.

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_admin(request, context, "SetStragglerCuttoffTime")
        self.check_admin_request(request)
        admin_name = request.header.sender
        self.aggregator.set_straggler_cutoff_time(request.timeout_in_seconds)
        return aggregator_pb2.SetStragglerCuttoffTimeResponse(
            header=self.get_header(admin_name)
        )

    def SetDynamicTaskArg(
        self, request: aggregator_pb2.SetDynamicTaskArgRequest, context: Request
    ):  # NOQA:N802
        """
        Set a value for a task argument

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_admin(request, context, "SetDynamicTaskArg")
        self.check_admin_request(request)
        admin_name = request.header.sender
        self.aggregator.set_dynamic_task_arg(
            request.task_name, request.arg_name, request.value
        )
        return aggregator_pb2.SetDynamicTaskArgResponse(
            header=self.get_header(admin_name)
        )

    def GetDynamicTaskArg(
        self, request: aggregator_pb2.GetDynamicTaskArgRequest, context: Request
    ):  # NOQA:N802
        """
        Get a value for a task argument

        Args:
            request: The gRPC message request
            context: The gRPC context

        """
        self.validate_admin(request, context, "GetDynamicTaskArg")
        self.check_admin_request(request)
        admin_name = request.header.sender
        value_dict = self.aggregator.get_dynamic_task_arg(
            request.task_name, request.arg_name
        )
        return aggregator_pb2.GetDynamicTaskArgResponse(
            header=self.get_header(admin_name),
            current_value=value_dict["current_value"],
            next_value=value_dict["next_value"],
        )


class AggregatorRESTServer:
    def __init__(
        self,
        aggregator,
        agg_port,
        tls=True,
        disable_client_auth=False,
        root_certificate=None,
        certificate=None,
        private_key=None,
        **kwargs,
    ):
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
        self.agg_port = agg_port
        self.tls = tls
        self.root_certificate = root_certificate
        self.certificate = certificate
        self.private_key = private_key
        self.disable_client_auth = disable_client_auth

        self.logger = logging.getLogger(__name__)

    def serve(self):
        """Start the REST API server."""

        app = FastAPI()
        aggregator_api = AggregatorRESTAPI(self.aggregator, self.tls)
        app.include_router(aggregator_api.router)

        args = {
            "app": app,
            "host": "0.0.0.0",
            "port": self.agg_port,
        }
        if self.tls:
            args["ssl_certfile"] = self.certificate
            args["ssl_keyfile"] = self.private_key

        if not self.disable_client_auth:
            args["ssl_ca_certs"] = self.root_certificate
            args["ssl_cert_reqs"] = ssl.CERT_REQUIRED

        self.logger.info("Starting Aggregator gRPC Server")
        uvicorn.run(
            **args,
            log_level=os.getenv("OPENFL_REST_LOGLEVEL", "info"),
            timeout_keep_alive=60,
        )
