# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""Aggregator module."""
from copy import deepcopy
import time
import queue
from logging import getLogger

from openfl.interface.aggregation_functions import WeightedAverage
from openfl.component.straggler_handling_functions import CutoffTimeBasedStragglerHandling
from openfl.databases import TensorDB
from openfl.pipelines import NoCompressionPipeline
from openfl.pipelines import TensorCodec
from openfl.protocols import base_pb2
from openfl.protocols import utils
from openfl.utilities import change_tags
from openfl.utilities import TaskResultKey
from openfl.utilities import TensorKey
from openfl.utilities.logs import write_metric


class Aggregator:
    r"""An Aggregator is the central node in federated learning.

    Args:
        aggregator_uuid (str): Aggregation ID.
        federation_uuid (str): Federation ID.
        authorized_cols (list of str): The list of IDs of enrolled collaborators.
        admins_endpoints_mapping* (dict): A mapping of admins common names and authorized endpoints (grpc service names)
        init_state_path* (str): The location of the initial weight file.
        last_state_path* (str): The file location to store the latest weight.
        best_state_path* (str): The file location to store the weight of the best model.
        db_store_rounds* (int): Rounds to store in TensorDB.

    Note:
        \* - plan setting.
    """

    def __init__(self,

                 aggregator_uuid,
                 federation_uuid,
                 authorized_cols,

                 admins_endpoints_mapping,

                 init_state_path,
                 best_state_path,
                 last_state_path,

                 assigner,
                 straggler_handling_policy=None,
                 rounds_to_train=256,
                 single_col_cert_common_name=None,
                 compression_pipeline=None,
                 db_store_rounds=1,
                 write_logs=False,
                 log_metric_callback=None,
                 dynamictaskargs=None,
                 **kwargs):
        """Initialize."""
        self.round_number = 0
        self.single_col_cert_common_name = single_col_cert_common_name

        if self.single_col_cert_common_name is not None:
            self._log_big_warning()
        else:
            # FIXME: '' instead of None is just for protobuf compatibility.
            # Cleaner solution?
            self.single_col_cert_common_name = ''

        self._end_of_round_check_done = [False] * rounds_to_train
        self.straggler_handling_policy_started_for_round = False
        self.stragglers = []
        self.available_collaborators = []

        self.rounds_to_train = rounds_to_train

        # if the collaborator requests a delta, this value is set to true
        self.authorized_cols = authorized_cols
        self.uuid = aggregator_uuid
        self.federation_uuid = federation_uuid
        self.assigner = assigner
        self.quit_job_sent_to = []  # TODO: check how will this behave in the scenario of adding/removing cols

        self.tensor_db = TensorDB()
        # FIXME: I think next line generates an error on the second round
        # if it is set to 1 for the aggregator.
        self.db_store_rounds = db_store_rounds

        # Gathered together logging-related objects
        self.logger = getLogger(__name__)
        self.write_logs = write_logs
        self.log_metric_callback = log_metric_callback

        self.dynamictaskargs = dynamictaskargs

        self.straggler_handling_policy = (
            straggler_handling_policy or CutoffTimeBasedStragglerHandling()
        )

        if self.write_logs:
            self.log_metric = write_metric
            if self.log_metric_callback:
                self.log_metric = log_metric_callback
                self.logger.info(f'Using custom log metric: {self.log_metric}')

        self.best_model_score = None
        self.metric_queue = queue.Queue()

        self.compression_pipeline = compression_pipeline or NoCompressionPipeline()
        self.tensor_codec = TensorCodec(self.compression_pipeline)

        self.init_state_path = init_state_path
        self.best_state_path = best_state_path
        self.last_state_path = last_state_path

        self.best_tensor_dict: dict = {}
        self.last_tensor_dict: dict = {}

        if kwargs.get('initial_tensor_dict', None) is not None:
            self._load_initial_tensors_from_dict(kwargs['initial_tensor_dict'])
            self.model = utils.construct_model_proto(
                tensor_dict=kwargs['initial_tensor_dict'],
                round_number=0,
                tensor_pipe=self.compression_pipeline)
        else:
            self.model: base_pb2.ModelProto = utils.load_proto(self.init_state_path)
            self._load_initial_tensors()  # keys are TensorKeys

        # load in the initial dynamic task args
        self._write_dynamic_task_args()

        self.collaborator_tensor_results = {}  # {TensorKey: nparray}}

        # these enable getting all tensors for a task
        self.collaborator_tasks_results = {}  # {TaskResultKey: list of TensorKeys}

        self.collaborator_task_weight = {}  # {TaskResultKey: data_size}

        # maintain a list of collaborators that have completed task and
        # reported results in a given round
        self.collaborators_done = []

        # this is for monitoring
        self.collaborator_start_time = {}  # {col_name: time relative to round start}
        self.collaborator_end_time = {}  # {col_name: {task_name: time relative to round start}}
        self.previous_round_status = {}  # see self._get_round_status
        self.first_col_start = None

        # for admin authorization
        self.admins_endpoints_mapping = admins_endpoints_mapping

        # new/dropped collaborators
        self.collaborators_to_add = []
        self.collaborators_to_remove = []

    def _load_initial_tensors(self):
        """
        Load all of the tensors required to begin federated learning.

        Required tensors are: \
            1. Initial model.

        Returns:
            None
        """
        tensor_dict, round_number = utils.deconstruct_model_proto(
            self.model, compression_pipeline=self.compression_pipeline)

        if round_number > self.round_number:
            self.logger.info(
                f'Starting training from round {round_number} of previously saved model'
            )
            self.round_number = round_number
        tensor_key_dict = {
            TensorKey(k, self.uuid, self.round_number, False, ('model',)):
                v for k, v in tensor_dict.items()
        }
        # all initial model tensors are loaded here
        self.tensor_db.cache_tensor(tensor_key_dict)
        self.logger.debug(f'This is the initial tensor_db: {self.tensor_db}')

    def _load_initial_tensors_from_dict(self, tensor_dict):
        """
        Load all of the tensors required to begin federated learning.

        Required tensors are: \
            1. Initial model.

        Returns:
            None
        """
        tensor_key_dict = {
            TensorKey(k, self.uuid, self.round_number, False, ('model',)):
                v for k, v in tensor_dict.items()
        }
        # all initial model tensors are loaded here
        self.tensor_db.cache_tensor(tensor_key_dict)
        self.logger.debug(f'This is the initial tensor_db: {self.tensor_db}')

    # MICAH CHANGE BEGIN: new functions for dynamic task args
    def set_dynamic_task_arg(self, task_name, arg_name, value):
        if self.dynamictaskargs is None or \
            task_name not in self.dynamictaskargs or \
            arg_name not in self.dynamictaskargs[task_name]:
            # MICAH TODO: better error
            raise KeyError
        elif value < self.dynamictaskargs[task_name][arg_name]['min'] or \
            value > self.dynamictaskargs[task_name][arg_name]['max']:
            # MICAH TODO: better error
            raise ValueError
        else:
            self.dynamictaskargs[task_name][arg_name]['value'] = value

    def get_dynamic_task_arg(self, task_name, arg_name):
        if self.dynamictaskargs is None or \
            task_name not in self.dynamictaskargs or \
            arg_name not in self.dynamictaskargs[task_name]:
            self.logger.info(f"No such keys {task_name} and {arg_name} in:\n{self.dynamictaskargs}")
            # MICAH TODO: better error
            raise KeyError
        return {
            'current_value': self.tensor_db.get_dynamic_arg(task_name, arg_name, self.round_number, self.uuid), 
            'next_value': self.dynamictaskargs[task_name][arg_name]['value'],
        }

    def _write_dynamic_task_args(self):
        if self.dynamictaskargs is None:
            return

        for task_name in self.dynamictaskargs.keys():
            for arg_name in self.dynamictaskargs[task_name]:
                self.tensor_db.cache_dynamic_arg(
                    task_name=task_name,
                    arg_name=arg_name,
                    agg_id=self.uuid,
                    round_number=self.round_number,
                    value=self.dynamictaskargs[task_name][arg_name]['value']
                )
    # MICAH CHANGE END: new functions for dynamic task args

    def _save_model(self, round_number, file_path):
        """
        Save the best or latest model.

        Args:
            round_number: int
                Model round to be saved
            file_path: str
                Either the best model or latest model file path

        Returns:
            None
        """
        # Extract the model from TensorDB and set it to the new model
        og_tensor_dict, _ = utils.deconstruct_model_proto(
            self.model, compression_pipeline=self.compression_pipeline)
        tensor_keys = [
            TensorKey(
                k, self.uuid, round_number, False, ('model',)
            ) for k, v in og_tensor_dict.items()
        ]
        tensor_dict = {}
        for tk in tensor_keys:
            tk_name, _, _, _, _ = tk
            tensor_dict[tk_name] = self.tensor_db.get_tensor_from_cache(tk)
            if tensor_dict[tk_name] is None:
                self.logger.info(f'Cannot save model for round {round_number}. Continuing...')
                return
        if file_path == self.best_state_path:
            self.best_tensor_dict = tensor_dict
        if file_path == self.last_state_path:
            self.last_tensor_dict = tensor_dict
        self.model = utils.construct_model_proto(
            tensor_dict, round_number, self.compression_pipeline)
        utils.dump_proto(self.model, file_path)

    def valid_collaborator_cn_and_id(self, cert_common_name,
                                     collaborator_common_name):
        """
        Determine if the collaborator certificate and ID are valid for this federation.

        Args:
            cert_common_name: Common name for security certificate
            collaborator_common_name: Common name for collaborator

        Returns:
            bool: True means the collaborator common name matches the name in
                  the security certificate.

        """
        # if self.test_mode_whitelist is None, then the common_name must
        # match collaborator_common_name and be in authorized_cols
        # FIXME: '' instead of None is just for protobuf compatibility.
        #  Cleaner solution?
        if self.single_col_cert_common_name == '':
            return (cert_common_name == collaborator_common_name
                    and collaborator_common_name in self.authorized_cols)
        # otherwise, common_name must be in whitelist and
        # collaborator_common_name must be in authorized_cols
        else:
            return (cert_common_name == self.single_col_cert_common_name
                    and collaborator_common_name in self.authorized_cols)

    def valid_admin_cn_and_id(self, cert_common_name,
                              admin_common_name):
        """
        Determine if the admin certificate and ID are valid for this federation.

        Args:
            cert_common_name: Common name for security certificate
            admin_common_name: Common name for admin

        Returns:
            bool: True means the admin common name matches the name in
                  the security certificate.

        """
        return (cert_common_name == admin_common_name
                and admin_common_name in self.admins_endpoints_mapping.keys())

    def valid_admin_endpoint(self, endpoint_id, admin_common_name):
        """
        Determine if endpoint being called by the admin is permitted

        Args:
            endpoint_id: ID of the endpoint. It corresponds to the grpc service name
                         defined in the protobuf file.
            admin_common_name: Common name of the admin

        Returns:
            bool: True means the endpoint is allowed.

        """
        # This assumes the key exists in the dictionary, since this function
        # is called after authentication
        return endpoint_id in self.admins_endpoints_mapping[admin_common_name]

    def all_quit_jobs_sent(self):
        """Assert all quit jobs are sent to collaborators."""
        return set(self.quit_job_sent_to) == set(self.authorized_cols)

    @staticmethod
    def _get_sleep_time():
        """
        Sleep 10 seconds.

        Returns:
            sleep_time: int
        """
        # Decrease sleep period for finer discretezation
        return 10

    def _time_to_quit(self):
        """
        If all rounds are complete, it's time to quit.

        Returns:
            is_time_to_quit: bool
        """
        if self.round_number >= self.rounds_to_train:
            return True
        return False

    def get_tasks(self, collaborator_name):
        """
        RPC called by a collaborator to determine which tasks to perform.

        Args:
            collaborator_name: str
                Requested collaborator name

        Returns:
            tasks: list[str]
                List of tasks to be performed by the requesting collaborator
                for the current round.
            sleep_time: int
            time_to_quit: bool
        """
        self.logger.debug(
            f'Aggregator GetTasks function reached from collaborator {collaborator_name}...'
        )

        # mark this collaborator as "available"
        if collaborator_name not in self.available_collaborators:
            self.available_collaborators.append(collaborator_name)

        # MICAH TODO: check for the end of the round
        # self._end_of_round_check() # MICAH NOTE: this causes: 
            # File "/usr/local/lib/python3.10/dist-packages/openfl/component/aggregator/aggregator.py", line 838, in
            # _compute_validation_related_task_metrics
            # 'metric_value': float(agg_results),
            # TypeError: float() argument must be a string or a real number, not 'tuple'

        # if it is time to quit, inform the collaborator
        if self._time_to_quit():
            self.logger.info(f'Sending signal to collaborator {collaborator_name} to shutdown...')
            self.quit_job_sent_to.append(collaborator_name)

            tasks = None
            sleep_time = 0
            time_to_quit = True

            return tasks, self.round_number, sleep_time, time_to_quit

        time_to_quit = False
        # otherwise, get the tasks from our task assigner
        tasks = self.assigner.get_tasks_for_collaborator(collaborator_name, self.round_number)

        # if no tasks, tell the collaborator to sleep
        if len(tasks) == 0:
            tasks = None
            sleep_time = self._get_sleep_time()

            return tasks, self.round_number, sleep_time, time_to_quit

        # if we do have tasks, remove any that we already have results for
        if isinstance(tasks[0], str):
            # backward compatibility
            tasks = [
                t for t in tasks if not self._collaborator_task_completed(
                    collaborator_name, t, self.round_number)
            ]
            if collaborator_name in self.stragglers:
                tasks = []

        else:
            tasks = [
                t for t in tasks if not self._collaborator_task_completed(
                    collaborator_name, t.name, self.round_number)
            ]
            if collaborator_name in self.stragglers:
                tasks = []

        # Do the check again because it's possible that all tasks have
        # been completed
        if len(tasks) == 0:
            tasks = None
            sleep_time = self._get_sleep_time()

            return tasks, self.round_number, sleep_time, time_to_quit

        self.logger.info(
            f'Sending tasks to collaborator {collaborator_name} for round {self.round_number}'
        )
        sleep_time = 0

        # Start straggler handling policy for timer based callback is required
        # for %age based policy callback is not required
        if not self.straggler_handling_policy_started_for_round:
            self.straggler_handling_policy_started_for_round = True
            self.straggler_handling_policy.start_policy(
                callback=self._straggler_cutoff_time_elapsed
            )
            cutoff = None
            if hasattr(self.straggler_handling_policy, 'straggler_cutoff_time'):
                cutoff = self.straggler_handling_policy.straggler_cutoff_time
            self.logger.info(f'Started straggler handling policy with cutoff time {cutoff}...')

        start_time = time.time()
        if self.first_col_start is None:
            # first_col_start used to differ from straggler_handling_policy.round_start_time
            # the former corresponds to the time the first collaborator starts,
            # while the latter used to correspond to the time the last collaborator starts.
            # anyway we choose to track them separately.
            self.first_col_start = start_time

        if collaborator_name not in self.collaborator_start_time:
            # NOTE: It seems that a collaborator will recieve tasks only once per round
            # even if there are multiple tasks (is this true?), but anyway I am recording
            # the start time only one time per round
            self.collaborator_start_time[collaborator_name] = start_time - self.first_col_start

        return tasks, self.round_number, sleep_time, time_to_quit

    def _straggler_cutoff_time_elapsed(self) -> None:
        """
        This method is called by the straggler handling policy when cutoff timer is elapsed.
        It applies straggler handling policy and ends the round early.

        Returns:
            None
        """
        self.logger.info(
            f"Round number: {self.round_number} cutoff timer elapsed after "
            f"{self.straggler_handling_policy.straggler_cutoff_time}s.\n"
            f"Applying {self.straggler_handling_policy.__class__.__name__} policy.\n"
            f"Collaborators done: {self.collaborators_done}.\n"
            f"Collaborators assigned in round being cut off: {self.assigner.get_assigned_collaborators()}\n."
        )

        # Check if minimum collaborators reported results
        if self.straggler_handling_policy.straggler_cutoff_check(
            len(self.collaborators_done), len(self.assigner.get_assigned_collaborators())
        ):
            self._end_round_due_to_stragglers()

    def get_aggregated_tensor(self, collaborator_name, tensor_name,
                              round_number, report, tags, require_lossless):
        """
        RPC called by collaborator.

        Performs local lookup to determine if there is an aggregated tensor available \
            that matches the request.

        Args:
            collaborator_name : str
                Requested tensor key collaborator name
            tensor_name: str
            require_lossless: bool
            round_number: int
            report: bool
            tags: tuple[str, ...]
        Returns:
            named_tensor : protobuf NamedTensor
                the tensor requested by the collaborator
        """
        self.logger.debug(f'Retrieving aggregated tensor {tensor_name},{round_number},{tags} '
                          f'for collaborator {collaborator_name}')

        if 'compressed' in tags or require_lossless:
            compress_lossless = True
        else:
            compress_lossless = False

        # TODO the TensorDB doesn't support compressed data yet.
        #  The returned tensor will
        # be recompressed anyway.
        if 'compressed' in tags:
            tags = change_tags(tags, remove_field='compressed')
        if 'lossy_compressed' in tags:
            tags = change_tags(tags, remove_field='lossy_compressed')

        tensor_key = TensorKey(
            tensor_name, self.uuid, round_number, report, tags
        )
        tensor_name, origin, round_number, report, tags = tensor_key

        if 'aggregated' in tags and 'delta' in tags and round_number != 0:
            agg_tensor_key = TensorKey(
                tensor_name, origin, round_number, report, ('aggregated',)
            )
        else:
            agg_tensor_key = tensor_key

        nparray = self.tensor_db.get_tensor_from_cache(agg_tensor_key)

        start_retrieving_time = time.time()
        while (nparray is None):
            self.logger.debug(f'Waiting for tensor_key {agg_tensor_key}')
            time.sleep(5)
            nparray = self.tensor_db.get_tensor_from_cache(agg_tensor_key)
            if (time.time() - start_retrieving_time) > 60:
                break

        if nparray is None:
            raise ValueError(f'Aggregator does not have an aggregated tensor for {tensor_key}')

        # quite a bit happens in here, including compression, delta handling,
        # etc...
        # we might want to cache these as well
        named_tensor = self._nparray_to_named_tensor(
            agg_tensor_key,
            nparray,
            send_model_deltas=True,
            compress_lossless=compress_lossless
        )

        return named_tensor

    def _nparray_to_named_tensor(self, tensor_key, nparray, send_model_deltas,
                                 compress_lossless):
        """
        Construct the NamedTensor Protobuf.

        Also includes logic to create delta, compress tensors with the TensorCodec, etc.
        """
        tensor_name, origin, round_number, report, tags = tensor_key
        # if we have an aggregated tensor, we can make a delta
        if 'aggregated' in tags and send_model_deltas:
            # Should get the pretrained model to create the delta. If training
            # has happened, Model should already be stored in the TensorDB
            model_tk = TensorKey(tensor_name,
                                 origin,
                                 round_number - 1,
                                 report,
                                 ('model',))

            model_nparray = self.tensor_db.get_tensor_from_cache(model_tk)

            assert (model_nparray is not None), (
                'The original model layer should be present if the latest '
                'aggregated model is present')
            delta_tensor_key, delta_nparray = self.tensor_codec.generate_delta(
                tensor_key,
                nparray,
                model_nparray
            )
            delta_comp_tensor_key, delta_comp_nparray, metadata = self.tensor_codec.compress(
                delta_tensor_key,
                delta_nparray,
                lossless=compress_lossless
            )
            named_tensor = utils.construct_named_tensor(
                delta_comp_tensor_key,
                delta_comp_nparray,
                metadata,
                lossless=compress_lossless
            )

        else:
            # Assume every other tensor requires lossless compression
            compressed_tensor_key, compressed_nparray, metadata = self.tensor_codec.compress(
                tensor_key,
                nparray,
                require_lossless=True
            )
            named_tensor = utils.construct_named_tensor(
                compressed_tensor_key,
                compressed_nparray,
                metadata,
                lossless=compress_lossless
            )

        return named_tensor

    def _collaborator_task_completed(self, collaborator, task_name, round_num):
        """
        Check if the collaborator has completed the task for the round.

        The aggregator doesn't actually know which tensors should be sent from the collaborator \
            so it must to rely specifically on the presence of previous results

        Args:
            collaborator : str
                collaborator to check if their task has been completed
            task_name : str
                The name of the task (TaskRunner function)
            round_num : int

        Returns:
            task_competed : bool
                Whether or not the collaborator has completed the task for this
                round
        """
        task_key = TaskResultKey(task_name, collaborator, round_num)
        return task_key in self.collaborator_tasks_results

    def send_local_task_results(self, collaborator_name, round_number, task_name,
                                data_size, named_tensors):
        """
        RPC called by collaborator.

        Transmits collaborator's task results to the aggregator.

        Args:
            collaborator_name: str
            task_name: str
            round_number: int
            data_size: int
            named_tensors: protobuf NamedTensor
        Returns:
             None
        """
        if self._time_to_quit() or collaborator_name in self.stragglers:
            self.logger.warning(
                f'STRAGGLER: Collaborator {collaborator_name} is reporting results '
                f'after task {task_name} has finished.'
            )
            return

        if self.round_number != round_number:
            self.logger.warning(
                f'Collaborator {collaborator_name} is reporting results'
                f' for the wrong round: {round_number}. Ignoring...'
            )
            return

        self.logger.info(
            f'Collaborator {collaborator_name} is sending task results '
            f'for {task_name}, round {round_number}'
        )

        task_key = TaskResultKey(task_name, collaborator_name, round_number)

        # we mustn't have results already
        if self._collaborator_task_completed(
            collaborator_name, task_name, round_number
        ):
            raise ValueError(
                f'Aggregator already has task results from collaborator {collaborator_name}'
                f' for task {task_key}'
            )

        # By giving task_key it's own weight, we can support different
        # training/validation weights
        # As well as eventually supporting weights that change by round
        # (if more data is added)
        self.collaborator_task_weight[task_key] = data_size

        # initialize the list of tensors that go with this task
        # Setting these incrementally is leading to missing values
        task_results = []

        for named_tensor in named_tensors:
            # quite a bit happens in here, including decompression, delta
            # handling, etc...
            tensor_key, value = self._process_named_tensor(
                named_tensor, collaborator_name)

            if 'metric' in tensor_key.tags:
                # Caution: This schema must be followed. It is also used in
                # gRPC message streams for director/envoy.
                metrics = {
                    'round': round_number,
                    'metric_origin': collaborator_name,
                    'task_name': task_name,
                    'metric_name': tensor_key.tensor_name,
                    'metric_value': float(value),
                }
                self.metric_queue.put(metrics)
                self.logger.metric("%s", str(metrics))

            task_results.append(tensor_key)

        self.collaborator_tasks_results[task_key] = task_results
        if collaborator_name not in self.collaborator_end_time:
            self.collaborator_end_time[collaborator_name] = {}
        self.collaborator_end_time[collaborator_name][task_name] = (
            time.time() - self.first_col_start)

        self._is_collaborator_done(collaborator_name)

        # if all assigned collaborators are done, end the round
        if len(self.collaborators_done) == len(self.assigner.get_assigned_collaborators()):
            self._end_of_round_check()

        # Check if straggler handler calls for round end
        elif self.straggler_handling_policy.straggler_cutoff_check(
            len(self.collaborators_done), len(self.assigner.get_assigned_collaborators())
        ):
            self._end_round_due_to_stragglers()

    def _end_round_due_to_stragglers(self):
        # determine stragglers
        self.stragglers = [
            collab_name for collab_name in self.assigner.get_assigned_collaborators()
            if collab_name not in self.collaborators_done
        ]
        if len(self.stragglers) != 0:
            self.logger.warning(
                f"Identified straggler collaborators: {self.stragglers}"
            )
        # end the round
        self._end_of_round_check()

    def _process_named_tensor(self, named_tensor, collaborator_name):
        """
        Extract the named tensor fields.

        Performs decompression, delta computation, and inserts results into TensorDB.

        Args:
            named_tensor:       NamedTensor (protobuf)
                protobuf that will be extracted from and processed
            collaborator_name:  str
                Collaborator name is needed for proper tagging of resulting
                tensorkeys

        Returns:
            tensor_key : TensorKey (named_tuple)
                The tensorkey extracted from the protobuf
            nparray : np.array
                The numpy array associated with the returned tensorkey
        """
        raw_bytes = named_tensor.data_bytes
        metadata = [{'int_to_float': proto.int_to_float,
                     'int_list': proto.int_list,
                     'bool_list': proto.bool_list}
                    for proto in named_tensor.transformer_metadata]
        # The tensor has already been transfered to aggregator,
        # so the newly constructed tensor should have the aggregator origin
        tensor_key = TensorKey(
            named_tensor.name,
            self.uuid,
            named_tensor.round_number,
            named_tensor.report,
            tuple(named_tensor.tags)
        )
        tensor_name, origin, round_number, report, tags = tensor_key
        assert ('compressed' in tags or 'lossy_compressed' in tags), (
            f'Named tensor {tensor_key} is not compressed'
        )
        if 'compressed' in tags:
            dec_tk, decompressed_nparray = self.tensor_codec.decompress(
                tensor_key,
                data=raw_bytes,
                transformer_metadata=metadata,
                require_lossless=True
            )
            dec_name, dec_origin, dec_round_num, dec_report, dec_tags = dec_tk
            # Need to add the collaborator tag to the resulting tensor
            new_tags = change_tags(dec_tags, add_field=collaborator_name)

            # layer.agg.n.trained.delta.col_i
            decompressed_tensor_key = TensorKey(
                dec_name, dec_origin, dec_round_num, dec_report, new_tags
            )
        if 'lossy_compressed' in tags:
            dec_tk, decompressed_nparray = self.tensor_codec.decompress(
                tensor_key,
                data=raw_bytes,
                transformer_metadata=metadata,
                require_lossless=False
            )
            dec_name, dec_origin, dec_round_num, dec_report, dec_tags = dec_tk
            new_tags = change_tags(dec_tags, add_field=collaborator_name)
            # layer.agg.n.trained.delta.lossy_decompressed.col_i
            decompressed_tensor_key = TensorKey(
                dec_name, dec_origin, dec_round_num, dec_report, new_tags
            )

        if 'delta' in tags:
            base_model_tensor_key = TensorKey(
                tensor_name, origin, round_number, report, ('model',)
            )
            base_model_nparray = self.tensor_db.get_tensor_from_cache(
                base_model_tensor_key
            )
            if base_model_nparray is None:
                raise ValueError(f'Base model {base_model_tensor_key} not present in TensorDB')
            final_tensor_key, final_nparray = self.tensor_codec.apply_delta(
                decompressed_tensor_key,
                decompressed_nparray, base_model_nparray
            )
        else:
            final_tensor_key = decompressed_tensor_key
            final_nparray = decompressed_nparray

        assert (final_nparray is not None), f'Could not create tensorkey {final_tensor_key}'
        self.tensor_db.cache_tensor({final_tensor_key: final_nparray})
        self.logger.debug(f'Created TensorKey: {final_tensor_key}')

        return final_tensor_key, final_nparray

    # TODO: To be removed after review
    def _end_of_task_check(self, task_name):
        """
        Check whether all collaborators who are supposed to perform the task complete.

        Args:
            task_name : str
                The task name to check

        Returns:
            complete : boolean
                Is the task done
        """
        if self._is_task_done(task_name):
            # now check for the end of the round
            self._end_of_round_check()

    def _prepare_trained(self, tensor_name, origin, round_number, report, agg_results):
        """
        Prepare aggregated tensorkey tags.

        Args:
           tensor_name : str
           origin:
           round_number: int
           report: bool
           agg_results: np.array
        """
        # The aggregated tensorkey tags should have the form of
        # 'trained' or 'trained.lossy_decompressed'
        # They need to be relabeled to 'aggregated' and
        # reinserted. Then delta performed, compressed, etc.
        # then reinserted to TensorDB with 'model' tag

        # First insert the aggregated model layer with the
        # correct tensorkey
        agg_tag_tk = TensorKey(
            tensor_name,
            origin,
            round_number + 1,
            report,
            ('aggregated',)
        )
        self.tensor_db.cache_tensor({agg_tag_tk: agg_results})

        # Create delta and save it in TensorDB
        base_model_tk = TensorKey(
            tensor_name,
            origin,
            round_number,
            report,
            ('model',)
        )
        base_model_nparray = self.tensor_db.get_tensor_from_cache(base_model_tk)
        if base_model_nparray is not None:
            delta_tk, delta_nparray = self.tensor_codec.generate_delta(
                agg_tag_tk,
                agg_results,
                base_model_nparray
            )
        else:
            # This condition is possible for base model
            # optimizer states (i.e. Adam/iter:0, SGD, etc.)
            # These values couldn't be present for the base
            # model because no training occurs on the aggregator
            delta_tk, delta_nparray = agg_tag_tk, agg_results

        # Compress lossless/lossy
        compressed_delta_tk, compressed_delta_nparray, metadata = self.tensor_codec.compress(
            delta_tk, delta_nparray
        )

        # TODO extend the TensorDB so that compressed data is
        #  supported. Once that is in place
        # the compressed delta can just be stored here instead
        # of recreating it for every request

        # Decompress lossless/lossy
        decompressed_delta_tk, decompressed_delta_nparray = self.tensor_codec.decompress(
            compressed_delta_tk,
            compressed_delta_nparray,
            metadata
        )

        self.tensor_db.cache_tensor({decompressed_delta_tk: decompressed_delta_nparray})

        # Apply delta (unless delta couldn't be created)
        if base_model_nparray is not None:
            self.logger.debug(f'Applying delta for layer {decompressed_delta_tk[0]}')
            new_model_tk, new_model_nparray = self.tensor_codec.apply_delta(
                decompressed_delta_tk,
                decompressed_delta_nparray,
                base_model_nparray
            )
        else:
            new_model_tk, new_model_nparray = decompressed_delta_tk, decompressed_delta_nparray

        # Now that the model has been compressed/decompressed
        # with delta operations,
        # Relabel the tags to 'model'
        (new_model_tensor_name, new_model_origin, new_model_round_number,
         new_model_report, new_model_tags) = new_model_tk
        final_model_tk = TensorKey(
            new_model_tensor_name,
            new_model_origin,
            new_model_round_number,
            new_model_report,
            ('model',)
        )

        # Finally, cache the updated model tensor
        self.tensor_db.cache_tensor({final_model_tk: new_model_nparray})

    def _compute_validation_related_task_metrics(self, task_name):
        """
        Compute all validation related metrics.

        Args:
            task_name : str
                The task name to compute
        """
        # By default, print out all of the metrics that the validation
        # task sent
        # This handles getting the subset of collaborators that may be
        # part of the validation task
        all_collaborators_for_task = self.assigner.get_collaborators_for_task(
            task_name, self.round_number
        )
        # Leave out straggler for the round even if they've paritally
        # completed given tasks
        collaborators_for_task = []
        for c in all_collaborators_for_task:
            if c in self.collaborators_done:
                collaborators_for_task.append(c)

        # The collaborator data sizes for that task
        collaborator_weights_unnormalized = {
            c: self.collaborator_task_weight[TaskResultKey(task_name, c, self.round_number)]
            for c in collaborators_for_task}
        weight_total = sum(collaborator_weights_unnormalized.values())
        collaborator_weight_dict = {
            k: v / weight_total
            for k, v in collaborator_weights_unnormalized.items()
        }

        # The validation task should have just a couple tensors (i.e.
        # metrics) associated with it. Because each collaborator should
        # have sent the same tensor list, we can use the first
        # collaborator in our subset, and apply the correct
        # transformations to the tensorkey to resolve the aggregated
        # tensor for that round
        task_agg_function = self.assigner.get_aggregation_type_for_task(task_name)
        task_key = TaskResultKey(task_name, collaborators_for_task[0], self.round_number)

        for tensor_key in self.collaborator_tasks_results[task_key]:
            tensor_name, origin, round_number, report, tags = tensor_key
            assert (collaborators_for_task[0] in tags), (
                f'Tensor {tensor_key} in task {task_name} has not been processed correctly'
            )
            # Strip the collaborator label, and lookup aggregated tensor
            new_tags = change_tags(tags, remove_field=collaborators_for_task[0])
            agg_tensor_key = TensorKey(tensor_name, origin, round_number, report, new_tags)
            agg_function = WeightedAverage() if 'metric' in tags else task_agg_function
            agg_results = self.tensor_db.get_aggregated_tensor(
                agg_tensor_key, collaborator_weight_dict, aggregation_function=agg_function)

            if report:
                # Caution: This schema must be followed. It is also used in
                # gRPC message streams for director/envoy.
                metrics = {
                    'metric_origin': 'aggregator',
                    'task_name': task_name,
                    'metric_name': tensor_key.tensor_name,
                    'metric_value': float(agg_results),
                    'round': round_number,
                }
                if agg_results is None:
                    self.logger.warning(
                        f'Aggregated metric {agg_tensor_key} could not be collected '
                        f'for round {self.round_number}. Skipping reporting for this round')
                self.metric_queue.put(metrics)
                self.logger.metric("%s", metrics)

                # FIXME: Configurable logic for min/max criteria in saving best.
                if 'validate_agg' in tags:
                    # Compare the accuracy of the model, potentially save it
                    if self.best_model_score is None or self.best_model_score < agg_results:
                        self.logger.metric(f'Round {round_number}: saved the best '
                                           f'model with score {agg_results:f}')
                        self.best_model_score = agg_results
                        self._save_model(round_number, self.best_state_path)
            if 'trained' in tags:
                self._prepare_trained(tensor_name, origin, round_number, report, agg_results)

    def _get_round_status(self):
        status = {
            "round": self.round_number,
            "round_start": self.first_col_start,
            "collaborators": self.authorized_cols,
            "start_times": self.collaborator_start_time,
            "end_times": self.collaborator_end_time,
            "stragglers": self.stragglers,
            "to_add_next_round": self.collaborators_to_add,
            "to_remove_next_round": self.collaborators_to_remove,
            "available_collaborators": self.available_collaborators,
            "assigned_collaborators": self.assigner.get_assigned_collaborators() or [],
        }
        return status

    def get_experiment_status(self):
        """
        RPC called by admin.

        Returns experiment status for the current and previous rounds.
        """
        current_round_status = self._get_round_status()
        previous_round_status = self.previous_round_status
        return current_round_status, previous_round_status

    def add_collaborator(self, collaborator_label, collaborator_cn):
        """
        RPC called by admin.

        Adds a collaborator to the federation. This function will save
        the collaborator in a temporary list. Actual addition of the collaborator
        will happen at the end of the current round.

        Args:
            collaborator_label: Collaborator common name  # TODO: fix after merging #944
            collaborator_cn: Collaborator common name
        Returns:
             None
        """
        # check if this collaborator was requested to be removed
        if (collaborator_label, collaborator_cn) in self.collaborators_to_remove:
            self.collaborators_to_remove.remove(collaborator_cn)
            return

        # check if this collaborator was already requested to be added
        if (collaborator_label, collaborator_cn) in self.collaborators_to_add:
            raise ValueError(f'{collaborator_cn} was already requested to be added')

        # check if this collaborator was already authorized
        if collaborator_cn in self.authorized_cols:
            raise ValueError(f'{collaborator_cn} was already authorized')

        self.collaborators_to_add.append((collaborator_label, collaborator_cn))

    def remove_collaborator(self, collaborator_label, collaborator_cn):
        """
        RPC called by admin.

        Removes a collaborator from the federation. This function will save
        the collaborator in a temporary list. Actual removal of the collaborator
        will happen at the end of the current round.

        Args:
            collaborator_label: Collaborator common name  # TODO: fix after merging #944
            collaborator_cn: Collaborator common name
        Returns:
             None
        """
        # check if this collaborator was requested to be added
        if (collaborator_label, collaborator_cn) in self.collaborators_to_add:
            self.collaborators_to_add.remove(collaborator_cn)
            return

        # check if this collaborator was already requested to be removed
        if (collaborator_label, collaborator_cn) in self.collaborators_to_remove:
            raise ValueError(f'{collaborator_cn} was already requested to be removed')

        # check if this collaborator was already not authorized
        if collaborator_cn not in self.authorized_cols:
            raise ValueError(f'{collaborator_cn} is already not authorized')

        self.collaborators_to_remove.append((collaborator_label, collaborator_cn))

    def set_straggler_cutoff_time(self, straggler_cutoff_time):
        if self.straggler_handling_policy is None:
            self.logger.info(f'Aggregator has no straggler_handling_policy. Skipping set_straggler_cutoff_time')
            return

        if not hasattr(self.straggler_handling_policy, 'set_straggler_cutoff_time'):
            self.logger.info(f'Aggregator straggler_handling_policy has no method for set_straggler_cutoff_time. Skipping call')
            return

        self.straggler_handling_policy.set_straggler_cutoff_time(straggler_cutoff_time)
        self.logger.info(f"Set straggler_cutoff_time to {self.straggler_handling_policy.straggler_cutoff_time}")


    def _end_of_round_check(self):
        """
        Check if the round complete.

        If so, perform many end of round operations,
        such as model aggregation, metric reporting, delta generation (+
        associated tensorkey labeling), and save the model

        Args:
            None

        Returns:
            None
        """
        self.logger.info(f'End of round check called...')
        if self._end_of_round_check_done[self.round_number]:
            return
        self.logger.info(f'Doing end of round...')

        # Compute all validation related metrics
        all_tasks = self.assigner.get_all_tasks_for_round(self.round_number)
        for task_name in all_tasks:
            self._compute_validation_related_task_metrics(task_name)

        # Save the round status
        self.previous_round_status = deepcopy(self._get_round_status())

        # Reset monitoring attributes
        self.collaborator_start_time = {}
        self.collaborator_end_time = {}
        self.first_col_start = None

        # Once all of the task results have been processed
        self._end_of_round_check_done[self.round_number] = True
        self.round_number += 1

        # add new collaborators to available list as needed
        # MICAH TODO: should this be label or CN? Need to check with Hasan
        for col_label, col_cn in self.collaborators_to_add:
            if col_cn not in self.available_collaborators:
                self.available_collaborators.append(col_cn)

        # remove new collaborators from available list as needed
        # MICAH TODO: should this be label or CN? Need to check with Hasan
        for col_label, col_cn in self.collaborators_to_remove:
            if col_cn in self.available_collaborators:
                self.available_collaborators.remove(col_cn)

        self.logger.info(f'Calling assigner for end of round.')
        self.logger.info(f'available_collaborators: {self.available_collaborators}')
        self.logger.info(f'stragglers: {self.stragglers}')

        self.logger.info(f'My assigner is a {type(self.assigner)}')

        # inform the task assigner of end of round and pass relevant state information
        # TODO: this should really be an event mechanism rather than hardcoded callbacks
        self.assigner.end_of_round(available_collaborators=self.available_collaborators,
                                   stragglers=self.stragglers,
                                   next_round=self.round_number)
    
        self.logger.info(f'Collaborators given assignments in the next round: {self.assigner.get_assigned_collaborators()}')

        # TODO: this should really be a clean "round state reset" function
        # such a state object would also be a clean object to pass to an event handler
        # resetting stragglers for task for a new round
        self.straggler_handling_policy_started_for_round = False
        self.stragglers = []
        # resetting available collaborators for a new round
        self.available_collaborators = []
        # resetting collaborators_done for next round
        self.collaborators_done = []

        # Save the latest model
        self.logger.info(f'Saving round {self.round_number} model...')
        self._save_model(self.round_number, self.last_state_path)

        # TODO This needs to be fixed!
        if self._time_to_quit():
            self.logger.info('Experiment Completed. Cleaning up...')
        else:
            self.logger.info(f'Starting round {self.round_number}...')

        # Cleaning tensor db
        self.tensor_db.clean_up(self.db_store_rounds)
        # Reset straggler handling policy for the next round.
        self.straggler_handling_policy.reset_policy_for_round()

        # MICAH CHANGE: set dynamic task arg values
        self._write_dynamic_task_args()

        collaborators_changed = False
        if (len(self.collaborators_to_add) + len(self.collaborators_to_remove)) > 0:
            collaborators_changed = True
            self.logger.info(f'Authorized collaborators for the round that ended: {self.authorized_cols}')
        else:
            self.logger.info(f'No change to authorized collaborators. These lists should be empty:')
        self.logger.info(f'Collaborators to add: {self.collaborators_to_add}')
        self.logger.info(f'Collaborators to remove: {self.collaborators_to_remove}')

        # Adding any new collaborators
        for col_label, col_cn in self.collaborators_to_add:
            self.authorized_cols.append(col_cn)  # TODO: modify this after merging #944
            self.assigner.add_collaborator(col_label, col_cn)
        self.collaborators_to_add.clear()

        # removing any dropped collaborators
        for col_label, col_cn in self.collaborators_to_remove:
            self.authorized_cols.remove(col_cn)  # TODO: modify this after merging #944
            self.assigner.remove_collaborator(col_label, col_cn)
        self.collaborators_to_remove.clear()

        if collaborators_changed:
            self.logger.info(f'Authorized collaborators for the next round: {self.authorized_cols}')


    # TODO: To be removed after review
    def _is_task_done(self, task_name):
        """Check that task is done."""
        all_collaborators = self.assigner.get_collaborators_for_task(
            task_name, self.round_number
        )

        collaborators_done = []
        for c in all_collaborators:
            if self._collaborator_task_completed(
                c, task_name, self.round_number
            ):
                collaborators_done.append(c)

        straggler_check = self.straggler_handling_policy.straggler_cutoff_check(
            len(collaborators_done), len(all_collaborators)
        )

        # all are done or straggler policy calls for early round end.
        return straggler_check or len(all_collaborators) == len(collaborators_done)

    def _is_collaborator_done(self, collaborator_name: str) -> None:
        """
        Check if all tasks given to the collaborator are completed then,
        completed or not.
        """
        # Get all tasks given to the collaborator for current round
        all_tasks = self.assigner.get_tasks_for_collaborator(
            collaborator_name, self.round_number
        )
        # Check if all given tasks are completed by the collaborator
        all_tasks_completed = True
        for task in all_tasks:
            if hasattr(task, "name"):
                task = task.name
            all_tasks_completed = (
                all_tasks_completed and self._collaborator_task_completed(
                    collaborator=collaborator_name, task_name=task,
                    round_num=self.round_number
                )
            )
        # If the collaborator has completed ALL tasks for current round,
        # update collaborators_done
        if all_tasks_completed:
            self.collaborators_done.append(collaborator_name)
            self.logger.info(
                f"Round: {self.round_number}, Collaborators that have completed all tasks: "
                f"{self.collaborators_done}"
            )

    # TODO: To be removed after review
    def _is_round_done(self):
        """Check that round is done."""
        tasks_for_round = self.assigner.get_all_tasks_for_round(self.round_number)

        return all(
            self._is_task_done(
                task_name) for task_name in tasks_for_round)

    def _log_big_warning(self):
        """Warn user about single collaborator cert mode."""
        self.logger.warning(
            f'\n{the_dragon}\nYOU ARE RUNNING IN SINGLE COLLABORATOR CERT MODE! THIS IS'
            f' NOT PROPER PKI AND '
            f'SHOULD ONLY BE USED IN DEVELOPMENT SETTINGS!!!! YE HAVE BEEN'
            f' WARNED!!!'
        )

    def stop(self, failed_collaborator: str = None) -> None:
        """Stop aggregator execution."""
        self.logger.info('Force stopping the aggregator execution.')
        # We imitate quit_job_sent_to the failed collaborator
        # So the experiment set to a finished state
        if failed_collaborator:
            self.quit_job_sent_to.append(failed_collaborator)

        # This code does not actually send `quit` tasks to collaborators,
        # it just mimics it by filling arrays.
        for collaborator_name in filter(lambda c: c != failed_collaborator, self.authorized_cols):
            self.logger.info(f'Sending signal to collaborator {collaborator_name} to shutdown...')
            self.quit_job_sent_to.append(collaborator_name)


the_dragon = '''

 ,@@.@@+@@##@,@@@@.`@@#@+  *@@@@ #@##@  `@@#@# @@@@@   @@    @@@@` #@@@ :@@ `@#`@@@#.@
  @@ #@ ,@ +. @@.@* #@ :`   @+*@ .@`+.   @@ *@::@`@@   @@#  @@  #`;@`.@@ @@@`@`#@* +:@`
  @@@@@ ,@@@  @@@@  +@@+    @@@@ .@@@    @@ .@+:@@@:  .;+@` @@ ,;,#@` @@ @@@@@ ,@@@* @
  @@ #@ ,@`*. @@.@@ #@ ,;  `@+,@#.@.*`   @@ ,@::@`@@` @@@@# @@`:@;*@+ @@ @`:@@`@ *@@ `
 .@@`@@,+@+;@.@@ @@`@@;*@  ;@@#@:*@+;@  `@@;@@ #@**@+;@ `@@:`@@@@  @@@@.`@+ .@ +@+@*,@
  `` ``     ` ``  .     `     `      `     `    `  .` `  ``   ``    ``   `       .   `



                                            .**
                                      ;`  `****:
                                     @**`*******
                         ***        +***********;
                        ,@***;` .*:,;************
                        ;***********@@***********
                        ;************************,
                        `*************************
                         *************************
                         ,************************
                          **#*********************
                          *@****`     :**********;
                          +**;          .********.
                          ;*;            `*******#:                       `,:
                                          ****@@@++::                ,,;***.
                                          *@@@**;#;:         +:      **++*,
                                          @***#@@@:          +*;     ,****
                                          @*@+****           ***`     ****,
                                         ,@#******.  ,       ****     **;,**.
                                         * ******** :,       ;*:*+    **  :,**
                                        #  ********::      *,.*:**`   *      ,*;
                                        .  *********:      .+,*:;*:   :      `:**
                                       ;   :********:       ***::**   `       ` **
                                       +   :****::***  ,    *;;::**`             :*
                                      ``   .****::;**:::    *;::::*;              ;*
                                      *     *****::***:.    **::::**               ;:
                                      #     *****;:****     ;*::;***               ,*`
                                      ;     ************`  ,**:****;               ::*
                                      :     *************;:;*;*++:                   *.
                                      :     *****************;*                      `*
                                     `.    `*****************;  :                     *.
                                     .`    .*+************+****;:                     :*
                                     `.    :;+***********+******;`    :              .,*
                                      ;    ::*+*******************. `::              .`:.
                                      +    :::**********************;;:`                *
                                      +    ,::;*************;:::*******.                *
                                      #    `:::+*************:::;********  :,           *
                                      @     :::***************;:;*********;:,           *
                                      @     ::::******:*********************:         ,:*
                                      @     .:::******:;*********************,         :*
                                      #      :::******::******###@*******;;****        *,
                                      #      .::;*****::*****#****@*****;:::***;  ``  **
                                      *       ::;***********+*****+#******::*****,,,,**
                                      :        :;***********#******#******************
                                      .`       `;***********#******+****+************
                                      `,        ***#**@**+***+*****+**************;`
                                       ;         *++**#******#+****+`      `.,..
                                       +         `@***#*******#****#
                                       +          +***@********+**+:
                                       *         .+**+;**;;;**;#**#
                                      ,`         ****@         +*+:
                                      #          +**+         :+**
                                      @         ;**+,       ,***+
                                      #      #@+****      *#****+
                                     `;     @+***+@      `#**+#++
                                     #      #*#@##,      .++:.,#
                                    `*      @#            +.
                                  @@@
                                 # `@
                                  ,                                                        '''
