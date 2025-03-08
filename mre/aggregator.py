import numpy as np
from threading import Lock

import yaml

import aggregator_pb2


class Aggregator:
    def __init__(self, authorized_cols, rounds_to_train):
        """Initialize."""

        self.tasks_done = {}
        for col in authorized_cols:
            self.tasks_done[col] = {i: [] for i in range(rounds_to_train)}

        self.all_tasks = ["aggregated_model_validation", "train", "locally_tuned_model_validation"]
        self.round_number = 0
        self._end_of_round_check_done = [False] * rounds_to_train

        self.rounds_to_train = rounds_to_train

        # if the collaborator requests a delta, this value is set to true
        self.authorized_cols = authorized_cols
        self.uuid = "uuid"
        self.federation_uuid = "uuid2"
        self.single_col_cert_common_name = ""
        self.quit_job_sent_to = []

        # To prevent race condition in checking round end
        self.end_of_round_check_lock = Lock()

        with open("to_send.yaml") as f:
            data = yaml.safe_load(f)
        self.arrays = {}
        for d in data:
            self.arrays[d["tensor_name"]] = np.random.random(size=d["val_shape"]).astype(d["val_type"])

    def valid_collaborator_cn_and_id(self, cert_common_name, collaborator_common_name):
        return cert_common_name == collaborator_common_name and collaborator_common_name in self.authorized_cols

    def all_quit_jobs_sent(self):
        return set(self.quit_job_sent_to) == set(self.authorized_cols)

    def get_tasks(self, collaborator_name):
        if self.round_number >= self.rounds_to_train:
            self.quit_job_sent_to.append(collaborator_name)
            tasks = None
            sleep_time = 0
            time_to_quit = True
            return tasks, self.round_number, sleep_time, time_to_quit

        time_to_quit = False
        tasks = [task for task in self.all_tasks if task not in self.tasks_done[collaborator_name][self.round_number]]
        if len(tasks) == 0:
            tasks = None
            sleep_time = 10
            return tasks, self.round_number, sleep_time, time_to_quit

        sleep_time = 0
        print(f"Sending tasks to {collaborator_name} round {self.round_number}")
        return tasks, self.round_number, sleep_time, time_to_quit

    def get_aggregated_tensor(self, collaborator_name, tensor_name, round_number, report, tags, require_lossless):
        nparray = self.arrays[tensor_name]

        array_shape = nparray.shape
        compressed_nparray = nparray.tobytes(order="C")

        metadata_protos = [
            aggregator_pb2.MetadataProto(
                int_to_float={},
                int_list=list(array_shape),
                bool_list=[],
            )
        ]

        return aggregator_pb2.NamedTensor(
            name=tensor_name,
            round_number=round_number,
            lossless=True,
            report=report,
            tags=tags,
            transformer_metadata=metadata_protos,
            data_bytes=compressed_nparray,
        )

    def send_local_task_results(self, collaborator_name, round_number, task_name, data_size, named_tensors):
        if self.round_number != round_number:
            print(f"STRAGGLER {collaborator_name} round {self.round_number}")
            return
        if task_name in self.tasks_done[collaborator_name][round_number]:
            print(f"ALREADY {collaborator_name} round {self.round_number}")
            raise ValueError(f"Aggregator already has task results from collaborator {collaborator_name} for task {task_name}")

        for named_tensor in named_tensors:
            array_shape = tuple(named_tensor.transformer_metadata[0].int_list)
            flat_array = np.frombuffer(named_tensor.data_bytes, dtype=np.float32)
            _ = np.reshape(flat_array, newshape=array_shape, order="C")

        self.tasks_done[collaborator_name][round_number].append(task_name)

        for col in self.tasks_done:
            if len(self.tasks_done[col][round_number]) != 3:
                return

        self._end_of_round_check()

    def _end_of_round_check(self):
        print("end of round check called")
        with self.end_of_round_check_lock:
            if self._end_of_round_check_done[self.round_number]:
                return
            self._end_of_round_check_done[self.round_number] = True
            print("ending round")
            self.round_number += 1

            with open("to_send.yaml") as f:
                data = yaml.safe_load(f)

            del self.arrays
            self.arrays = {}
            for d in data:
                self.arrays[d["tensor_name"]] = np.random.random(size=d["val_shape"]).astype(d["val_type"])
