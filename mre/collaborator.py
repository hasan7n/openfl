# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""Collaborator module."""

from time import sleep, time

import aggregator_pb2
from client import AggregatorGRPCClient

import yaml
import numpy as np
import sys


class Collaborator:
    def __init__(self, coln):
        self.collaborator_name = f"col{coln}@example.com"

        self.client = AggregatorGRPCClient(
            "localhost",
            5505,
            True,
            False,
            "certs/ca_cert/root.crt",
            f"certs/col{coln}/crt.crt",
            f"certs/col{coln}/key.key",
            aggregator_uuid="uuid",
            federation_uuid="uuid2",
            single_col_cert_common_name=None,
            for_admin=False,
        )

    def run(self):
        """Run the collaborator."""
        while True:
            tasks, round_number, sleep_time, time_to_quit = self.client.get_tasks(self.collaborator_name)
            if time_to_quit:
                print("quitting")
                break
            elif sleep_time > 0:
                print(f"Sleeping {sleep_time} round {round_number}")
                sleep(sleep_time)  # some sleep function
            else:
                print(f"Received tasks, {tasks} round {round_number}")
                for task in tasks:
                    with open(f"./analysis/start____{round_number}____{self.collaborator_name}____{task}____{time()}", "w"):
                        pass
                    self.do_task(task, round_number)
                    with open(f"./analysis/end____{round_number}____{self.collaborator_name}____{task}____{time()}", "w"):
                        pass

    def do_task(self, task, round_number):
        task = task.name
        # this would return a list of what tensors we require as TensorKeys
        if task == "aggregated_model_validation":
            with open("to_get.yaml") as f:
                data = yaml.safe_load(f)
            names = [d["tensor_name"] for d in data]
            _ = {name: self.get_aggregated_tensor_from_aggregator(name, round_number, require_lossless=True) for name in names}

        if task == "train":
            with open("to_send.yaml") as f:
                data = yaml.safe_load(f)
            global_output_tensor_dict = {}
            for d in data:
                global_output_tensor_dict[d["tensor_name"]] = np.random.random(size=d["val_shape"]).astype(d["val_type"])
        else:
            global_output_tensor_dict = {
                "val_eval": np.random.random(size=[]).astype(np.float64),
                "val_eval_C1": np.random.random(size=[]).astype(np.float64),
                "val_eval_C2": np.random.random(size=[]).astype(np.float64),
                "val_eval_C3": np.random.random(size=[]).astype(np.float64),
                "val_eval_C4": np.random.random(size=[]).astype(np.float64),
            }
        self.send_task_results(global_output_tensor_dict, round_number, task)

    def get_aggregated_tensor_from_aggregator(self, name, round_number, require_lossless=False):
        named_tensor = self.client.get_aggregated_tensor(self.collaborator_name, name, round_number, False, ("h",), require_lossless)
        array_shape = tuple(named_tensor.transformer_metadata[0].int_list)
        flat_array = np.frombuffer(named_tensor.data_bytes, dtype=np.float32)
        return np.reshape(flat_array, newshape=array_shape, order="C")

    def send_task_results(self, tensor_dict, round_number, task_name):
        named_tensors = []
        print(f"converting tensors to named ones, round {round_number}")
        for tensor_name, nparray in tensor_dict.items():
            if nparray.dtype != np.float32:
                nparray = nparray.astype(np.float32)
            array_shape = nparray.shape
            compressed_nparray = nparray.tobytes(order="C")

            metadata_protos = [
                aggregator_pb2.MetadataProto(
                    int_to_float={},
                    int_list=list(array_shape),
                    bool_list=[],
                )
            ]

            nt = aggregator_pb2.NamedTensor(
                name=tensor_name,
                round_number=round_number,
                lossless=True,
                report=False,
                tags=("k",),
                transformer_metadata=metadata_protos,
                data_bytes=compressed_nparray,
            )
            named_tensors.append(nt)

        data_size = 10
        self.client.send_local_task_results(self.collaborator_name, round_number, task_name, data_size, named_tensors)


if __name__ == "__main__":
    n = int(sys.argv[1])
    Collaborator(n).run()
