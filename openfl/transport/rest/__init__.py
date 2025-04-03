# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""openfl.transport.grpc package."""

from .aggregator_client import AggregatorRESTClient
from .aggregator_server import AggregatorRESTServer


__all__ = [
    "AggregatorRESTServer",
    "AggregatorRESTClient",
]
