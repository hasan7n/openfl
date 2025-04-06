# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

max_metadata_size = 32 * 2**20
max_message_length = 2**30

channel_options = [
    ("grpc.max_metadata_size", max_metadata_size),
    ("grpc.max_send_message_length", max_message_length),
    ("grpc.max_receive_message_length", max_message_length),
    ("grpc.keepalive_time_ms", 20000),
    ("grpc.keepalive_timeout_ms", 60000),
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.max_connection_idle_ms", 20000),
    ("grpc.http2.max_ping_strikes", 0),
]
