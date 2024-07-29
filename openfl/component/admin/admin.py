# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""Collaborator module."""

from logging import getLogger


class Admin:
    r"""The Admin object class.

    Args:
        admin_name (string): The common name for the admin
        aggregator_uuid: The unique id for the aggregator
        federation_uuid: The unique id for the federation
        client: a gRPC client
    """

    def __init__(
        self, admin_name, aggregator_uuid, federation_uuid, client, **kwargs
    ):
        """Initialize."""
        self.admin_name = admin_name
        self.aggregator_uuid = aggregator_uuid
        self.federation_uuid = federation_uuid
        self.client = client
        self.logger = getLogger(__name__)

    def add_collaborator(self, col_label, col_cn):
        """Send a request to the aggregator to add a collaborator."""
        self.logger.info("Adding collaborator...")
        self.client.admin_add_collaborator(self.admin_name, col_label, col_cn)

    def remove_collaborator(self, col_label, col_cn):
        """Send a request to the aggregator to remove a collaborator."""
        self.logger.info("Removing collaborator...")
        self.client.admin_remove_collaborator(
            self.admin_name, col_label, col_cn
        )

    def get_experiment_status(self):
        """Get experiment status from the aggregator."""
        self.logger.info("Querying the experiment status...")
        status_dict = self.client.admin_get_experiment_status(self.admin_name)
        return status_dict
