# Copyright (C) 2020-2023 Intel Corporation # TODO: header??
# SPDX-License-Identifier: Apache-2.0
"""Admin module."""

import sys
import yaml
from logging import getLogger

from click import echo
from click import group
from click import option
from click import pass_context
from click import Path as ClickPath

from openfl.utilities.path_check import is_directory_traversal

logger = getLogger(__name__)


@group()
@pass_context
def admin(context):
    """Manage Federated Learning Admin."""
    context.obj["group"] = "admin"


@admin.command(name="get_experiment_status")
@option(
    "-p",
    "--plan",
    required=False,
    help="Federated learning plan [plan/plan.yaml]",
    default="plan/plan.yaml",
    type=ClickPath(exists=True),
)
@option(
    "-n",
    "--admin_name",
    required=True,
    help="The certified common name of the admin",
)
@option(
    "-o",
    "--output_file",
    required=False,
    default=None,
    help="File path for saving the status. Defaults to STDOUT.",
)
def get_experiment_status(plan, admin_name, output_file):
    """Get status of an experiment"""
    from pathlib import Path

    from openfl.federated import Plan

    if is_directory_traversal(plan):
        echo(
            "Federated learning plan path is out of the openfl workspace scope."
        )
        sys.exit(1)

    plan = Plan.parse(plan_config_path=Path(plan).absolute())
    status = plan.get_admin(admin_name).get_experiment_status()
    if output_file:
        with open(output_file, "w") as f:
            yaml.safe_dump(status, f)
    else:
        echo(yaml.safe_dump(status))


@admin.command(name="add_collaborator")
@option(
    "-p",
    "--plan",
    required=False,
    help="Federated learning plan [plan/plan.yaml]",
    default="plan/plan.yaml",
    type=ClickPath(exists=True),
)
@option(
    "-n",
    "--admin_name",
    required=True,
    help="The certified common name of the admin",
)
@option(
    "--col_label",
    required=True,
    help="The collaborator label",
)
@option(
    "--col_cn",
    required=True,
    help="The certified common name of the collaborator",
)
def add_collaborator(plan, admin_name, col_label, col_cn):
    """Add a collaborator to the federation."""
    from pathlib import Path

    from openfl.federated import Plan

    if is_directory_traversal(plan):
        echo(
            "Federated learning plan path is out of the openfl workspace scope."
        )
        sys.exit(1)

    plan = Plan.parse(plan_config_path=Path(plan).absolute())
    plan.get_admin(admin_name).add_collaborator(col_label, col_cn)


@admin.command(name="remove_collaborator")
@option(
    "-p",
    "--plan",
    required=False,
    help="Federated learning plan [plan/plan.yaml]",
    default="plan/plan.yaml",
    type=ClickPath(exists=True),
)
@option(
    "-n",
    "--admin_name",
    required=True,
    help="The certified common name of the admin",
)
@option(
    "--col_label",
    required=True,
    help="The collaborator label",
)
@option(
    "--col_cn",
    required=True,
    help="The certified common name of the collaborator",
)
def remove_collaborator(plan, admin_name, col_label, col_cn):
    """Remove a collaborator from the federation."""
    from pathlib import Path

    from openfl.federated import Plan

    if is_directory_traversal(plan):
        echo(
            "Federated learning plan path is out of the openfl workspace scope."
        )
        sys.exit(1)

    plan = Plan.parse(plan_config_path=Path(plan).absolute())
    plan.get_admin(admin_name).remove_collaborator(col_label, col_cn)


@admin.command(name="set_straggler_cutoff_time")
@option(
    "-p",
    "--plan",
    required=False,
    help="Federated learning plan [plan/plan.yaml]",
    default="plan/plan.yaml",
    type=ClickPath(exists=True),
)
@option(
    "-n",
    "--admin_name",
    required=True,
    help="The certified common name of the admin",
)
@option(
    "--timeout_in_seconds",
    required=True,
    type=int,
    help="The number of seconds to set the new straggler cutoff timeout to.",
)
def set_straggler_cutoff_time(plan, admin_name, timeout_in_seconds):
    """Set the cutoff timeout in the straggler handler to a new value (if supported)."""
    from pathlib import Path

    from openfl.federated import Plan

    if is_directory_traversal(plan):
        echo(
            "Federated learning plan path is out of the openfl workspace scope."
        )
        sys.exit(1)

    plan = Plan.parse(plan_config_path=Path(plan).absolute())
    plan.get_admin(admin_name).set_straggler_cutoff_time(timeout_in_seconds)
