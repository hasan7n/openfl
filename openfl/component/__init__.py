# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""openfl.component package."""

from .aggregator import Aggregator
from .assigner import Assigner
from .assigner import RandomGroupedAssigner
from .assigner import StaticGroupedAssigner
from .assigner import DynamicRandomGroupedAssigner
from .collaborator import Collaborator
from .straggler_handling_functions import StragglerHandlingPolicy
from .straggler_handling_functions import CutoffTimeBasedStragglerHandling
from .straggler_handling_functions import PercentageBasedStragglerHandling
from .admin import Admin

__all__ = [
    'Assigner',
    'RandomGroupedAssigner',
    'StaticGroupedAssigner',
    'DynamicRandomGroupedAssigner',
    'Aggregator',
    'Collaborator',
    'StragglerHandlingPolicy',
    'CutoffTimeBasedStragglerHandling',
    'PercentageBasedStragglerHandling',
    'Admin'
]
