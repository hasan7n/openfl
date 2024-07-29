# Copyright (C) 2020-2023 Intel Corporation
# SPDX-License-Identifier: Apache-2.0

"""Cutoff time based Straggler Handling function."""
import numpy as np
import time
import threading
from typing import Callable
from logging import getLogger

from openfl.component.straggler_handling_functions import StragglerHandlingPolicy


class CutoffTimeBasedStragglerHandling(StragglerHandlingPolicy):
    MINIMUM_CUTOFF_SECONDS=20 # TODO: This helps for when we decouple minimum from timeout?? Maybe

    def __init__(
        self,
        round_start_time=None,
        straggler_cutoff_time=np.inf,
        minimum_reporting=1,
        **kwargs
    ):
        if minimum_reporting <= 0:
            raise ValueError(f"minimum_reporting cannot be {minimum_reporting}")

        self.round_start_time = round_start_time
        self._set_cutoff_time(straggler_cutoff_time)
        self.minimum_reporting = minimum_reporting
        self.logger = getLogger(__name__)

        if self.straggler_cutoff_time == np.inf:
            self.logger.info(
                "CutoffTimeBasedStragglerHandling is disabled as straggler_cutoff_time "
                "is set to np.inf."
            )

    def start_policy(self, callback: Callable) -> None:
        """
        Start time-based straggler handling policy for collaborator for
        a particular round.

        Args:
            callback: Callable
                Callback function for when straggler_cutoff_time elapses

        Returns:
            None
        """
        # If straggler_cutoff_time is set to infinite or
        # if the timer already expired for the current round do not start
        # the timer again until next round.
        if self.straggler_cutoff_time == np.inf:
            return
        self.reset_policy_for_round()
        self.round_start_time = time.time()
        self.timer = threading.Timer(
            self.straggler_cutoff_time, callback,
        )
        self.timer.daemon = True
        self.timer.start()
        # save the callback in case we need to directly call it due to setting new cutoff value
        self.callback = callback


    def _set_cutoff_time(self, straggler_cutoff_time):
        self.straggler_cutoff_time = max(straggler_cutoff_time, CutoffTimeBasedStragglerHandling.MINIMUM_CUTOFF_SECONDS)
        

    def set_straggler_cutoff_time(self, straggler_cutoff_time):
        # cancel current timer
        self.reset_policy_for_round()
        
        # set the new value
        self._set_cutoff_time(straggler_cutoff_time)

        # if new time has expired, run callback
        if self.__straggler_time_expired():
            self.callback()
        # otherwise, set the new timer
        else:
            self.timer = threading.Timer(
                self.straggler_cutoff_time - (time.time() - self.round_start_time), self.callback,
            )
            self.timer.daemon = True
            self.timer.start()


    def reset_policy_for_round(self) -> None:
        """
        Reset policy variable for the next round.

        Args:
            None

        Returns:
            None
        """
        if hasattr(self, "timer"):
            self.timer.cancel()
            delattr(self, "timer")

    # MICAH TODO: This interface needs to be made more general
    def straggler_cutoff_check(
        self, num_collaborators_done: int, num_all_collaborators: int,
    ) -> bool:
        """
        If minimum_reporting collaborators have reported results within
        straggler_cutoff_time then return True, otherwise False.

        Args:
            num_collaborators_done: int
                Number of collaborators finished.
            num_all_collaborators: int
                Total number of collaborators.

        Returns:
            bool
        """
        # Check if time has expired
        if not self.__straggler_time_expired():
            return False
        else:
            self.logger.info(
                f"{num_collaborators_done} collaborators reported results within "
                "cutoff time."
            )
            # Check if minimum_reporting collaborators have reported results
            if self.__minimum_collaborators_reported(num_collaborators_done):
                self.logger.info(f"Minimum of {self.minimum_reporting} met. Round should end.")
                return True
            else:
                self.logger.info(f"Minimum of {self.minimum_reporting} NOT met. Round should NOT end.")
                return False

    def __straggler_time_expired(self) -> bool:
        """
        Determines if straggler_cutoff_time is elapsed.
        """
        return (
            self.round_start_time is not None
            and ((time.time() - self.round_start_time) > self.straggler_cutoff_time)
        )

    def __minimum_collaborators_reported(self, num_collaborators_done) -> bool:
        """
        If minimum required collaborators have reported results, then return True
        otherwise False.
        """
        return num_collaborators_done >= self.minimum_reporting
