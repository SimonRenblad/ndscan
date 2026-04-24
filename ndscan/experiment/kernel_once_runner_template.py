import time
from numpy import int32
from artiq.language import (EnvExperiment, HasEnvironment, kernel, portable, compile, rpc, TerminationRequested)
from artiq.coredevice.exceptions import RTIOUnderflow
from artiq.language.core import Kernel, KernelInvariant
from artiq.coredevice.core import Core

from ndscan.experiment.fragment import (ExpFragment, Fragment, RestartKernelTransitoryError,
                       TransitoryError)
from ndscan.experiment.parameters import ParamBase, ParamStore
from .$fragment_class import Wrapper$fragment_class

@compile
class _InnerFragmentRunner:
    fragment: KernelInvariant[Wrapper$fragment_class]
    core: KernelInvariant[Core]
    max_rtio_underflow_retries: KernelInvariant[int32]
    max_transitory_error_retries: KernelInvariant[int32]
    num_underflows_caught: Kernel[int32]
    num_transitory_errors_caught: Kernel[int32]
    _continue_running: KernelInvariant[bool]
    """Object wrapping fragment execution to be able to execute everything in one kernel
    invocation (no difference for non-kernel fragments).
    """
    
    def __init__(self, runner, fragment, max_rtio_underflow_retries: int,
              max_transitory_error_retries: int,
              skip_on_persistent_transitory_error: bool,
              continue_running: bool = False,
              is_time_series: bool = False
             ):
        self.runner = runner
        self.core = runner.core
        self.fragment = Wrapper$fragment_class(fragment)
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.num_underflows_caught = 0
        self.num_transitory_errors_caught = 0
        self._continue_running = continue_running

    @kernel
    def _run(self) -> bool:
        try:
            while True:
                try:
                    self.fragment.device_setup()
                    self.fragment.run_once()
                    return True
                except RTIOUnderflow:
                    self.num_underflows_caught += 1
                    if self.num_underflows_caught > self.max_rtio_underflow_retries:
                        raise
                except RestartKernelTransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
                    return False
                except TransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
        finally:
            self.fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True

    @kernel
    def run_continuous_kernel(self) -> bool:
        self.core.reset()
        return self._continuous_loop()

    @rpc
    def scheduler_check_pause(self) -> bool:
        return self.runner.scheduler.check_pause()

    # TODO(srenblad): add back print statements for transitory error / underflows
    @portable
    def _continuous_loop(self) -> bool:
        try:
            while not self.scheduler_check_pause():
                try:
                    self.fragment.device_setup()
                    self.fragment.run_once()
                    self._finish_continuous_point()
                    if not self._continue_running:
                        return True

                    # One point is now finished, so reset transitory error counters for
                    # the next one.
                    self.num_transitory_errors_caught = 0
                    self.num_underflows_caught = 0
                except RTIOUnderflow:
                    self.num_underflows_caught += 1
                    if self.num_underflows_caught > self.max_rtio_underflow_retries:
                        raise
                except RestartKernelTransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
                    return False
                except TransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
            return False
        finally:
            self.fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True

    @rpc(flags={"async"})
    def _finish_continuous_point(self):
        f = self.runner
        if f._is_time_series:
            f._timestamp_sink.push(time.monotonic() - f._time_series_start)
        else:
            f._point_phase = not f._point_phase
            f.set_dataset(f.dataset_prefix + "point_phase",
                             f._point_phase,
                             broadcast=True)
