from artiq.tools import load_with_loader
from artiq.master.worker_impl import StringLoader


_header_imports = """
from __future__ import annotations
import logging
from itertools import islice
import numpy as np
from numpy import int32, int64
from artiq.coredevice.core import Core
from ndscan.experiment.fragment import Fragment
from ndscan.experiment.scan_runner import ResultBatcher
from ndscan.experiment.parameters import FloatParamStore, IntParamStore, BoolParamStore
from ndscan.experiment.result_channels import ResultChannel, FloatChannel
from ndscan.experiment.default_analysis import DefaultAnalysis, ResultPrefixAnalysisWrapper
from ndscan.experiment import (kernel, rpc, compile, Kernel, KernelInvariant, portable,
                               RTIOUnderflow, print_rpc, RestartKernelTransitoryError,
                               TransitoryError)

logger = logging.getLogger(__name__)

@rpc(flags={{"async"}})
def log_failed_cleanup(path: str):
    logger.error(f"device_cleanup() failed for '{{path}}'.")
"""


_fragment_template = """
from {fragment_module} import {fragment_name}
{subscan_import}
{subfrags_imports}

@compile
class Inner{fragment_name}:
    fragment: KernelInvariant[{fragment_name}]
    {subscan_type}
{subfrags_types}

    def __init__(self, fragment, *args, **kwargs):
        self.fragment = fragment
        for s in self.fragment._subfragments:
            setattr(self, s._fragment_path[-1], s.inner_fragment)

    def init_subscan(self, subscan):
        self.subscan = subscan

    @portable
    def device_setup_subfragments(self):
{device_setup}

    @portable
    def device_cleanup_subfragments(self):
{device_cleanup}

    @portable
    def device_setup(self):
        self.fragment.device_setup()
        self.device_setup_subfragments()

    @portable
    def device_cleanup(self):
        self.fragment.device_cleanup()
        self.device_cleanup_subfragments()

    @portable
    def run_once(self):
        {run_once_behavior}
"""


_runner_template = """
{fragment_import}

_RUN_CHUNK_PROCEED = 0
_RUN_CHUNK_INTERRUPTED = 1
_RUN_CHUNK_SCAN_COMPLETE = 2

@compile
class {runner_name}:
    _fragment: KernelInvariant[Inner{fragment_class}]
    _pause_check_interval_mu: Kernel[int64]
    _last_pause_check_mu: Kernel[int64]
    max_rtio_underflow_retries: KernelInvariant[int32]
    max_transitory_error_retries: KernelInvariant[int32]
    skip_on_persistent_transitory_error: KernelInvariant[bool]
    core: KernelInvariant[Core]
{param_store_types}

    def __init__(self, runner, fragment, axes, axis_sinks, max_rtio_underflow_retries,
                 max_transitory_error_retries,
                 skip_on_persistent_transitory_error):
        self.core = runner.core
        self.scheduler = runner.scheduler
        self._fragment = fragment.inner_fragment
        self._axes = axes
        self._axis_sinks = axis_sinks

        self._pause_check_interval_mu = self.core.seconds_to_mu(0.2)
        self._last_pause_check_mu = int64(0)
        self.max_rtio_underflow_retries = int32(max_rtio_underflow_retries)
        self.max_transitory_error_retries = int32(max_transitory_error_retries)
        self.skip_on_persistent_transitory_error = bool(skip_on_persistent_transitory_error)

        for i, axis in enumerate(axes):
            setattr(self, "_param_store_{{}}".format(i), axis.param_store)

        self._result_batcher = None

    def set_points(self, points):
        self._points = points
        self._current_chunk = []
        self._update_host_param_stores()

{run_chunk}

    @rpc(flags={{"async"}})
    def _install_result_batcher(self):
        self._result_batcher = ResultBatcher(self._fragment.fragment)
        self._result_batcher.install()

    @rpc(flags={{"async"}})
    def _remove_result_batcher(self):
        self._result_batcher.remove()
        self._result_batcher = None

    @rpc
    def scheduler_check_pause(self) -> bool:
        return self.scheduler.check_pause()

    @kernel
    def acquire(self) -> bool:
        self._install_result_batcher()
        try:
            self._last_pause_check_mu = self.core.get_rtio_counter_mu()
            while True:
                result = self._run_chunk()
                if result == _RUN_CHUNK_INTERRUPTED:
                    return False
                if result == _RUN_CHUNK_SCAN_COMPLETE:
                    return True
                assert result == _RUN_CHUNK_PROCEED
        finally:
            self._remove_result_batcher()
            self._fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True

    @kernel
    def _run_point(self) -> bool:
        num_underflows = 0
        num_transitory_errors = 0
        while True:
            if self._should_pause():
                return True
            try:
                self._fragment.device_setup()
                self._fragment.run_once()
                break
            except RTIOUnderflow:
                if num_underflows >= self.max_rtio_underflow_retries:
                    raise
                num_underflows += 1
                print_rpc("Ignoring RTIOUnderflow")
                self._retry_point()
            except RestartKernelTransitoryError:
                print_rpc("Caught transitory error, restarting kernel")
                self._retry_point()
                return True
            except TransitoryError:
                if num_transitory_errors >= self.max_transitory_error_retries:
                    if self.skip_on_persistent_transitory_error:
                        self._skip_point()
                        return False
                    raise
                num_transitory_errors += 1
                print_rpc("Caught transitory error, retrying")
                self._retry_point()
        self._point_completed()
        return False

    @kernel
    def _should_pause(self) -> bool:
        current_time_mu = self.core.get_rtio_counter_mu()
        if (current_time_mu - self._last_pause_check_mu >
                self._pause_check_interval_mu):
            self._last_pause_check_mu = current_time_mu
            if self.scheduler_check_pause():
                return True
        return False

    @rpc
    def _get_param_values_chunk(self) -> {param_values_return_value}:
        # Number of scan points to send at once. After each chunk, the kernel needs to
        # execute a blocking RPC to fetch new points, so this should be chosen such
        # that latency/constant overhead and throughput are balanced. 10 is an arbitrary
        # choice based on the observation that even for fast experiments, 10 points take
        # a good fraction of a second, while it is still low enough not to run into any
        # memory management issues on the kernel.
        CHUNK_SIZE = 10

        self._current_chunk.extend(
            islice(self._points, CHUNK_SIZE - len(self._current_chunk)))

        values = tuple([] for _ in self._axes)
        for p in self._current_chunk:
            for i, (value, axis) in enumerate(zip(p, self._axes)):
                # KLUDGE: Explicitly coerce value to the target type here so we can use
                # the regular (float) scans for integers until proper support for int
                # scans is implemented.
                values[i].append(
                    axis.param_store.to_rpc_type(
                            axis.param_store.value_from_pyon(value)))
        return values

    @rpc(flags={{"async"}})
    def _retry_point(self):
        self._result_batcher.discard_current()

    @rpc(flags={{"async"}})
    def _skip_point(self):
        self._result_batcher.discard_current()
        values = self._current_chunk.pop(0)
        logger.error("Skipping point: %s", values)
        self._update_host_param_stores()

    @rpc(flags={{"async"}})
    def _point_completed(self):
        # This might raise an exception, which will only bubble up to the user during
        # the next synchronous RPC request. As this only occurs when the user code
        # contains a logic error (failure to call push() on a result channel), this
        # should be acceptable, however.
        self._result_batcher.ensure_complete_and_push()

        # Now that we know that a complete point was successfully produced, also record
        # the axis coordinates.
        values = self._current_chunk.pop(0)
        for value, sink in zip(values, self._axis_sinks):
            sink.push(value)

        # Prepare for the next point.
        self._update_host_param_stores()

    def _update_host_param_stores(self):
        if self._is_out_of_points():
            return
        # Set the host-side parameter stores.
        next_values = self._current_chunk[0]
        for value, axis in zip(next_values, self._axes):
            axis.param_store.set_value(axis.param_store.value_from_pyon(value))

    def _is_out_of_points(self):
        if self._current_chunk:
            return False
        # Current chunk is empty, but we might be at a chunk boundary.
        self._get_param_values_chunk()
        return not self._current_chunk
"""

_runner_noscan_template = """
{fragment_import}

@compile
class InnerNoScanRunner:
    fragment: KernelInvariant[Inner{fragment_class}]
    core: KernelInvariant[Core]
    max_rtio_underflow_retries: KernelInvariant[int32]
    max_transitory_error_retries: KernelInvariant[int32]
    num_underflows_caught: Kernel[int32]
    num_transitory_errors_caught: Kernel[int32]
    _continue_running: KernelInvariant[bool]
    
    def __init__(self, runner, fragment, max_rtio_underflow_retries: int,
              max_transitory_error_retries: int,
              skip_on_persistent_transitory_error: bool,
              continue_running: bool = False,
              is_time_series: bool = False
             ):
        self.runner = runner
        self.core = runner.core
        self.fragment = Inner{fragment_class}(fragment)
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.num_underflows_caught = 0
        self.num_transitory_errors_caught = 0
        self._continue_running = continue_running

    # TODO(srenblad): add back print statements
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

    @rpc(flags={{"async"}})
    def _finish_continuous_point(self):
        f = self.runner
        if f._is_time_series:
            f._timestamp_sink.push(time.monotonic() - f._time_series_start)
        else:
            f._point_phase = not f._point_phase
            f.set_dataset(f.dataset_prefix + "point_phase",
                             f._point_phase,
                             broadcast=True)
"""


# should be appended to the OWNER fragment
# TODO(srenblad): make a separate module asw
_subscan_template = """
{runner_import}

@compile
class {subscan_name}:
    runner: KernelInvariant[{runner_name}]

    def __init__(self, owner, runner):
        self.owner = owner
        self.runner = runner

    @kernel
    def acquire(self):
        if not self.runner.acquire():
            raise RestartKernelTransitoryError("Subscan interrupted by pause request")
        self._finalize()

    @rpc(flags={{"async"}})
    def _finalize(self):
        self.owner._push_results()
        self.owner._regenerate_points()
"""


class GeneratedModuleHandler:
    def __init__(self, name="ndscan__generated"):
        self.backing_string = _header_imports.format()
        self.module = None
        self.name = name

    def execute_module(self):
        loader = StringLoader(self.name, self.backing_string)
        self.module = load_with_loader(self.name, loader)
        return self.module

    def import_module(self):
        return self.module

    def add_fragment(self, *args, **kwargs):
        f = _fragment_template.format(*args, **kwargs)
        self.backing_string += "\n\n"
        self.backing_string += f

    def add_runner(self, *args, **kwargs):
        r = _runner_template.format(*args, **kwargs)
        self.backing_string += "\n\n"
        self.backing_string += r

    def add_subscan(self, *args, **kwargs):
        s = _subscan_template.format(*args, **kwargs)
        self.backing_string += "\n\n"
        self.backing_string += s

    def add_noscan_runner(self, *args, **kwargs):
        r = _runner_noscan_template.format(*args, **kwargs)
        self.backing_string += "\n\n"
        self.backing_string += r

    def dump_source_code(self, filename):
        with open(filename, "w+") as f:
            f.write(self.backing_string)
