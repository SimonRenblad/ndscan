from ndscan.experiment import kernel, rpc, compile, Kernel, KernelInvariant, portable, RTIOUnderflow, print_rpc, RestartKernelTransitoryError, TransitoryError
from artiq.coredevice.core import Core
import numpy as np
from numpy import int32, int64
from ndscan.experiment.scan_runner import ResultBatcher
from ndscan.experiment.parameters import FloatParamStore, IntParamStore, BoolParamStore
from itertools import islice
from .$fragment_class import Wrapper$fragment_class


_RUN_CHUNK_PROCEED = 0
_RUN_CHUNK_INTERRUPTED = 1
_RUN_CHUNK_SCAN_COMPLETE = 2

@compile
class InternalKernelScanRunner:
    _fragment: Kernel[Wrapper$fragment_class]
    _pause_check_interval_mu: Kernel[int64]
    _last_pause_check_mu: Kernel[int64]
    max_rtio_underflow_retries: KernelInvariant[int32]
    max_transitory_error_retries: KernelInvariant[int32]
    skip_on_persistent_transitory_error: KernelInvariant[bool]
    core: KernelInvariant[Core]
$param_store_types

    def __init__(self, fragment, axes, axis_sinks, core,
                 scheduler,
                 max_rtio_underflow_retries,
                 max_transitory_error_retries,
                 skip_on_persistent_transitory_error):
        self.core = core
        self.scheduler = scheduler
        self._fragment = Wrapper$fragment_class(fragment)
        self._axes = axes
        self._axis_sinks = axis_sinks

        self._pause_check_interval_mu = self.core.seconds_to_mu(0.2)
        self._last_pause_check_mu = int64(0)
        self.max_rtio_underflow_retries = int32(max_rtio_underflow_retries)
        self.max_transitory_error_retries = int32(max_transitory_error_retries)
        self.skip_on_persistent_transitory_error = bool(skip_on_persistent_transitory_error)

        for i, axis in enumerate(axes):
            setattr(self, f"_param_store_{i}", axis.param_store)

        self._result_batcher = None

    def set_points(self, points):
        self._points = points
        self._current_chunk = []
        self._update_host_param_stores()

$run_chunk

    @rpc(flags={"async"})
    def _install_result_batcher(self):
        self._result_batcher = ResultBatcher(self._fragment)
        self._result_batcher.install()

    @rpc(flags={"async"})
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
        """Execute the fragment for a single point (with the currently set parameters).

        :return: Whether the kernel should be exited/experiment should be paused before
            continuing (``True`` to pause, ``False`` to continue immediately).
        """
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
    def _get_param_values_chunk(self) -> $param_values_return_value:
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

    @rpc(flags={"async"})
    def _retry_point(self):
        self._result_batcher.discard_current()

    @rpc(flags={"async"})
    def _skip_point(self):
        self._result_batcher.discard_current()
        values = self._current_chunk.pop(0)
        logger.error("Skipping point: %s", values)
        self._update_host_param_stores()

    @rpc(flags={"async"})
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
        """Set host-side parameter stores for the scan axes to their current values,
        i.e. as specified by the next point in the current scan chunk.

        This ensures that if a parameter is scanned from a kernel scan that requires
        a host RPC to update (e.g. a non-@kernel device_setup()), the RPC'd code will
        execute using the expected values.
        """
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
