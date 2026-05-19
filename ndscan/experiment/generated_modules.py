from artiq.tools import load_with_loader
from artiq.master.worker_impl import StringLoader

# TODO(srenblad) can probably eliminate imports entirely since execute_generated_module
# will need to be propagated pre-compilation separate from the other machinery

_header_imports = """
from __future__ import annotations
import logging
from itertools import islice
import numpy as np
from numpy import int32, int64
from artiq.coredevice.core import Core
from ndscan.experiment.entry_point import KernelContinuousRunner, KernelOnceRunner
from ndscan.experiment.fragment import Fragment, log_failed_cleanup
from ndscan.experiment.scan_runner import ResultBatcher, KernelScanRunner
from ndscan.experiment.result_channels import ResultChannel, FloatChannel
from ndscan.experiment.default_analysis import DefaultAnalysis, ResultPrefixAnalysisWrapper
from ndscan.experiment import (kernel, rpc, compile, Kernel, KernelInvariant, portable,
                               RTIOUnderflow, print_rpc, RestartKernelTransitoryError,
                               TransitoryError)

"""


_fragment_template = """
from {fragment_module} import {fragment_name}

@compile
class Inner{fragment_name}:
    fragment: KernelInvariant[{fragment_name}]
    {subscan_type}
{subfrags_types}

    def __init__(self, fragment, *args, **kwargs):
        self.fragment = fragment
        if fragment.inner_subscan is not None:
            self.subscan = fragment.inner_subscan
        for s in self.fragment._subfragments:
            setattr(self, s._fragment_path[-1], s.inner_fragment)

    @portable
    def device_setup_subfragments(self):
{device_setup}

    @portable
    def device_cleanup_subfragments(self):
{device_cleanup}

    @portable
    def device_setup(self):
        {setup_fragment}
        self.device_setup_subfragments()

    @portable
    def device_cleanup(self):
        {cleanup_fragment}
        self.device_cleanup_subfragments()

    @portable
    def run_once(self):
        {run_once_behavior}
"""


_runner_template = """
_RUN_CHUNK_PROCEED = 0
_RUN_CHUNK_INTERRUPTED = 1
_RUN_CHUNK_SCAN_COMPLETE = 2

@compile
class {runner_name}:
    _fragment: KernelInvariant[Inner{fragment_class}]
    core: KernelInvariant[Core]
    runner: KernelInvariant[KernelScanRunner]

    def __init__(self, runner, fragment):
        self.runner = runner
        self.core = runner.core
        self.scheduler = runner.scheduler
        self._fragment = fragment.inner_fragment

    @kernel
    def acquire(self, device_cleanup: bool) -> bool:
        self.runner._install_result_batcher()
        try:
            self.runner._last_pause_check_mu = self.core.get_rtio_counter_mu()
            while True:
                result = self._run_chunk()
                if result == _RUN_CHUNK_INTERRUPTED:
                    return False
                if result == _RUN_CHUNK_SCAN_COMPLETE:
                    return True
                assert result == _RUN_CHUNK_PROCEED
        finally:
            self.runner._remove_result_batcher()
            if device_cleanup:
                self._fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True

    @kernel
    def _run_chunk(self) -> int32:
        values = self.runner._get_param_values_chunk()
        stride = values[0]
        if stride == 0:
            return _RUN_CHUNK_SCAN_COMPLETE
        for i in range(stride):
            for j in range(len(self.runner._float_stores)):
                self.runner._float_stores[j].set_from_rpc(values[1][j*stride + i])
            for j in range(len(self.runner._int_stores)):
                self.runner._int_stores[j].set_from_rpc(values[2][j*stride + i])
            for j in range(len(self.runner._bool_stores)):
                self.runner._bool_stores[j].set_from_rpc(values[3][j*stride + i])
            if self._run_point():
                return _RUN_CHUNK_INTERRUPTED
        return _RUN_CHUNK_PROCEED

    @kernel
    def _run_point(self) -> bool:
        num_underflows = 0
        num_transitory_errors = 0
        while True:
            if self.runner._should_pause():
                return True
            try:
                self._fragment.device_setup()
                self._fragment.run_once()
                break
            except RTIOUnderflow:
                if num_underflows >= self.runner.max_rtio_underflow_retries:
                    raise
                num_underflows += 1
                print_rpc("Ignoring RTIOUnderflow")
                self.runner._retry_point()
            except RestartKernelTransitoryError:
                print_rpc("Caught transitory error, restarting kernel")
                self.runner._retry_point()
                return True
            except TransitoryError:
                if num_transitory_errors >= self.runner.max_transitory_error_retries:
                    if self.runner.skip_on_persistent_transitory_error:
                        self.runner._skip_point()
                        return False
                    raise
                num_transitory_errors += 1
                print_rpc("Caught transitory error, retrying")
                self.runner._retry_point()
        self.runner._point_completed()
        return False
"""

_continuous_runner_template = """
@compile
class InnerKernelContinuousRunner:
    core: KernelInvariant[Core]
    fragment: KernelInvariant[Inner{fragment_class}]
    runner: KernelInvariant[KernelContinuousRunner]
    num_underflows_caught: Kernel[int32]
    num_transitory_errors_caught: Kernel[int32]
    _continue_running: KernelInvariant[bool]
    
    def __init__(self, runner, fragment, continue_running):
        self.runner = runner
        self.fragment = fragment
        self.core = runner.core
        self._continue_running = continue_running
        self.num_underflows_caught = 0
        self.num_transitory_errors_caught = 0

    # TODO(srenblad): add back print statements
    @kernel
    def run(self) -> bool:
        self.runner.core.reset()
        try:
            while not self.runner.scheduler_check_pause():
                try:
                    self.fragment.device_setup()
                    self.fragment.run_once()
                    self.runner._finish_continuous_point()
                    if not self._continue_running:
                        return True

                    # One point is now finished, so reset transitory error counters for
                    # the next one.
                    self.num_transitory_errors_caught = 0
                    self.num_underflows_caught = 0
                except RTIOUnderflow:
                    self.num_underflows_caught += 1
                    if self.num_underflows_caught > self.runner.max_rtio_underflow_retries:
                        raise
                except RestartKernelTransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.runner.max_transitory_error_retries):
                        raise
                    return False
                except TransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.runner.max_transitory_error_retries):
                        raise
            return False
        finally:
            self.fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True

"""

_once_runner_template = """
@compile
class InnerKernelOnceRunner:
    fragment: KernelInvariant[Inner{fragment_class}]
    num_underflows_caught: Kernel[int32]
    num_transitory_errors_caught: Kernel[int32]

    def __init__(self, runner, fragment):
        self.runner = runner
        self.fragment = fragment
        self.num_underflows_caught = 0
        self.num_transitory_errors_caught = 0

    # TODO(srenblad): add back print statements
    @kernel
    def run(self) -> bool:
        try:
            while True:
                try:
                    self.fragment.device_setup()
                    self.fragment.run_once()
                    return True
                except RTIOUnderflow:
                    self.num_underflows_caught += 1
                    if self.num_underflows_caught > self.runner.max_rtio_underflow_retries:
                        raise
                except RestartKernelTransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.runner.max_transitory_error_retries):
                        raise
                    return False
                except TransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.runner.max_transitory_error_retries):
                        raise
        finally:
            self.fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True
"""

# should be appended to the OWNER fragment
# TODO(srenblad): ensure this works for nested continuous / run once runners asw?
_subscan_template = """
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

# we use scheduler rid to allow for experiment pipeline
_GENERATED_STORE = {}


def get_module_handler(rid):
    if rid not in _GENERATED_STORE:
        _GENERATED_STORE[rid] = GeneratedModuleHandler(rid)
    return _GENERATED_STORE[rid]


class GeneratedModuleHandler:
    def __init__(self, rid, name="ndscan__generated"):
        self.name = name + f"_{rid}"
        self.rid = rid
        self.backing_string = _header_imports.format()

    def execute_module(self):
        loader = StringLoader(self.name, self.backing_string)
        module = load_with_loader(self.name, loader)
        # executing module, remove from store
        _GENERATED_STORE[self.rid] = None
        return module

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

    def add_once_runner(self, *args, **kwargs):
        r = _once_runner_template.format(*args, **kwargs)
        self.backing_string += "\n\n"
        self.backing_string += r

    def add_continuous_runner(self, *args, **kwargs):
        r = _continuous_runner_template.format(*args, **kwargs)
        self.backing_string += "\n\n"
        self.backing_string += r

    def dump_source_code(self, filename=None):
        if filename is None:
            filename = self.name + ".py"
        with open(filename, "w+") as f:
            f.write(self.backing_string)
