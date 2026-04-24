"""Generic scanning loop.

While :mod:`.scan_generator` describes a scan to be run in the abstract, this module
contains the implementation to actually execute one within an ARTIQ experiment. This
will likely be used by end users via
:class:`~ndscan.experiment.entry_point.FragmentScanExperiment` or subscans.
"""

import logging
import numpy as np
from artiq.coredevice.exceptions import RTIOUnderflow
from artiq.language import HasEnvironment, kernel, rpc
from artiq.tools import load_with_loader
from artiq.master.worker_impl import StringLoader
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import islice
from typing import Any
from string import Template
import os
import textwrap
from .default_analysis import AnnotationContext, DefaultAnalysis
from .fragment import ExpFragment, TransitoryError, RestartKernelTransitoryError
from .parameters import ParamStore
from .result_channels import ResultChannel, ResultSink, SingleUseSink
from .scan_generator import generate_points, ScanGenerator, ScanOptions
from .utils import is_kernel

__all__ = [
    "ScanAxis", "ScanSpec", "ScanRunner", "select_runner_class",
    "match_default_analysis", "filter_default_analyses", "describe_scan",
    "describe_analyses"
]

logger = logging.getLogger(__name__)


@dataclass
class ScanAxis:
    """Describes a single axis that is being scanned.

    Apart from the metadata, this also includes the necessary information to execute the
    scan at runtime; i.e. the :class:`.ParamStore` to modify in order to set the
    parameter.
    """
    param_schema: dict[str, Any]
    path: str
    param_store: ParamStore


@dataclass
class ScanSpec:
    """Describes a single scan."""

    #: The list of parameters that are scanned.
    axes: list[ScanAxis]

    #: Generators that give the points for each of the specified axes.
    generators: list[ScanGenerator]

    #: Applicable :class:`.ScanOptions`.
    options: ScanOptions


class ScanRunner(HasEnvironment):
    """Runs the actual loop that executes an :class:`.ExpFragment` for a specified list
    of scan axes (on either the host or core device, as appropriate).
    """
    def build(self,
              max_rtio_underflow_retries: int = 3,
              max_transitory_error_retries: int = 10,
              skip_on_persistent_transitory_error: bool = False):
        """
        :param max_rtio_underflow_retries: Number of RTIOUnderflows to tolerate per scan
            point (by simply trying again) before giving up. Three is a pretty arbitrary
            default – we don't want to block forever in case the experiment is faulty,
            but also want to tolerate ~1% underflow chance for experiments where tight
            timing is critical.
        :param max_transitory_error_retries: Number of transitory errors to tolerate per
            scan point (by simply trying again) before giving up.
        :param skip_on_persistent_transitory_error: By default, transitory errors above
            the configured limit are raised for the calling code to handle (possibly
            terminating the experiment). If ``True``, points with too many transitory
            errors will be skipped instead after logging an error. Consequences for
            overall system robustness should be considered before using this in
            automated code.
        """
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.skip_on_persistent_transitory_error = skip_on_persistent_transitory_error
        self.setattr_device("core")
        self.setattr_device("scheduler")

    def run(self, fragment: ExpFragment, spec: ScanSpec,
            axis_sinks: list[ResultSink]) -> None:
        """Run a scan of the given fragment, with axes as specified.

        Integrates with the ARTIQ scheduler to pause/terminate execution as requested.

        :param fragment: The fragment to iterate.
        :param options: The options for the scan generator.
        :param axis_sinks: A list of :class:`.ResultSink` instances to push the
            coordinates for each scan point to, matching ``scan.axes``.
        """
        # TODO: Support parameters which require host_setup() when changed.
        self.setup(fragment, spec.axes, axis_sinks)
        self.set_points(generate_points(spec.generators, spec.options))
        while True:
            # After every pause(), pull in dataset changes (immediately as well to catch
            # changes between the time the experiment is prepared and when it is run, to
            # keep the semantics uniform).
            fragment.recompute_param_defaults()
            try:
                # FIXME: Need to handle transitory errors here.
                fragment.host_setup()

                # For on-core-device scans, we'll spawn a kernel here.
                # TODO: this is where the generated kernel will have to start
                # everything must be ready for here.
                if self.acquire():
                    return
            finally:
                fragment.host_cleanup()
                # For host-only scans, self.core might be artiq.sim.devices.Core or
                # similar without a close() method.
                if hasattr(self.core, "close"):
                    self.core.close()
            self.scheduler.pause()

    def setup(self, fragment: ExpFragment, axes: list[ScanAxis],
              axis_sinks: list[ResultSink]) -> None:
        raise NotImplementedError

    def set_points(self, points: Iterator[tuple]) -> None:
        raise NotImplementedError

    def acquire(self) -> bool:
        """
        :return: ``true`` if scan is complete, ``false`` if the scan has been
            interrupted and ``acquire()`` should be called again to complete it.
        """
        raise NotImplementedError


class ResultBatcher:
    """Intercepts all result channel sinks of the given fragment, making sure that every
    channel has seen exactly one ``push()`` before forwarding the results to whatever
    sinks might have been set originally in one batch.

    This makes sure that buggy ``ExpFragment`` implementations that do not always push
    a result, or points that failed halfway through, do not lead to "desynchronised"
    datasets/… (where the indices in the struct-of-arrays construction no longer match
    up).
    """
    def __init__(self, fragment: ExpFragment) -> None:
        self._fragment = fragment
        self._orig_sinks = dict[ResultChannel, ResultSink]()

    def install(self) -> None:
        """Start intercepting results."""
        channels = dict[str, ResultChannel]()
        self._fragment._collect_result_channels(channels)
        for channel in channels.values():
            if channel.sink is None:
                continue
            self._orig_sinks[channel] = channel.sink
            channel.sink = SingleUseSink()

    def discard_current(self) -> None:
        """Discard any results that may have been pushed already (e.g. if a point was
        interrupted.)
        """
        for channel in self._orig_sinks.keys():
            if channel.sink.is_set():
                # This is normal, e.g. when a transitory error interrupts a point.
                logger.debug("Discarding result for '%s'", channel)
            channel.sink.reset()

    def ensure_complete_and_push(self) -> None:
        """Make sure each result channel has been pushed to (failing if not), and then
        forward the results to the original sinks.
        """
        # First check whether we have all the values.
        for channel in self._orig_sinks.keys():
            if not channel.sink.is_set():
                raise ValueError(f"Missing value for result channel '{channel}' " +
                                 "(push() not called for current point)")
        # Only then forward them.
        for channel, orig_sink in self._orig_sinks.items():
            orig_sink.push(channel.sink.get())
            channel.sink.reset()

    def remove(self) -> None:
        """Stop intercepting results, restoring the original sinks."""
        self.discard_current()

        # Restore direct access to original sinks for future use.
        for channel, original_sink in self._orig_sinks.items():
            channel.set_sink(original_sink)
        self._orig_sinks.clear()

    def __enter__(self) -> "ResultBatcher":
        self.install()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.remove()


class HostScanRunner(ScanRunner):
    def setup(self, fragment: ExpFragment, axes: list[ScanAxis],
              axis_sinks: list[ResultSink]) -> None:
        self._fragment = fragment
        self._axes = axes
        self._axis_sinks = axis_sinks

    def set_points(self, points: Iterator[tuple]) -> None:
        self._points = points

    def acquire(self) -> bool:
        with ResultBatcher(self._fragment) as result_batcher:
            try:
                # FIXME: Need to handle transitory errors here (or possibly, would be
                # enough to do so in ScanRunner.run(), which we want anyway for
                # host_setup(), etc.).
                while True:
                    axis_values = next(self._points, None)
                    if axis_values is None:
                        return True
                    for (axis, value) in zip(self._axes, axis_values):
                        axis.param_store.set_value(value)
                    self._fragment.device_setup()
                    self._fragment.run_once()

                    result_batcher.ensure_complete_and_push()
                    for (sink, value) in zip(self._axis_sinks, axis_values):
                        # Now that we know self._fragment successfully produced a
                        # complete point, also record the axis coordinates.
                        sink.push(value)

                    if self.scheduler.check_pause():
                        return False
            finally:
                self._fragment.device_cleanup()


# this relies on the existence of a kernel_runner_internal which is templated due to the lack of metaprogramming and generics
class KernelScanRunner(ScanRunner):
    # Note: ARTIQ Python is currently severely limited in its support for generics or
    # metaprogramming. While the interface for this class is effortlessly generic, the
    # implementation might well be a long-forgotten ritual for invoking Cthulhu.

    def setup(self, fragment: ExpFragment, axes: list[ScanAxis],
              axis_sinks: list[ResultSink]) -> None:
        self._fragment = fragment
        fragment_class = self._fragment.__class__.__name__
        fragment_module = fragment.__class__.__module__
        # template
        file_dir = os.path.dirname(__file__)
        with open(os.path.join(file_dir, "kernel_runner_template.py"), "r") as f:
            s = f.read()
        self._internal_runner_template = Template(s)

        # Set up members to be accessed from the kernel through the
        # _get_param_values_chunk RPC call later.
        self._axes = axes
        self._axis_sinks = axis_sinks

        # Interval between scheduler.check_pause() calls on the core device (or rather,
        # the minimum interval; calls are only made after a point has been completed).
        self._pause_check_interval_mu = self.core.seconds_to_mu(0.2)
        self._last_pause_check_mu = np.int64(0)

        param_values_return_value = "tuple["
        param_values_return_value += ", ".join(["list[" + a.param_store.RpcType.__name__ + "]" for a in axes])
        param_values_return_value += "]"

        # first all param setter functions
        param_store_types = ""
        for i, axis in enumerate(axes):
            param_store_types += f"_param_store_{i}: Kernel[{axis.param_store.__class__.__name__}]\n"

        param_store_types = textwrap.indent(param_store_types, "    ")
    
        # then the run_chunk function
        param_decl = " ".join(f"p{idx}," for idx in range(len(axes)))
        run_chunk = "@portable\n"
        run_chunk += "def _run_chunk(self) -> int32:\n"
        run_chunk += "    " + f"({param_decl}) = self._get_param_values_chunk()\n"
        run_chunk += "    if len(p0) == 0:\n"  # No more points
        run_chunk += "        return 2\n"
        run_chunk += "    for i in range(len(p0)):\n"
        for idx in range(len(axes)):
            run_chunk += "        self._param_store_{0}.set_from_rpc(p{0}[i])\n".format(idx)
        run_chunk += "        if self._run_point():\n"
        run_chunk += "            return 1\n"
        run_chunk += "    return 0"
        
        self._internal_runner_string = self._internal_runner_template.substitute(
            run_chunk=textwrap.indent(run_chunk, "    "),
            param_values_return_value=param_values_return_value,
            param_store_types=param_store_types,
            fragment_class=fragment_class,
            fragment_module=fragment_module
        )
        with open(os.path.join(file_dir, "generated/runner.py"), "w+") as f:
            f.write(self._internal_runner_string)
        # # TODO the actual runner (for now we will synthezise a string and examine it for issues)
        # module = load_with_loader(StringLoader("<synthesized>", self._internal_runner_string))
        from .generated import runner
        self._internal_runner = runner.InternalKernelScanRunner(
            self,
            self._fragment,
            self._axes,
            self._axis_sinks,
            self.max_rtio_underflow_retries,
            self.max_transitory_error_retries,
            self.skip_on_persistent_transitory_error,
        )
        # # we might have to do this in some other function tbh
        # self._inner_fragment = fragment_module.InternalFragment()
        # then pass it along innit
        # self._internal_runner = module.InternalKernelScanRunner(
        #   self._fragment,
        #   self._axes,
        #   self._axis_sinks
        # )

    def set_points(self, points):
        self._internal_runner.set_points(points)

    def acquire(self) -> bool:
        return self._internal_runner.acquire()


def select_runner_class(fragment: ExpFragment) -> type[ScanRunner]:
    if is_kernel(fragment.run_once):
        return KernelScanRunner
    else:
        return HostScanRunner


def match_default_analysis(analysis: DefaultAnalysis, axes: Iterable[ScanAxis]) -> bool:
    """Return whether the given default analysis can be executed for the given scan
    axes.

    The implementation is currently a bit more convoluted than necessary, as we want to
    catch cases where the parameter specified by the analysis is scanned indirectly
    through overrides. (TODO: Do we really, though? This matches the behaviour prior to
    the refactoring towards exposing a set of required axis handles from
    DefaultAnalysis, but we should revisit this.)
    """
    stores = {a.param_store for a in axes}
    assert None not in stores, "Can only match analyses after stores have been created"
    return {a._store for a in analysis.required_axes()} == stores


def filter_default_analyses(fragment: ExpFragment,
                            axes: Iterable[ScanAxis]) -> list[DefaultAnalysis]:
    """Return the default analyses of the given fragment that can be executed for the
    given scan spec.

    See :func:`match_default_analysis`.
    """
    ax = list(axes)  # Don't exhaust an arbitrary iterable.
    result = []
    for analysis in fragment.get_default_analyses():
        if not isinstance(analysis, DefaultAnalysis):
            raise ValueError(
                f"Unexpected get_default_analyses() return value for {fragment}: "
                "Expected list of ndscan.experiment.DefaultAnalysis instances, got "
                f"element of type '{analysis}'")
        if match_default_analysis(analysis, ax):
            result.append(analysis)
    return result


def describe_scan(spec: ScanSpec, fragment: ExpFragment,
                  short_result_names: dict[ResultChannel, str]) -> dict[str, Any]:
    """Return metadata for the given spec in stringly typed dictionary form.

    :param spec: :class:`.ScanSpec` describing the scan.
    :param fragment: Fragment being scanned.
    :param short_result_names: Map from result channel objects to shortened names.
    """
    desc = {}

    desc["fragment_fqn"] = fragment.fqn
    axis_specs = [{
        "param": ax.param_schema,
        "path": ax.path,
    } for ax in spec.axes]
    for ax, gen in zip(axis_specs, spec.generators):
        gen.describe_limits(ax)

    desc["axes"] = axis_specs
    desc["seed"] = spec.options.seed

    # KLUDGE: Skip non-saved channels to make sure the UI doesn't attempt to display
    # them; they should possibly just be ignored there.
    desc["channels"] = {
        name: channel.describe()
        for (channel, name) in short_result_names.items() if channel.save_by_default
    }

    return desc


def describe_analyses(analyses: Iterable[DefaultAnalysis],
                      context: AnnotationContext) -> dict[str, Any]:
    """Return metadata for the given analyses in stringly typed dictionary form.

    :param analyses: The :class:`.DefaultAnalysis` objects to describe (already filtered
        to those that apply to the scan, and thus are describable by the context).
    :param context: Used to resolve any references to scanned parameters/results
        channels/analysis results.

    :return: The analysis metadata (``annotations``/``online_analyses``), with all
        references to fragment tree objects resolved, and ready for JSON/…
        serialisation.
    """
    desc = {}
    desc["annotations"] = []
    desc["online_analyses"] = {}
    for analysis in analyses:
        annotations, online_analyses = analysis.describe_online_analyses(context)
        desc["annotations"].extend(annotations)
        for name, spec in online_analyses.items():
            if name in desc["online_analyses"]:
                raise ValueError(
                    f"An online analysis with name '{name}' already exists")
            desc["online_analyses"][name] = spec
    return desc
