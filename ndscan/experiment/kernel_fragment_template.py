from artiq.language import HasEnvironment, kernel, portable, rpc, compile, Kernel, KernelInvariant
from collections import OrderedDict
from collections.abc import Iterable
from copy import deepcopy
import logging
from typing import Any, Callable

from ndscan.experiment.fragment import Fragment
from ndscan.experiment.default_analysis import DefaultAnalysis, ResultPrefixAnalysisWrapper
from ndscan.experiment.result_channels import ResultChannel, FloatChannel

from $fragment_module import $fragment_name
$subfrags_imports

logger = logging.getLogger(__name__)

@rpc(flags={"async"})
def log_failed_cleanup(path: str):
    logger.error(f"device_cleanup() failed for '{path}'.")


@compile
class Wrapper$fragment_name:
    fragment: KernelInvariant[$fragment_name]
$subfrags_types

    """Main building block."""
    def __init__(self, fragment, *args, **kwargs):
        """Initialise this fragment instance; called from the ``HasEnvironment``
        constructor.

        This sets up the machinery for registering parameters and result channels with
        the fragment tree, and then calls :meth:`build_fragment` to actually perform the
        fragment-specific setup. This method should not typically be overwritten.

        :param fragment_path: Full path of the fragment, as a list starting from the
            root. For instance, ``[]`` for the top-level fragment, or ``["foo", "bar"]``
            for a subfragment created by ``setattr_fragment("bar", …)`` in a fragment
            created by ``setattr_fragment("foo", …)``.
        :param args: Arguments to be forwarded to :meth:`build_fragment`.
        :param kwargs: Keyword arguments to be forwarded to :meth:`build_fragment`.
        """
        self.fragment = fragment
        for s in self.fragment._subfragments:
$subfrags_const

        #: Maps names of non-overridden parameters of this fragment (i.e., matching the
        #: attribute names of the respective ParamHandles) to *Param instances.
        self._free_params = OrderedDict()

        #: Maps own attribute name to the ParamHandles of the rebound parameters in
        #: their original subfragment.
        self._rebound_subfragment_params = dict()

        #: List of (param, store) tuples of parameters set to their defaults after
        #: init_params().
        self._default_params = []

        #: Maps full path of own result channels to ResultChannel instances.
        self._result_channels = {}

    @portable
    def device_setup_subfragments(self):
$device_setup

    @portable
    def device_cleanup_subfragments(self):
$device_cleanup

    def _has_trivial_device_setup(self):
        assert not self._building
        empty_setup = self.device_setup.__func__ is Fragment.device_setup
        return empty_setup and self._all_subfragment_setup_trivial

    def _has_trivial_device_cleanup(self):
        assert not self._building
        empty_cleanup = self.device_cleanup.__func__ is Fragment.device_cleanup
        return empty_cleanup and self._all_subfragment_cleanup_trivial

    def host_setup(self):
        for s in self._subfragments:
            if s in self._detached_subfragments:
                continue
            s.host_setup()

    @portable
    def device_setup(self):
        self.fragment.device_setup()
        self.device_setup_subfragments()

    def host_cleanup(self):
        for s in self._subfragments[::-1]:
            if s in self._detached_subfragments:
                continue
            try:
                s.host_cleanup()
            except Exception:
                logger.exception("Cleanup failed for '%s'", s._stringize_path())

    @portable
    def device_cleanup(self):
        self.fragment.device_cleanup()
        self.device_cleanup_subfragments()

    @portable
    def run_once(self):
        self.fragment.run_once()
