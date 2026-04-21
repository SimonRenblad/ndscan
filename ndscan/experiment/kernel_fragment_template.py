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

    @portable
    def device_setup_subfragments(self):
$device_setup

    @portable
    def device_cleanup_subfragments(self):
$device_cleanup

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
        self.fragment.run_once()
