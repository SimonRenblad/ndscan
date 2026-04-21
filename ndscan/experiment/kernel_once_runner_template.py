from artiq.language import (EnvExperiment, HasEnvironment, kernel, portable, compile, rpc, TerminationRequested)
from artiq.coredevice.exceptions import RTIOUnderflow
from artiq.language.core import Kernel, KernelInvariant, Core

from .fragment import (ExpFragment, Fragment, RestartKernelTransitoryError,
                       TransitoryError)
from .parameters import ParamBase, ParamStore
from .$fragment_class import Wrapper$fragment_class

@compile
class _InnerFragmentRunner(HasEnvironment):
    fragment: KernelInvariant[Wrapper$fragment_class]
    core: KernelInvariant[Core]
    max_rtio_underflow_retries: KernelInvariant[int32]
    max_transitory_error_retries: KernelInvariant[int32]
    skip_on_persistent_transitory_error: KernelInvariant[bool]
    num_underflows_caught: Kernel[int32]
    num_transitory_errors_caught: Kernel[int32]
    """Object wrapping fragment execution to be able to execute everything in one kernel
    invocation (no difference for non-kernel fragments).
    """
    def build(self, fragment, max_rtio_underflow_retries: int,
              max_transitory_error_retries: int):
        self.fragment = Wrapper$fragment_class(fragment)
        self.max_rtio_underflow_retries = max_rtio_underflow_retries
        self.max_transitory_error_retries = max_transitory_error_retries
        self.num_underflows_caught = 0
        self.num_transitory_errors_caught = 0


    def run(self) -> bool:
        """Execute device_setup()/run_once(), retrying if nececssary.

        :return: ``True`` if execution completed, ``False`` if it should be attempted
            again (RestartKernelTransitoryError).
        """
        # TODO: Unify with FragmentScanExperiment._run_continuous().
        if is_kernel(self.fragment.fragment.run_once):
            self.setattr_device("core")
            return self._run_on_kernel()
        else:
            return self._run()

    @kernel
    def _run_on_kernel(self):
        """Force the portable _run() to run on the kernel."""
        return self._run()

    # TODO(srenblad): this will need to be separate for kernel and host..
    @portable
    def _run(self):
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
                    # print("Ignoring RTIOUnderflow (", self.num_underflows_caught, "/",
                    #       self.max_rtio_underflow_retries, ")")
                except RestartKernelTransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
                    # print("Caught transitory error, restarting kernel")
                    return False
                except TransitoryError:
                    self.num_transitory_errors_caught += 1
                    if (self.num_transitory_errors_caught >
                            self.max_transitory_error_retries):
                        raise
                    # print("Caught transitory error (",
                    #       self.num_transitory_errors_caught, "/",
                    #       self.max_transitory_error_retries, "), retrying")
        finally:
            self.fragment.device_cleanup()
        assert False, "Execution never reaches here, return is just to pacify compiler."
        return True
