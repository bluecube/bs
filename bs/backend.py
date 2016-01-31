from . import service
from . import context
from . import traversal

import contextlib
import logging

def connect(build_directory, force_restart):
    return service.ServiceProxy(Backend,
                                build_directory / "backend_handle.json",
                                force_restart)

class Backend(service.Service):
    _timeout = 20 * 60 # Shut down after 20 minutes of inactivity

    def __init__(self, control_file):
        #self.monitor = monitor.Monitor()
        self.context = context.Context(control_file.parent)
        self.stack = contextlib.ExitStack()

    def __enter__(self):
        try:
            #self.stack.enter_context(self.monitor)
            self.stack.callback(self.context.save)
        except:
            self.stack.close()
            raise

    def __exit__(self, ex_type, ex_value, ex_tb):
        suppress = self.stack.__exit__(ex_type, ex_value, ex_tb)
        suppress = suppress or ex_type == TimeoutError
        return suppress

    def need_run_config(build_script, self):
        return True

    def upload_targets(self, build_script, targets):
        #TODO: Targets uploaded here should have limited life time
        # else we would leak targets from unused build scripts
        self.context.set_targets(build_script, targets)

    def update(self, build_script, targets, output_directory):
        available_targets = self.context.targets[build_script]
        if targets is None:
            selected_targets = available_targets
        else:
            selected_targets = (target for target
                                in available_targes
                                if target.name in targets)
        traversal.update(self.context, selected_targets, self.context.files.values())
        #TODO: return service.IteratorWrapper with events (building X, building X failed, ...)
