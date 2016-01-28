from . import service
from . import context

import contextlib
import logging

def connect(build_directory):
    return service.ServiceProxy(Backend, build_directory / "backend_handle.json")

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
        suppress = self.stack.__exit(ex_type, ex_value, ex_tb)
        suppress = suppress or ex_type == TimeoutError
        return suppress

    def need_run_config(self):
        return True

    def upload_targets(self, build_script, targets):
        self.context.set_targets(build_script, targets)

    def update(self, build_script, targets, output_directory):
        self.context.update()
