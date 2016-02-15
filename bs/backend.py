from . import service
from . import context
from . import traversal

import contextlib
import queue

def connect(build_directory, force_restart):
    try:
        build_directory.mkdir(parents=True)
    except FileExistsError:
        pass
    return service.ServiceProxy(Backend,
                                build_directory / "backend_handle.json",
                                force_restart)

class TaskLog:
    """ A message passing queue """
    def __init__(self, context):
        self.context = context
        self.queue = queue.Queue()

    def log(self, fmt, *args, **kwargs):
        self.queue.put(fmt.format(*args, **kwargs))

    def __iter__(self):
        item = self.queue.get()
        while item is not None:
            yield item
            item = self.queue.get()

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
        with open("/tmp/nodes", "w") as fp:
            self.context.dump_graph(fp)

        self.context.update(q, selected_targets)

        return service.IteratorWrapper(_iterate_queue(q))
