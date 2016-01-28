from . import backend as backend_
from . import nodes
from . import util

import pathlib
import inspect

class UserContext:
    def __init__(self, root):
        self.root = root
        self._files = {}
        self._targets = []

    def apply(self, builder, inputs, output_names = None):
        inputs = [self._wrap_input(x) for x in util.maybe_iterable(inputs)]
        output_count = builder.get_output_count(len(inputs))

        if output_names is None:
            output_names = [None] * output_count
        else:
            output_names = list(util.maybe_iterable(output_names))
            if len(output_names) != output_count:
                raise Exception("Wrong number of output names passed "
                                "(builder requires {}, given {})".format(output_count,
                                                                         len(output_names)))

        application = nodes.Application(builder, inputs, output_names)
        return application.outputs

    def _wrap_input(self, input):
        if isinstance(input, nodes.Node):
            return input
        else:
            path = pathlib.Path(input)
            if path not in self._files:
                self._files[path] = nodes.SourceFile(path)
            return self._files[path]

    def add_target(self, target):
        self._targets.append(target)

def run(configure_callback,
        root_directory = None,
        build_directory = None,
        output_directory = None):
    """ Run the build.
    configure_callback is possibly invoked if necessary.
    build_directory sets the build directory. Default is location_of_caller / "build".
                    This location also holds a reference to the backend process,
    output_directory sets where the built targets are placed. Deault is build_directory / "output"."""

    caller_frame = inspect.stack()[1]
    caller_filename = pathlib.Path(caller_frame[1])
    del caller_frame

    if root_directory is not None:
        root_directory = pathlib.Path(root_directory)
    else:
        root_directory = caller_filename.parent

    if build_directory is not None:
        build_directory = pathlib.Path(build_dir)
    else:
        build_directory = root_directory / "build"

    if output_directory is not None:
        output_directory = pathlib.Path(output_directory)
    else:
        output_directory = build_directory / "output"

    with backend_.connect(build_directory) as backend:
        if backend.need_run_config():
            context = UserContext(root_directory)
            configure_callback(context)
            backend.upload_targets(caller_filename, context._targets)

        backend.update(caller_filename, None, output_directory) # TODO: Always updating all targets
