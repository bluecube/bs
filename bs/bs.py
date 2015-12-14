from . import context
from . import nodes
from . import util

import pathlib
import inspect

class Bs:
    """ Main user facing object of the build system. """

    def __init__(self, root=None, build_directory=None, output_directory=None):
        self._context = None
        if root is not None:
            self.root = pathlib.Path(root)
        else:
            caller_frame = inspect.stack()[1]
            caller_filename = caller_frame[1]
            del caller_frame
            self.root = pathlib.Path(caller_filename).parent

        if build_directory is not None:
            self.build_directory = pathlib.Path(build_dir)
        else:
            self.build_directory = self.root / "build"

        if output_directory is not None:
            self.output_directory = pathlib.Path(output_directory)
        else:
            self.output_directory = self.build_directory / "output"

    def __enter__(self):
        self._context = context.Context(self.build_directory, self.output_directory)
        return self

    def __exit__(self, ex_type, ex_val, ex_traceback):
        if ex_type is not None:
            return

        self._context.prepare_build()
        self._context.clean_build()
        self._context.save()

    def apply(self, builder, inputs, output_names = None):
        application = nodes.Application(self._context, builder, inputs, output_names)
        return application.outputs

    def add_target(self, target):
        for node in util.maybe_iterable(target):
            self._context.add_target(node)
