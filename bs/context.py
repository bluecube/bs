import contextlib
import tempfile
import collections
import pathlib
import shutil

from . import nodes
from . import cache
from . import traversal

class Context:
    """ State of the system. """
    def __init__(self, build_directory, output_directory):
        self.build_directory = build_directory
        self.output_directory = output_directory
        self.temp_directory = self.build_directory / "tmp"
        self.cache = cache.Cache(self.build_directory / "cache")

        self.verbose = True

        self._files = {} # Mapping of file paths to nodes.File instances
        self._targets = []
        self._dirty = set()

    def file_by_path(self, path):
        if path not in self._files:
            self._files[path] = nodes.SourceFile(self, path)
        return self._files[path]

    def add_target(self, target):
        if target in self._targets:
            raise Exception("Attempting to set a node as a target second time")
        self._targets.append(target)

    def _mark_targets(self):
        """ Mark the final targets in all registered nodes. """
        for target in self._targets:
            to_mark = collections.deque([target])
            while to_mark:
                node = to_mark.popleft()

                if target not in node.targets:
                    node.targets.add(target)
                    to_mark.extend(node.dependencies)

    def prepare_build(self):
        self._mark_targets()

    def clean_build(self):
        """ Build targets, don't assume any files exist. """

        traversal.update(self._targets, self._files.values(), 1)

    def run_command(self, command):
        if self.verbose:
            print(command)

        return subprocess.check_output(command,
                                       stderr=sys.stderr,
                                       universal_newline=True)

    @contextlib.contextmanager
    def tempfile(self, filenames):
        """ Context manager returning a path to a temporary file in the build directory.
        The file is created and immediately closed, so that it can be used by other
        process on windows. The file is deleted when the context manager ends. """

        try:
            self.temp_directory.mkdir(parents=True)
        except FileExistsError:
            pass
        fd, path = tempfile.mkstemp(prefix="", suffix="." + filename,
                                    dir=str(self.temp_directory))
        os.close(fd)
        path = pathlib.Path(path)

        try:
            yield path
        finally:
            if path.exists():
                path.unlink()

    @contextlib.contextmanager
    def tempdir(self):
        """ Context manager returning a path to a temporary directory in the build directory.
        The directory is deleted when the context manager ends. """

        try:
            self.temp_directory.mkdir(parents=True)
        except FileExistsError:
            pass
        path = tempfile.mkdtemp(prefix="", suffix="",
                                dir=str(self.temp_directory))
        path = pathlib.Path(path)

        try:
            yield path
        finally:
            shutil.rmtree(str(path))
