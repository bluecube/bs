import contextlib
import tempfile
import collections
import pathlib
import shutil
import os
import subprocess
import sys

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
        path = pathlib.Path(path)
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

        with open("/tmp/nodes.dot", "w") as fp:
            self.dump_graph(fp)

        traversal.update(self._targets, self._files.values(), 1)

        self._link_targets(self._targets)

    def _link_targets(self, targets):
        """ Link the specified target files to the output directory. """
        try:
            self.output_directory.mkdir(parents=True)
        except FileExistsError:
            pass

        try:
            relative_output_directory = self.output_directory.relative_to(self.build_directory)
        except ValueError:
            relative_build_directory = None
        else:
            relative_build_directory = pathlib.Path("..")
            for _ in relative_output_directory.parts[1:]:
                relative_build_directory = relative_build_directory / ".."

        for target in targets:
            cached_path = target.get_path()
            output_file = self.output_directory / target.name

            if output_file.exists() or output_file.is_symlink():
                output_file.unlink()

            symlink_path = cached_path.resolve() # Fallback if relatie paths can't be used
            try:
                relative_cached_file = cached_path.relative_to(self.build_directory)
            except ValueError:
                pass
            else:
                if relative_build_directory is not None:
                    symlink_path = relative_build_directory / relative_cached_file

            print("Symlinking", symlink_path, "to", output_file)

            output_file.symlink_to(symlink_path)

    def run_command(self, command):
        command = [str(x) for x in command]
        if self.verbose:
            print(command)

        return subprocess.check_output(command,
                                       stderr=sys.stderr,
                                       universal_newlines=True)

    def dump_graph(self, fp):
        to_process = list(self._targets)[:]
        nodes = set(to_process)

        fp.write("digraph Nodes{")

        while to_process:
            node = to_process.pop()
            for dep in node.dependencies:
                fp.write("{} -> {};".format(id(dep), id(node)))
                if dep not in nodes:
                    nodes.add(dep)
                    to_process.append(dep)

        for node in nodes:
            fp.write('{}[label="{}"];'.format(id(node), str(node)))
        fp.write("}")

    @contextlib.contextmanager
    def tempfile(self, filename=""):
        """ Context manager returning a path to a temporary file in the build directory.
        The file is created and immediately closed, so that it can be used by other
        process on windows. The file is deleted when the context manager ends. """

        try:
            self.temp_directory.mkdir(parents=True)
        except FileExistsError:
            pass

        if len(filename):
            suffix = "." + filename
        else:
            suffix = ""

        fd, path = tempfile.mkstemp(prefix="", suffix=suffix,
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
