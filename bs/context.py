import contextlib
import tempfile
import collections
import pathlib
import shutil
import os
import subprocess
import sys
import weakref

from . import nodes
from . import cache
from . import traversal

class Context:
    """ State of the system.
    Holds the graph of dependencies. """
    def __init__(self, build_directory):
        self.build_directory = build_directory
        self.temp_directory = self.build_directory / "tmp"
        self.cache = cache.Cache(self.build_directory / "cache")

        self.files = weakref.WeakValueDictionary() # Mapping of file paths to nodes.File instances
        self.targets = {} # build script path -> [targets]
        self.dirty = weakref.WeakSet()

    def file_by_path(self, path):
        if path not in self.files:
            node = nodes.SourceFile(path)
            node.targets = weakref.WeakSet()
            self.files[path] = node

        return self.files[path]
        self.dirty = weakref.WeakSet()

    def set_targets(self, build_script, targets):
        for target in targets:
            self._check_target_nodes(target)
        self.targets[build_script] = targets

    def _check_target_nodes(self, target):
        """ Mark the final targets in all dependency nodes,  """
        to_visit = collections.deque([target])
        while to_visit:
            node = to_visit.popleft()

            if isinstance(node, nodes.SourceFile):
                assert len(node.dependencies) == 0
                assert node is not target # TODO: Check this sooner with an understandable exception
                if node.path in self.files:
                    old_node = node
                    node = self.files[node.path]

                    for revdep in node.reverse_dependencies:
                        revdep.remove_dependency(old_node)
                        revdep.add_dependency(node) # TODO: Node names
                else:
                    self.files[node.path] = node

            if node.targets is None:
                node.targets = weakref.WeakSet()

            if target not in node.targets:
                node.targets.add(target)
                to_visit.extend(node.dependencies)

    def save(self):
        self.cache.save()

    def _link_targets(self, targets, output_directory):
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
            cached_path = target.get_path(self)
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

    def run_command(self, command, timeout=600): #TODO: Somehow set default timeout
        command = [str(x) for x in command]
        #if self.verbose: TODO: Client has to run the build steps !!!
            #print(command)

        with subprocess.Popen(command,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              universal_newlines=True) as p:
            try:
                stdout, stderr = p.communicate(timeout=timeout)
            except:
                p.kill()
                p.wait()
                raise

            if p.returncode != 0:
                raise Exception("Command failed", command, stdout, stderr, p.returncode)

            return stdout

    def dump_graph(self, fp):
        fp.write("digraph Nodes{\n")

        to_process = []
        nodes = set()

        for source_id, targets in self.targets.items():
            fp.write('"{}"[shape="box"];\n'.format(source_id))
            for target in targets:
                fp.write('"{}" -> {};\n'.format(source_id, id(target)))
                if target in nodes:
                    continue
                nodes.add(target)
                to_process.append(target)

        while to_process:
            node = to_process.pop()
            for dep in node.dependencies:
                fp.write("{} -> {};\n".format(id(dep), id(node)))
                if dep not in nodes:
                    nodes.add(dep)
                    to_process.append(dep)

        for node in nodes:
            fp.write('{}[label="{}"];\n'.format(id(node), str(node)))
        fp.write("}\n")

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
