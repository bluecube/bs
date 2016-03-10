from . import service
from . import context
from . import traversal
from . import cache
from . import nodes

import tempfile
import collections
import pathlib
import shutil
import os
import subprocess
import sys
import weakref
import contextlib
import threading

def connect(build_directory, force_restart):
    try:
        build_directory.mkdir(parents=True)
    except FileExistsError:
        pass
    return service.ServiceProxy(Backend,
                                build_directory / "backend_handle.json",
                                force_restart)

class _TargetData:
    def __init__(self, backend, target_node):
        self.node = target_node

        # Dirty nodes that are dirty, but don't have any dirty dependencies
        self.start_nodes = weakref.WeakSet()

        self._process_nodes(backend, target_node)

    def _process_nodes(self, backend, target):
        """ Visit all dependencies of the targets and prepare them. """
        to_visit = collections.deque([target])
        while to_visit:
            node = to_visit.popleft()

            # TODO: Maybe merge even non-file nodes
            if isinstance(node, nodes.SourceFile):
                assert len(node.dependencies) == 0
                assert node is not target # TODO: Check this sooner with an understandable exception
                if node.path in backend.files:
                    old_node = node
                    node = backend.files[node.path]

                    for revdep in node.reverse_dependencies:
                        revdep.remove_dependency(old_node)
                        revdep.add_dependency(node) # TODO: Node names

                    if old_node is target:
                        # Edge case
                        # The target node will always be visited first, so there
                        # shouldn't be a problem with changing the node reference
                        # while it's already stored in other nodes.
                        # TODO: Write a test for this.
                        target = node
                else:
                    backend.files[node.path] = node

            if node.targets is None:
                node.targets = weakref.WeakSet()

            if target not in node.targets:
                node.targets.add(target)
                to_visit.extend(node.dependencies)

            if node.reverse_dependencies is None:
                node.reverse_dependencies = weakref.WeakSet()

            # TODO: I don't like this part:
            for dep in node.dependencies:
                if dep.reverse_dependencies is None:
                    dep.reverse_dependencies = weakref.WeakSet()
                dep.reverse_dependencies.add(node)

            # All nodes are initially dirty
            node.dirty = True
            if not node.dependencies:
                self.start_nodes.add(node)

        return target


class Backend(service.Service):
    """ State of the build system itself. Holds the graph of dependencies.
    Intended to run as a service, but probably could also work directly. """
    _timeout = 20 * 60 # Shut down after 20 minutes of inactivity

    def __init__(self, control_file):
        self.stack = contextlib.ExitStack()
        enter_context = self.stack.enter_context

        try:
            self.build_directory = control_file.parent
            self.temp_directory = self.build_directory / "tmp"
            self.cache = cache.Cache(self.build_directory / "cache")

            self.files = weakref.WeakValueDictionary() # Mapping of file paths to nodes.File instances
            self.target_data = {} # build script path -> [_TargetData]

            #self.monitor = monitor.Monitor()
        except:
            self.stack.close()
            raise

    def __enter__(self):
        try:
            self.stack.enter_context(self.cache)
            #self.stack.enter_context(self.monitor)
        except:
            self.stack.close()
            raise

    def __exit__(self, ex_type, ex_value, ex_tb):
        suppress = self.stack.__exit__(ex_type, ex_value, ex_tb)
        suppress = suppress or ex_type == TimeoutError
        return suppress

    def need_run_config(build_script, self):
        return True

    def set_targets(self, build_script, targets):
        #TODO: Targets uploaded here should have limited life time
        # else we would leak targets from unused build scripts
        self.target_data[build_script] = [_TargetData(self, target) for target in targets]

    def update(self, build_script, target_names, output_directory):
        """ Update targets.
        This function runs the job in a thread and returns an iterator with progress
        messages. """
        available_targets = self.target_data[build_script]
        if target_names is None:
            selected_targets = available_targets
        else:
            selected_targets = (target for target
                                in available_targes
                                if target.name in targets)

        with open("/tmp/nodes", "w") as fp:
            self._dump_graph(fp)

        c = context.Context(self)

        thread = threading.Thread(target=self._update_internal,
                                  args=(c, selected_targets, output_directory),
                                  daemon=True)
        thread.start()
        # TODO: The thread should be stopped when the client disconnects
        # TODO: Or maybe we shouldn't have a separate thread here?
        # It could all be done in the regular workers themselves.
        # Outputs could be linked when update is processed on a node that is a target.

        return service.IteratorWrapper(c.iterate_log_messages())

    def _update_internal(self, c, selected_target_data, output_directory):
        """ Launched in anothre thread, does the actual work. """
        try:
            targets = set()
            initial_dirty = set()
            for target_data in selected_target_data:
                targets.add(target_data.node)
                initial_dirty.update(target_data.start_nodes)

                c.log(str(target_data))

            #traversal.update(self, targets, self.files.values())

            #self._link_outputs(targets, output_directory)
        except Exception as e:
            c.exception(e)
        else:
            c.finish()

    def _file_by_path(self, path):
        #TODO: Is this one needed?
        if path not in self.files:
            node = nodes.SourceFile(path)
            node.targets = weakref.WeakSet()
            self.files[path] = node

        return self.files[path]

#    def _set_node_dirty(self, node, dirty):
#        assert dirty != node.dirty
#
#        node.dirty = dirty
#
#        if dirty and all(not dep.dirty for dep in node.dependencies):
#            for target in node.targets:
#                self.target_data[target].start_nodes.add(node)
#        if not dirty 
#

    def _link_outputs(self, targets, output_directory):
        """ Link the specified target files to the output directory. """
        try:
            output_directory.mkdir(parents=True)
        except FileExistsError:
            pass

        try:
            relative_output_directory = output_directory.relative_to(self.build_directory)
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

    def _dump_graph(self, fp):
        """ Write the graph in graphviz format """
        fp.write("digraph Nodes{\n")

        to_process = []
        nodes = set()

        for source_id, target_data in self.target_data.items():
            fp.write('"{}"[shape="box"];\n'.format(source_id))
            for target_data_item in target_data:
                node = target_data_item.node
                fp.write('{} -> "{}";\n'.format(id(node), source_id))
                if node in nodes:
                    continue
                nodes.add(node)
                to_process.append(node)

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

