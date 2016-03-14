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
import concurrent.futures

def connect(build_directory, force_restart):
    try:
        build_directory.mkdir(parents=True)
    except FileExistsError:
        pass
    return service.ServiceProxy(Backend,
                                build_directory / "backend_handle.json",
                                force_restart)

def _node_job(node, context):
    """ Launched in another thread, updates a single node and submits jobs for
    other nodes. """
    try:
        context.log(str(node))
        if context.stop_flag:
            return

        with context.backend._lock:
            need_update = not node.dirty
            unlocked_nodes = self._set_node_clean(node) #!!!

        if need_update:
            node.update(context)

        if context.stop_flag:
            return

        with context.backend._lock:
            for target in node.targets & c.targets:
                target.submit_waiting_jobs(context)
                # TODO: Avoid submiting the same node twice

        if node in c.targets:
            c.processed_targets.add(node)
            self._link_output_file(node, output_directory)
    except Exception as e:
        c.exception(e)

class _TargetData:
    """ Represents target. """
    def __init__(self, backend, target_node):
        # Dirty nodes that are dirty, but don't have any dirty dependencies
        self.waiting_nodes = weakref.WeakSet()
        self.node = self._process_nodes(backend, target_node)

    def submit_waiting_jobs(self, context):
        """ Submit update jobs for all nodes depended on by the target
            that are currently waiting """
        context.log("Submiting {} jobs", len(self.waiting_nodes))
        for node in self.waiting_nodes:
            context.backend.executor.submit(_node_job(node, context))

    def _process_nodes(self, backend, target_node):
        """ Visit all dependencies of the targets and prepare them. """
        to_visit = collections.deque([target_node])
        while to_visit:
            node = to_visit.popleft()

            # TODO: Maybe merge even non-file nodes
            if isinstance(node, nodes.SourceFile):
                assert len(node.dependencies) == 0
                assert node is not target_node # TODO: Check this sooner with an understandable exception
                if node.path in backend.files:
                    old_node = node
                    node = backend.files[node.path]

                    for revdep in node.reverse_dependencies:
                        revdep.remove_dependency(old_node)
                        revdep.add_dependency(node) # TODO: Node names

                    if old_node is target_node:
                        # Edge case
                        # The target node will always be visited first, so there
                        # shouldn't be a problem with changing the node reference
                        # while it's already stored in other nodes.
                        # TODO: Write a test for this.
                        target_node = node
                else:
                    backend.files[node.path] = node

            if node.targets is None:
                node.targets = weakref.WeakSet()

            if self not in node.targets:
                node.targets.add(self)
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
                self.waiting_nodes.add(node)

        return target_node


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

            self.executor = concurrent.futures.ThreadPoolExecutor(4) #TODO: Configurable number of workers

            #self.monitor = monitor.Monitor()
        except:
            self.stack.close()
            raise

    def __enter__(self):
        try:
            self.stack.enter_context(self.cache)
            self.stack.enter_context(self.executor)
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
        """ Update targets. Returns an iterator with progress messages. """

        available_targets = self.target_data[build_script]
        if target_names is None:
            selected_targets = available_targets
        else:
            selected_targets = (target for target
                                in available_targes
                                if target.name in targets)

        with open("/tmp/nodes", "w") as fp:
            self._dump_graph(fp)

        c = context.Context(self, selected_targets, output_directory)
        # TODO: Stop context when connection from client is closed

        for target in selected_targets:
            target.submit_waiting_jobs(c)

        c.log("X")

        return service.IteratorWrapper(c.iterate_log_messages())

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
#                self.target_data[target].waiting_nodes.add(node)
#        if not dirty 

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

