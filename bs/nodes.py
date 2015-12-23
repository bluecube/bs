from . import util

class Node:
    """ Base class for node of the dependency graph.
    context: Used for resolving paths nad registering nodes and hashes.
             Either the bs.context.Context or bs.Bs that contains. """
    def __init__(self, context):
        try:
            self.context = context._context
        except AttributeError:
            self.context = context

        self.dependencies = set()
        self.reverse_dependencies = set()
        self.targets = set()

    def add_dependency(self, other):
        if other in self.dependencies:
            raise RuntimeError("Dependency already existed")
        assert self not in other.reverse_dependencies

        self.dependencies.add(other)
        other.reverse_dependencies.add(self)

    def remove_dependency(self, other):
        if other not in self.dependencies:
            raise RuntimeError("Removing nonexistent dependency")

        self.dependencies.remove(other)
        other.reverse_dependencies.remove(self)

    def get_hash(self):
        raise NotImplementedError()

    def update(self):
        """ Called when a change is detected on a node or its dependencies. """
        pass

    @classmethod
    def str_helper(cls, *args):
        return cls.__name__ + "(" + ", ".join(str(arg) for arg in args) + ")"

    @classmethod
    def hash_helper(cls, *args):
        return util.sha1_iterable([cls.__name__], *args)


class Builder(Node):
    def build(self, input_paths, output_paths):
        raise NotImplementedError()

    def get_output_names(self, input_names):
        """ Return list of names for the output files.
        This list also specifies the number of outputs.
        If a default name doesn't make sense for an ouptut, None should be in its place in the list. """
        raise NotImplementedError()

    def get_hash(self): # Included here just to make requirements for Builder clear.
        raise NotImplementedError()

    def __str__(self):
        return self.__class__.__name__

class Application(Node):
    """ A node that connects builder, inputs files and generated files. """
    def __init__(self, context, builder, inputs, output_names):
        super().__init__(context)

        self.builder = builder
        self.add_dependency(builder)

        self.inputs = [self._wrap_input(x) for x in util.maybe_iterable(inputs)]
        for input in self.inputs:
            self.add_dependency(input)

        output_count = builder.get_output_count(len(self.inputs))

        if output_names is None:
            output_names = [None] * output_count
        else:
            output_names = list(util.maybe_iterable(output_names))
            if len(output_names) != output_count:
                raise Exception("Wrong number of output names passed")

        self.outputs = [GeneratedFile(self.context, self, i, name)
                        for i, name
                        in enumerate(output_names)]

        self.timer = util.Timer()

        self.implicit_dependencies = None
        self._find_cached_implicit_dependencies()

    def _find_cached_implicit_dependencies(self):
        partial_hash = self._get_hash(None)
        candidates = self.context.cache.get_candidate_implicit_dependencies(partial_hash)

        for deps in candidates:
            if self._try_implicit_dependencies(deps):
                return True

        self._set_implicit_dependencies(None)
        return False

    def _try_implicit_dependencies(self, deps):
        ret = []
        for path, hash in deps:
            node = self.context.file_by_path(path)
            if node.get_hash() == hash:
                ret.append(node)
            else:
                return False
        self._set_implicit_dependencies(ret)
        return True

    def _wrap_input(self, input):
        if isinstance(input, Node):
            return input
        else:
            return self.context.file_by_path(input)

    def _set_implicit_dependencies(self, nodes):
        if self.implicit_dependencies is not None:
            for node in self.implicit_dependencies:
                self.remove_dependency(node)
        self.implicit_dependencies = nodes
        if nodes is not None:
            for node in nodes:
                if node not in self.dependencies:
                    self.add_dependency(node)

    def update(self):
        if self._find_cached_implicit_dependencies():
            print("Have cached resutls", str(self))
            return

        #print("Building", str(self))
        with self.context.tempdir() as temp, \
             self.timer:

            input_paths = [input.get_path() for input in self.inputs]
            output_paths = [temp/output.name for output in self.outputs]

            computed_deps = self.builder.build(input_paths, output_paths)
            if computed_deps is None:
                computed_deps = []

            self._set_implicit_dependencies([self.context.file_by_path(p) for p in computed_deps])

            self.context.cache.put(self.get_hash(), self._get_hash(None),
                                   output_paths,
                                   [(node.get_path(), node.get_hash()) for node in self.implicit_dependencies])

            for node in self.inputs:
                node.accessed()
            for node in self.implicit_dependencies:
                node.accessed()

    def get_hash(self):
        return self._get_hash(self.implicit_dependencies)

    def _get_hash(self, implicit_dependencies):
        if implicit_dependencies is not None:
            implicit_dependencies = (x.get_hash() for x in implicit_dependencies)
        else:
            implicit_dependencies = [None]
        return self.hash_helper([self.builder.get_hash()],
                                (x.get_hash() for x in self.inputs),
                                implicit_dependencies)

    def accessed(self):
        """ Called after one of this application's files is used. """
        assert(self.implicit_dependencies is not None)
        self.context.cache.accessed(self.get_hash())

    def __str__(self):
        return self.str_helper(self.builder, *self.inputs)


class File(Node):
    def __init__(self, context):
        super().__init__(context)
        self.directory = _DirectoryProxy(self)

    def get_path(self):
        raise NotImplementedError()

    def accessed(self):
        return

class SourceFile(File):
    def __init__(self, context, path):
        super().__init__(context)
        self.path = path

    def get_path(self):
        return self.path

    def get_hash(self):
        return util.sha1_file(self.path)

    def __str__(self):
        return str(self.path)


class GeneratedFile(File):
    """ A file generated by a build step.
    It may have a fixed filename component, but can change its path.
    This kind of file can be deleted any time (forcing rebuilds when it is necessary later)
    or (potentially) cached even when not needed. """

    def __init__(self, context, application, index, name):
        super().__init__(context)
        assert(context is application.context)
        self.application = application
        self.index = index
        self.name = name or "output{:02d}".format(index)
        self.add_dependency(application)

    def get_path(self):
        return self.context.cache.get_directory(self.application.get_hash()) / self.name

    def get_hash(self):
        return self.hash_helper([self.application.get_hash(), self.index, self.name])

    def accessed(self):
        self.application.accessed()

    def __str__(self):
        return self.str_helper(self.application.builder.__class__.__name__,
                               self.index)


class _DirectoryProxy:
    def __init__(self, file_node):
        self.node = file_node

    def __str__(self):
        return str(self.node.get_path().parent)
