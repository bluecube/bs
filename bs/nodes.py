from . import util
import pathlib

class Node:
    """ Base class for node of the dependency graph. """
    def __init__(self):
        self.dependencies = set()
        self.named_dependencies = {}

        # The following is only used by the context -- gets set to a set after
        # being transfered to backend.
        self.reverse_dependencies = None
        self.targets = None
        self.dirty = None

    def add_dependency(self, other, name=None):
        if other in self.dependencies:
            raise RuntimeError("Dependency already existed")

        if name is not None:
            if name in self.named_dependencies:
                raise RuntimeError("Dependency name already existed")
            self.named_dependencies[name] = other

        self.dependencies.add(other)

    def remove_dependency(self, other):
        if other not in self.dependencies:
            raise RuntimeError("Removing nonexistent dependency")

        for k, v in self.named_dependencies.items():
            if v is other:
                del self.named_dependencies[k]
                break

        self.dependencies.remove(other)

    def get_hash(self):
        raise NotImplementedError()

    def update(self, context):
        """ Called when a change is detected on a node or its dependencies. """
        pass

    def expand_variables(self, context, string):
        class Wrapper:
            """ Maps self.name to o.get_name(context) """
            def __init__(self, o, context):
                self._o = o
                self._context = context
            def __getattr__(self, name):
                return getattr(self._o, "get_" + name)(self._context)
        return string.format(**{k: Wrapper(v, context)
                                for k, v
                                in self.named_dependencies.items()})

    @classmethod
    def str_helper(cls, *args):
        return cls.__name__ + "(" + ", ".join(str(arg) for arg in args) + ")"

    @classmethod
    def hash_helper(cls, *args):
        return util.sha1_iterable([cls.__name__], *args)

    def __format__(self, fmt):
        raise Exception("You shouldn't format nodes. Maybe `.path` or `.directory` is misising?")


class Builder(Node):
    def build(self, context, input_paths, output_paths):
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
    def __init__(self, builder, inputs, output_names):
        super().__init__()

        self.builder = builder
        self.add_dependency(builder)

        self.inputs = inputs
        for input in self.inputs:
            self.add_dependency(input)

        self.outputs = [GeneratedFile(self, i, name)
                        for i, name
                        in enumerate(output_names)]

        self.timer = util.Timer()

        self.implicit_dependencies = None

    def _find_cached_implicit_dependencies(self, context):
        partial_hash = self._get_hash(None)
        candidates = context.cache.get_candidate_implicit_dependencies(partial_hash)

        for deps in candidates:
            if self._try_implicit_dependencies(context, deps):
                return True

        self._set_implicit_dependencies(None)
        return False

    def _try_implicit_dependencies(self, context, deps):
        ret = []
        for path, hash in deps:
            node = context.file_by_path(path)
            if node.get_hash() == hash:
                ret.append(node)
            else:
                return False
        self._set_implicit_dependencies(ret)
        return True

    def _set_implicit_dependencies(self, nodes):
        if self.implicit_dependencies is not None:
            for node in self.implicit_dependencies:
                self.remove_dependency(node)
        self.implicit_dependencies = nodes
        if nodes is not None:
            for node in nodes:
                if node not in self.dependencies:
                    self.add_dependency(node)

    def update(self, context):
        if self._find_cached_implicit_dependencies(context):
            #print("Have cached resutls", str(self))
            return

        #print("Building", str(self))
        with context.tempdir() as temp, \
             self.timer:

            input_paths = [input.get_path(context) for input in self.inputs]
            output_paths = [temp/output.name for output in self.outputs]

            computed_deps = self.builder.build(context, input_paths, output_paths)
            if computed_deps is None:
                computed_deps = []

            implicit_dependencies = []
            for path in computed_deps:
                path = pathlib.Path(path)
                if not path.is_absolute():
                    raise Exception("Builder must return implicit dependencies as absolute paths.")
                node = context.file_by_path(path)
                node.targets.union(self.targets)
                implicit_dependencies.append(node)

            self._set_implicit_dependencies(implicit_dependencies)

            context.cache.put(self.get_hash(), self._get_hash(None),
                              output_paths,
                              [(node.get_path(context), node.get_hash()) for node in self.implicit_dependencies])

            for node in self.inputs:
                node.accessed(context)
            for node in self.implicit_dependencies:
                node.accessed(context)

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

    def accessed(self, context):
        """ Called after one of this application's files is used. """
        assert(self.implicit_dependencies is not None)
        context.cache.accessed(self.get_hash())

    def __str__(self):
        return self.str_helper(self.builder, *self.inputs)


class File(Node):
    def __init__(self):
        super().__init__()

    def get_path(self, context):
        raise NotImplementedError()

    def get_directory(self, context):
        return self.get_path(context).parent

    def accessed(self, context):
        return

class SourceFile(File):
    def __init__(self, path):
        super().__init__()
        self.path = util.make_absolute(path)

    def get_path(self, context):
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

    def __init__(self, application, index, name):
        super().__init__()
        self.application = application
        self.index = index
        self.name = name or "output{:02d}".format(index)
        self.add_dependency(application)

    def get_path(self, context):
        return context.cache.get_directory(self.application.get_hash()) / self.name

    def get_hash(self):
        return self.hash_helper([self.application.get_hash(), self.index, self.name])

    def accessed(self, context):
        self.application.accessed(context)

    def __str__(self):
        return self.str_helper(self.application.builder.__class__.__name__,
                               self.index)
