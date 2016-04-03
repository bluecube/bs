from . import nodes
from . import cache
from . import traversal

import queue
import contextlib

class Context:
    """ State of single update.
    Used by the nodes' update methods as an interface to backend and
    to give reports trough the shared queue. """

    def __init__(self, backend, targets, output_directory):
        self.stop_flag = False

        self.backend = backend
        self._queue = queue.Queue()
        self._finished = False
        self._exception = None
        self.targets = targets

    def log(self, fmt, *args, **kwargs):\
        #TODO: Convert this to use logging
        self._queue.put(fmt.format(*args, **kwargs))

    def finish(self):
        self._finished = True

    def exception(self, e):
        self._finished = True
        self.stop_flag = True
        self._exception = e

    def iterate_log_messages(self):
        """ Go through the logged messages.
        This is intended to be called from a different thread than writing the messages.
        Iteration stops if the future associated with this context is not running
        and will raise any exceptions raised inside the future. """

        while not (self._finished and self._queue.empty()):
            item = self._queue.get()
            yield item

        if self._exception:
            raise self._exception

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
