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
