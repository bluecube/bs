from nose.tools import *
import tempfile
import pathlib
from bs import service
import os
import contextlib
import signal
import time
import multiprocessing
import pickle

@nottest
class FunkyException(Exception):
    pass

@nottest
class S(service.Service):
    _timeout = 60

    class _connection_class:
        def __init__(self, instance, address):
            self.instance = instance
            self.address = address

        def __enter__(self):
            self.instance._connections.append(self.address)

        def __exit__(self, *exc_info):
            self.instance._connections.remove(self.address)

    def __init__(self, control_file):
        self._control_file = control_file
        self._value = 0
        self._connections = []

    def get_control_file(self):
        return self._control_file

    def get_pid(self):
        return os.getpid()

    def exit(self):
        self._stop()

    def set_value(self, value):
        self._value = value

    def get_value(self):
        return self._value

    def get_connections(self):
        return self._connections

    def exception(self):
        raise FunkyException("Test exception")

    def iterate(self):
        def x():
            for i in range(10):
                yield self._value
                self._value += 1
        return service.IteratorWrapper(x())

@nottest
class T(S):
    _timeout = 1.5

    def set_path(self, path):
        self._path = path

    def __exit__(self, exc_type, exc_val, ex_tb):
        with self._path.open("wb") as fp:
            pickle.dump(exc_val, fp)
        if exc_type == TimeoutError:
            return True # Supress exception


@nottest
class U(S):
    @staticmethod
    def _connection_class(*args, **kwargs):
        raise FunkyException("abc")

@nottest
class V(S):
    @staticmethod
    def _connection_class(instance, address):
        raise Exception(Boom())

@nottest
class Boom:
    def __getstate__(self):
        raise FunkyException()

@nottest
@contextlib.contextmanager
def connection_helper(connection):
    pid = connection.get_pid()
    try:
        assert pid != os.getpid()
        yield connection
    finally:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass # The process was already stopped

@nottest
@contextlib.contextmanager
def chdir(d):
    original = os.getcwd()
    os.chdir(str(d))
    try:
        yield
    finally:
        os.chdir(original)

def basic_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file1 = tmp / "ctrl1"
        assert not control_file1.exists()

        print("Opening service proxy s1a")
        with service.ServiceProxy(S, control_file1) as s1a:
            stack.enter_context(connection_helper(s1a))
            eq_(s1a.get_control_file(), control_file1)
            assert s1a.get_pid() != os.getpid()
            s1a.set_value(5)

            with assert_raises(FunkyException):
                s1a.exception()

            control_file2 = tmp / "ctrl2"
            print("Opening service proxy s2")
            with service.ServiceProxy(S, control_file2) as s2:
                stack.enter_context(connection_helper(s2))
                eq_(s2.get_control_file(), control_file2)
                assert s2.get_pid() != os.getpid()
                assert s2.get_pid() != s1a.get_pid()
                s2.set_value(2)
                eq_(s1a.get_value(), 5)
                eq_(s2.get_value(), 2)

            print("Opening service proxy s1b")
            with service.ServiceProxy(S, control_file1) as s1b:
                stack.enter_context(connection_helper(s1a))
                eq_(s1b.get_control_file(), control_file1)
                assert s1a.get_pid() != os.getpid()
                eq_(s1a.get_pid(), s1b.get_pid())
                eq_(s1b.get_value(), 5)

                s1a.exit()
                time.sleep(0.5)
                assert not control_file1.exists()
                with assert_raises(Exception):
                    s1a.get_pid()
                with assert_raises(Exception):
                    s1b.get_pid()
        # s2 gets destroyed by the context manager

def another_process_test():
    def set_value(control_file):
        with service.ServiceProxy(S, control_file) as s:
            s.set_value(99)

    def check_value(control_file):
        with service.ServiceProxy(S, control_file) as s:
            eq_(s.get_value(), 99)
            s.exit()

    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))
        control_file = tmp/"control_file"

        p1 = multiprocessing.Process(target=set_value, args=(control_file,))
        p1.start()
        p1.join()

        assert control_file.exists()

        p2 = multiprocessing.Process(target=check_value, args=(control_file,))
        p2.start()
        p2.join()

        time.sleep(0.5)
        assert not control_file.exists()

def timeout_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file = tmp / "ctrl"
        out_file = tmp / "out"
        assert not control_file.exists()

        print("Opening service proxy s")
        with service.ServiceProxy(T, control_file) as s:
            stack.enter_context(connection_helper(s))

            s.set_path(out_file)

            time.sleep(2.0)

            with out_file.open("rb") as fp:
                loaded = pickle.load(fp)
                assert isinstance(loaded, TimeoutError)

            with assert_raises(Exception):
                s.get_pid()
            assert not control_file.exists()

def iterator_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file = tmp / "ctrl"

        s = stack.enter_context(service.ServiceProxy(S, control_file))
        stack.enter_context(connection_helper(s))

        iterator = s.iterate()

        assert_sequence_equal(list(iterator), list(range(10)))

        eq_(s.get_value(), 10)

        with assert_raises(StopIteration):
            next(iterator)

        eq_(s.get_value(), 10)

def relative_path_test():
    """ Check that we can connect to the service even when specifying the control file as a relative path """
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        stack.enter_context(chdir(tmp))

        control_file = "ctrl"

        s = stack.enter_context(service.ServiceProxy(S, control_file))
        stack.enter_context(connection_helper(s))

        eq_(s.get_value(), 0)

def force_reload_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file = tmp / "ctrl"

        s1 = stack.enter_context(service.ServiceProxy(S, control_file, force_restart = True))
        stack.enter_context(connection_helper(s1))
        s1.set_value(100)
        eq_(s1.get_value(), 100)

        s2 = stack.enter_context(service.ServiceProxy(S, control_file, force_restart = True))
        stack.enter_context(connection_helper(s2))

        with assert_raises(Exception):
            s1.get_value()
        eq_(s2.get_value(), 0)
        with assert_raises(Exception):
            s1.get_value()
        eq_(s2.get_value(), 0)

def client_class_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file = tmp / "ctrl"

        with service.ServiceProxy(S, control_file) as s1:
            eq_(len(s1.get_connections()), 1)
            with service.ServiceProxy(S, control_file) as s2:
                # s2 connections must be accessed first here, because
                # the connection object is initialized during a first function call
                # -- during s2.get_connections(). s1 will then see it without problems.
                eq_(len(s2.get_connections()), 2)
                eq_(len(s1.get_connections()), 2)
            time.sleep(0.5) # Give s2 time to close the connection
            eq_(len(s1.get_connections()), 1)

def invalid_client_class_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file = tmp / "ctrl"

        with service.ServiceProxy(U, control_file) as s:
            with assert_raises(FunkyException):
                s.get_pid()

def exception_in_exception_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file = tmp / "ctrl"

        with service.ServiceProxy(V, control_file) as s:
            with assert_raises(FunkyException):
                s.get_pid()
