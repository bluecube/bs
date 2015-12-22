from nose.tools import *
import tempfile
import pathlib
from bs import service
import os
import contextlib
import signal
import time
import multiprocessing

class S(service.Service):
    def __init__(self, control_file):
        self._control_file = control_file
        self._value = 0

    def get_control_file(self):
        return str(self._control_file)

    def get_pid(self):
        return os.getpid()

    def exit(self):
        self._exit()

    def set_value(self, value):
        self._value = value

    def get_value(self):
        return self._value

@nottest
@contextlib.contextmanager
def connection_helper(control_file):
    connection  = S.connect(control_file)

    pid = connection.get_pid()
    assert pid != os.getpid()
    try:
        yield connection
    finally:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass # The process was already stopped


def basic_test():
    with contextlib.ExitStack() as stack:
        tmp = pathlib.Path(stack.enter_context(tempfile.TemporaryDirectory()))

        control_file1 = tmp / "ctrl1"

        assert not control_file1.exists()

        s1a = stack.enter_context(connection_helper(control_file1))
        eq_(s1a.get_control_file(), str(control_file1))
        assert s1a.get_pid() != os.getpid()
        s1a.set_value(5)

        s1b = stack.enter_context(connection_helper(control_file1))
        eq_(s1b.get_control_file(), str(control_file1))
        assert s1a.get_pid() != os.getpid()
        eq_(s1a.get_pid(), s1b.get_pid())
        eq_(s1b.get_value(), 5)

        control_file2 = tmp / "ctrl2"
        s2 = stack.enter_context(connection_helper(control_file2))
        eq_(s2.get_control_file(), str(control_file2))
        assert s2.get_pid() != os.getpid()
        assert s2.get_pid() != s1a.get_pid()
        s2.set_value(2)
        eq_(s1a.get_value(), 5)
        eq_(s2.get_value(), 2)

        s1a.exit()
        time.sleep(0.1)
        assert not control_file1.exists()
        with assert_raises(Exception):
            s1a.get_pid()
        with assert_raises(Exception):
            s1b.get_pid()

        eq_(s2.get_value(), 2)

        # s2 gets destroyed by the context manager

def another_process_test():
    def set_value(control_file):
        service = S.connect(control_file)
        service.set_value(99)

    def check_value(control_file):
        service = S.connect(control_file)
        eq_(service.get_value(), 99)
        service.exit()

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

        assert not control_file.exists()
