from . import util

import json
import os
import sys
import pickle
import time
import threading
import multiprocessing
import socket
import socketserver
import logging
import traceback
import pathlib

logger = logging.getLogger(__name__)

class Service:
    """ Base class for a service that runs as a background process and makes all
    of its non underscore methods available using pickle-over-socket RPC interface.
    A service is identified by a simple json file that contains its PID and
    port number.
    Service is a context manager. Entered when service starts, exited when it stops.

    Instance variables:
    _timeout -- Must be set by subclass. After this many seconds without any request
                the server will raise TimeoutError and stop. None means no limit.
    _last_call_time -- Time of last RPC call.
    _server -- SocketServer subclass that handles the connection
    _lock -- Lock that protects all method calls.
    _client_address -- (ip, port) tuple of client that calls the current function
    """

    def __init__(self, control_file):
        """ Initialize the server of this service.
        Argument `control_file` contains path to the control file and is not used
        in the default implementation. """

    def __enter__(self):
        """ To be overridden """

    def __exit__(self, *exc):
        """ To be overridden """

    def _stop(self):
        """ Exit the main loop. Intended to be called by subclasses. """
        #logger.info("Service stop requested.")
        threading.Thread(target=self._server.shutdown, daemon=True).start()


class ServiceProxy:
    def __init__(self, cls, control_file, force_restart=False):
        self._cls = cls
        self._control_file = util.make_absolute(pathlib.Path(control_file))
        self._socket = None
        self._rfile = None
        self._wfile = None
        self._force_restart = force_restart

    def __enter__(self):
        """ Connect to the service, start it if not already running.
        Returns proxy for the service. """

        try:
            self._try_connect(0.5)
            if self._socket is not None and self._force_restart:
                logger.info("Stopping service %s with control file %s (forced restart)",
                            self._cls.__name__,
                            self._control_file)
                self._call("_stop")
                self._close()
                time.sleep(0.5)
            if self._socket is None:
                logger.info("Starting service %s with control file %s",
                            self._cls.__name__,
                            self._control_file)
                process = multiprocessing.Process(target=_run, args=(self._cls,
                                                                     self._control_file))
                process.start()
                # The process will fork and exit, we can join it immediately
                process.join()
                self._try_connect(1.5)
                if self._socket is None:
                    raise Exception("Failed to start the service")

            return self
        except:
            self._close()
            raise

    def __exit__(self, *ex):
        self._close()

    def __getattr__(self, name):
        def func(*args, **kwargs):
            return self._call(name, *args, **kwargs)
        return func

    def _call(self, name, *args, **kwargs):
        pickle.dump((name, args, kwargs), self._wfile)
        result, exc_info = pickle.load(self._rfile)
        if exc_info is None:
            if isinstance(result, IteratorWrapper):
                result._proxy = self
            return result
        else:
            raise exc_info[1] from Exception("Original traceback:\n" +"".join(traceback.format_list(exc_info[2])))

    def _open(self, port):
        self._socket = socket.create_connection(("localhost", port))
        self._rfile = self._socket.makefile("rb", -1)
        self._wfile = self._socket.makefile("wb", 0)

    def _close(self):
        if self._wfile:
            self._wfile.close()
            self._wfile = None
        if self._rfile:
            self._rfile.close()
            self._rfile = None
        if self._socket:
            self._socket.close()
            self._socket = None

    def _try_connect(self, timeout):
        end_time = time.time() + timeout
        while True:
            if self._try_connect_once():
                return
            if time.time() > end_time:
                return
            time.sleep(0.1)

    def _try_connect_once(self):
        try:
            with self._control_file.open("r") as fp:
                loaded = json.load(fp)
        except FileNotFoundError:
            return False
        except ValueError as e:
            return False

        try:
            self._open(loaded["port"])
        except ConnectionRefusedError:
            return False

        assert self._socket is not None
        return True

class IteratorWrapper:
    """ Class that marks wrapped iterators. These are iterated in the service and
    only their results are transfered. """
    def __init__(self, it):
        self.it = iter(it)
        self._proxy = None

    def __iter__(self):
        return self

    def __next__(self):
        return self._proxy._call(self.it)


class _PickleRPCServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, instance):
        self.instance = instance
        super().__init__(address, _PickleRPCRequestHandler)

    def service_actions(self):
        super().service_actions()

        with self.instance._lock:
            if self.instance._timeout is None or \
               time.time() <= self.instance._last_call_time + self.instance._timeout:
                return
            raise TimeoutError("Timed out waiting for RPC calls ({} > {} + {})".format(
                                 time.time(),
                                 self.instance._last_call_time,
                                 self.instance._timeout))

class _PickleRPCRequestHandler(socketserver.StreamRequestHandler):
    def handle(self):
        iterators = {}
        while True:
            try:
                func_name, args, kwargs = pickle.load(self.rfile)

                with self.server.instance._lock:
                    self.server.instance._client_address = self.client_address
                    if func_name in iterators:
                        assert len(args) == 0
                        assert len(kwargs) == 0
                        result = next(iterators[func_name])
                    else:
                        func = getattr(self.server.instance, func_name)
                        result = func(*args, **kwargs)

                    if isinstance(result, IteratorWrapper):
                        iterator_id = "!" + str(id(result.it))
                        iterators[iterator_id] = result.it # TODO: Now we are memory leaking exhausted iterators
                        result.it = iterator_id
                    self.server.instance._last_call_time = time.time()
            except:
                ex_type, ex_value, ex_tb = sys.exc_info()
                pickle.dump((None, (ex_type, ex_value, traceback.extract_tb(ex_tb))), self.wfile)
                # TODO: Pass traceback for exceptions as well
            else:
                pickle.dump((result, None), self.wfile)

def _run(cls, control_file):
    """ The actual code run by the service.
    This always runs in another process. """

    _daemonize()

    try:
        with control_file.open("w") as fp:
            instance = cls(control_file)

            instance._server = _PickleRPCServer(("localhost", 0), instance)
            instance._last_call_time = time.time()
            instance._lock = threading.Lock()

            json.dump({"pid": os.getpid(),
                       "port": instance._server.socket.getsockname()[1]},
                      fp)

        with instance:
            instance._server.serve_forever()

    except Exception as e:
        with (control_file.parent / "service_error").open("w") as fp:
            traceback.print_exc(file=fp)
    finally:
        control_file.unlink()

def _daemonize():
    #TODO: The following part is unix only.
    # After it finishes current process should be reasonably daemonized
    os.chdir("/")
    if os.fork():
        sys.exit(0)
    os.setsid()

    sys.stdout.close()
    os.close(0)
    sys.stderr.close()
    os.close(1)
    sys.stdin.close()
    os.close(2)
