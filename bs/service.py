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
        logger.info("Service stop requested.")
        threading.Thread(target=self._server.shutdown, daemon=True).start()


class ServiceProxy:
    def __init__(self, cls, control_file):
        self._cls = cls
        self._control_file = control_file
        self._socket = None
        self._rfile = None
        self._wfile = None

    def __enter__(self):
        """ Connect to the service, start it if not already running.
        Returns proxy for the service. """

        try:
            self._try_connect(0.5)
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
            pickle.dump((name, args, kwargs), self._wfile)
            result, exc_info = pickle.load(self._rfile)
            if exc_info is None:
                return result
            else:
                raise exc_info[1]

        return func

    def _open(self, port):
        self._socket = socket.create_connection(("localhost", port))
        self._rfile = self._socket.makefile("rb", -1)
        self._wfile = self._socket.makefile("wb", 0)

    def _close(self):
        if self._wfile:
            self._wfile.close()
        if self._rfile:
            self._rfile.close()
        if self._socket:
            self._socket.close()

    def _try_connect(self, timeout):
        end_time = time.time() + timeout
        while True:
            try:
                with self._control_file.open("r") as fp:
                    loaded = json.load(fp)
            except FileNotFoundError:
                pass
            except ValueError as e:
                pass
            else:
                self._open(loaded["port"])

            time.sleep(0.1)

            if time.time() > end_time:
                return

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
            logger.info("Timed out waiting for RPC calls")
            raise TimeoutError("Timed out waiting for RPC calls ({} > {} + {})".format(
                                 time.time(),
                                 self.instance._last_call_time,
                                 self.instance._timeout))

class _PickleRPCRequestHandler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            try:
                func_name, args, kwargs = pickle.load(self.rfile)
            except EOFError:
                break

            try:
                if func_name.startswith("_"):
                    raise NameError("Underscore names are not forwarded from service")

                func = getattr(self.server.instance, func_name)
                with self.server.instance._lock:
                    self.server.instance._client_address = self.client_address
                    result = func(*args, **kwargs)
                    self.server.instance._last_call_time = time.time()
            except:
                ex_type, ex_value, ex_tb = sys.exc_info()
                pickle.dump((None, (ex_type, ex_value, None)), self.wfile)
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
        logger.exception("Unhandled exception in service")
    finally:
        control_file.unlink()

def _daemonize():
    #TODO: The following part is unix only.
    # After it finishes current process should be reasonably daemonized
    os.chdir("/")
    if os.fork():
        sys.exit(0)
    os.setsid()
