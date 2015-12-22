import json
import multiprocessing
import os
import sys
import xmlrpc.server
import time
import threading

class Service:
    """ Base class for a service that runs as a background process and makes all
    of its non underscore methods other than `connect` available using XMLRPC interface.
    A service is identified by a simple json file that contains its PID and
    port number. """

    @classmethod
    def connect(cls, control_file):
        """ Connect to the service, start it if not already running.
        Returns proxy for the service. """

        ret = _try_connect(control_file, 0.5)
        if not ret:
            process = multiprocessing.Process(target=_run, args=(cls, control_file))
            process.start()
            # The process will fork and exit, we can join it immediately
            process.join()
            ret = _try_connect(control_file, 1.5)
            if not ret:
                raise Exception("Failed to start the service")

        return ret

    def __init__(self, control_file):
        """ Initialize the server of this service.
        Argument `control_file` contains path to the control file and is not used
        in the default implementation. """

    def _exit(self):
        """ Exit the main loop. Intended to be called by subclasses. """
        # Starts the thread that only stops the server and immediately exits
        threading.Thread(target=self._server.shutdown).start()

    def _dispatch(self, method, params):
        if method.startswith("_"):
            raise NameError("Underscore names are not forwarded from service")

        return getattr(self, method)(*params)


def _try_connect(control_file, timeout):
    end_time = time.time() + timeout
    while True:
        try:
            with control_file.open("r") as fp:
                loaded = json.load(fp)
        except FileNotFoundError:
            pass
        except ValueError as e:
            pass
        else:
            proxy = xmlrpc.client.ServerProxy("http://localhost:{}".format(loaded["port"]))
            return proxy

        time.sleep(0.1)

        if time.time() > end_time:
            return False

def _run(cls, control_file):
    """ The actual code run by the service.
    This always runs in another process. """

    _daemonize()

    try:
        with control_file.open("w") as fp:
            instance = cls(control_file)

            instance._server = xmlrpc.server.SimpleXMLRPCServer(("localhost", 0), allow_none=True)
            instance._server.register_instance(instance)

            json.dump({"pid": os.getpid(),
                       "port": instance._server.socket.getsockname()[1]},
                      fp)

        instance._server.serve_forever()
    finally:
        control_file.unlink()

def _daemonize():
    #TODO: The following part is unix only.
    # After it finishes current process should be reasonably daemonized
    os.chdir("/")
    if os.fork():
        sys.exit(0)
    os.setsid()
