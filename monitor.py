import pathlib
import hashlib
import threading
import functools
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

def _synchronized(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            _ = self._lock
        except AttributeError:
            self._lock = threading.Lock()

        with self._lock:
            return f(self, *args, **kwargs)
    return wrapper

class Monitor:
    def __init__(self):
        self._observer = Observer()
        self._handler = _EventHandler(self)
        self._watches = []

        self._old_state = {}
        self._changed = {}

        lock = threading.Lock()

    def __enter__(self):
        self._observer.start()
        return self

    def __exit__(self, *args):
        self._observer.stop()
        self._observer.join()

    @staticmethod
    def _get_hash(path):
        hasher = hashlib.sha1()
        try:
            with path.open("rb") as fp:
                print("Hashing", path)
                while True:
                    block = fp.read(4096)
                    if len(block) == 0:
                        break
                    else:
                        hasher.update(block)

            return hasher.digest()
        except OSError:
            print(path, "doesn't exist")
            return None

    @_synchronized
    def _examine_path(self, path):
        path = pathlib.Path(path).resolve()
        hash = self._get_hash(path)

        if hash == self._old_state.get(path, None):
            if path in self._changed:
                del self._changed[path]
        else:
            self._changed[path] = hash

    @_synchronized
    def watch(self, path, recursive = False):
        self._observer.schedule(self._handler, path, recursive)

        path = pathlib.Path(path).resolve()

        if not recursive:
            self._old_state[path] = self._get_hash(path)
        else:
            for (dirpath, dirnames, filenames) in os.walk(str(path)):
                for filename in filenames:
                    fullpath = pathlib.Path(dirpath) / filename
                    self._old_state[fullpath] = self._get_hash(fullpath)

    @_synchronized
    def update(self):
        ret = list(self._changed.keys())

        for path, hash in self._changed.items():
            self._old_state[path] = hash
        self._changed.clear()

        return ret

class _EventHandler(FileSystemEventHandler):
    def __init__(self, monitor):
        self._monitor = monitor

    def on_created(self, event):
        if event.is_directory:
            return
        self._monitor._examine_path(event.src_path)

    def on_deleted(self, event):
        if event.is_directory:
            return
        self._monitor._examine_path(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        self._monitor._examine_path(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self._monitor._examine_path(event.src_path)
        self._monitor._examine_path(event.desct_path)


with Monitor() as m:
    m.watch("/home/cube/Factorio/src", True)
    m.watch("/home/cube/Factorio/libraries", True)
    while True:
        input()
        print(m.update())

