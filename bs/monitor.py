from . import util

import pathlib
import threading
import functools
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class Monitor:
    def __init__(self):
        self._observer = Observer()
        self._handler = _EventHandler(self)
        self._watches = []

        self._old_state = {}
        self._changed = {}

    def __enter__(self):
        self._observer.start()
        return self

    def __exit__(self, *args):
        self._observer.stop()
        self._observer.join()

    @util.synchronized
    def _examine_path(self, path):
        try:
            path = pathlib.Path(path).resolve()
        except FileNotFoundError:
            hash = None
        else:
            hash = util.sha1_file(path)

        if hash == self._old_state.get(path, None):
            if path in self._changed:
                del self._changed[path]
        else:
            self._changed[path] = hash

    @util.synchronized
    def watch(self, path, recursive = False):
        self._observer.schedule(self._handler, path, recursive)

        path = pathlib.Path(path).resolve()

        if not recursive:
            self._old_state[path] = util.sha1_file(path)
        else:
            for (dirpath, dirnames, filenames) in os.walk(str(path)):
                for filename in filenames:
                    fullpath = pathlib.Path(dirpath) / filename
                    self._old_state[fullpath] = util.sha1_file(fullpath)

    @util.synchronized
    def update(self):
        ret = list(self._changed.keys())

        for path, hash in self._changed.items():
            self._old_state[path] = hash
        self._changed.clear()

        return ret

class _EventHandler(FileSystemEventHandler):
    def __init__(self, monitor):
        self._monitor = monitor

    def on_any_event(self, event):
        if event.is_directory:
            return
        self._monitor._examine_path(event.src_path)
        try:
            dest_path = event.dest_path
        except AttributeError:
            pass
        else:
            self._monitor._examine_path(dest_path)

with Monitor() as m:
    m.watch("/home/cube/Factorio/src", True)
    m.watch("/home/cube/Factorio/libraries", True)
    while True:
        input()
        print(m.update())

