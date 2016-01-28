import hashlib
import mmap
import contextlib
import time

@contextlib.contextmanager
def mmap_file(path):
    """ Open a file and mmap it as read only.
    If the file is empty, then returns empty bytes."""
    if path.stat().st_size == 0:
        yield b""
    else:
        with path.open("rb") as fp:
            with mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                yield mm

def sha1_iterable(*iterables):
    """ Return a hash of all elements in iterable, each followed by a null byte.
    Iterable must contain bytes (used as is), or strings (encoded to utf-8)"""
    hasher = hashlib.sha1()
    for iterable in iterables:
        for item in iterable:
            try:
                hasher.update(item)
            except TypeError:
                hasher.update(repr(item).encode("utf8"))
            hasher.update(b"\0")
    return hasher.digest()

def sha1_file(path):
    """ Read a file (pathlib.Path) and return hash of its content. """
    hasher = hashlib.sha1()
    with mmap_file(path) as mm:
        hasher.update(mm)
    return hasher.digest()

def maybe_iterable(val):
    if isinstance(val, str) or isinstance(val, bytes):
        return [val]

    try:
        iter(val)
    except:
        return [val]
    else:
        return val

class Timer:
    def __init__(self, ewma_smoothing = 0.5, include_exceptions = False):
        self.time = None
        self._start_time = None
        self._smoothing = ewma_smoothing
        self._include_exceptions = include_exceptions

    def __enter__(self):
        self._start_time = time.perf_counter()

    def __exit__(self, ex_type, ex_val, ex_tb):
        if ex_type is None or self._include_exceptions:
            elapsed = time.perf_counter() - self._start_time
            if self.time is None:
                self.time = elapsed
            else:
                self.time = self._smoothing * self.time + (1 - self._smoothing) * elapsed

def synchronized(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return f(self, *args, **kwargs)
    return wrapper
