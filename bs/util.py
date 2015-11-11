import hashlib
import mmap
import contextlib

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
                hasher.update(str(item).encode("utf-8"))
            hasher.update(b"\0")
    return hasher.digest()

def sha1_file(path):
    """ Read a file (pathlib.Path) and return hash of its content. """
    hasher = hashlib.sha1()
    with mmap_file(path) as mm:
        hasher.update(mm)
    return hasher.digest()

# This version is simpler, but consistently slower by about 25%
#def sha1_file(path, blocksize=4096):
#    hasher = hashlib.sha1()
#    with path.open("rb") as fp:
#        while True:
#            block = fp.read(4096)
#            if len(block) == 0:
#                break
#            else:
#                hasher.update(block)
#
#    return hasher.digest()
