import hashlib
import mmap

def sha1_file(path):
    """ Read a file (pathlib.Path) and return hash of its content. """

    hasher = hashlib.sha1()
    if path.stat().st_size > 0:
        with path.open("rb") as fp:
            mm = mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ)
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

