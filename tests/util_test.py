from nose.tools import *
import os
import tempfile
import pathlib

import bs.util

def check_file_sha1(directory, filename, content, sha1):
    path = os.path.join(directory, filename)
    with open(path, "w") as fp:
        fp.write(content)
    eq_(bs.util.sha1_file(pathlib.Path(path)), bytearray.fromhex(sha1))

def sha1_file_test():
    with tempfile.TemporaryDirectory() as d:
        check_file_sha1(d, "empty.txt", "", "da39a3ee5e6b4b0d3255bfef95601890afd80709")
        check_file_sha1(d, "abc.txt", "abc", "a9993e364706816aba3e25717850c26c9cd0d89d")
