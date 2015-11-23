from nose.tools import *
import os
import tempfile
import pathlib

import bs.util

def sha1_file_test():
    with tempfile.TemporaryDirectory() as d:
        def check(filename, content, sha1):
            path = os.path.join(d, filename)
            with open(path, "w") as fp:
                fp.write(content)
            eq_(bs.util.sha1_file(pathlib.Path(path)), bytearray.fromhex(sha1))

        check("empty.txt", "", "da39a3ee5e6b4b0d3255bfef95601890afd80709")
        check("abc.txt", "abc", "a9993e364706816aba3e25717850c26c9cd0d89d")

def sha1_iterable_test():
    hashes = set()
    def check_unique(*args):
        nonlocal hashes
        h = bs.util.sha1_iterable(*args)
        assert h not in hashes
        hashes.add(h)

    list1 = [1, "a", b"b"]
    list2 = list(range(5))

    check_unique([])
    check_unique(list1)
    check_unique(reversed(list1))
    check_unique(list1 + ["a"])
    check_unique(iter(list2))
    check_unique(list1, list2)
    check_unique([None])
    check_unique(["None"])
    check_unique(map(str, list2))

    eq_(bs.util.sha1_iterable(list1, list2), bs.util.sha1_iterable(list1 + list2))

def maybe_iterable_test():
    assert_sequence_equal(bs.util.maybe_iterable("a"), ["a"])
    assert_sequence_equal(bs.util.maybe_iterable(b"a"), [b"a"])
    assert_sequence_equal(bs.util.maybe_iterable([1]), [1])
    assert_sequence_equal(bs.util.maybe_iterable(range(3)), [0, 1, 2])
    assert_sequence_equal(bs.util.maybe_iterable(1), [1])
