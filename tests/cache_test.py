from nose.tools import *
import contextlib
import tempfile
import shutil
import pathlib

from bs import cache

@nottest
@contextlib.contextmanager
def cache_fixture():
    directory = pathlib.Path(tempfile.mkdtemp(prefix="test_cache.", suffix=""))
    c = cache.Cache(pathlib.Path(directory), size_limit = 10)

    try:
        yield c
    finally:
        shutil.rmtree(str(directory))

@nottest
def file_name(i):
    return "file{:04d}".format(i)

@nottest
@contextlib.contextmanager
def make_files(count):
    files = []
    directory = pathlib.Path(tempfile.mkdtemp(prefix="test_file_creation_area.", suffix=""))

    try:
        for i in range(count):
            path = directory / file_name(i)
            with path.open("w") as fp:
                fp.write("X")
            files.append(path)
        yield files
    finally:
        shutil.rmtree(str(directory))

@nottest
def check_files(directory, count):
    """ Check that the directory contains exactly files created by make_files(count) """

    for i, path in enumerate(sorted(directory.iterdir())):
        eq_(path.name, file_name(i))
        with path.open("r") as fp:
            eq_(fp.read(), "X")

def simple_test():
    with cache_fixture() as c:
        with make_files(2) as files:
            c.put(b"final", b"partial", files, [])

        check_files(c.get_directory(b"final"), 2)
        eq_(c.get_candidate_implicit_dependencies(b"partial"), [[]])
        eq_(c.get_candidate_implicit_dependencies(b"unknown"), [])

def implicit_deps_test():
    with cache_fixture() as c:
        c.put(b"final-1-a", b"partial-1", [], [("file1", b"version1")])
        c.put(b"final-1-b", b"partial-1", [], [("file1", b"version2"), ("file2", b"version1")])
        c.put(b"final-1-c", b"partial-1", [], [("file1", b"version2"), ("file2", b"version2")])
        c.put(b"final-2-a", b"partial-2", [], [("file1", b"version1"), ("file2", b"version1")])
        c.put(b"final-2-b", b"partial-2", [], [("file1", b"version2"), ("file2", b"version1")])

        eq_(c.get_candidate_implicit_dependencies(b"partial-1"),
            [[("file1", b"version1")],
             [("file1", b"version2"), ("file2", b"version1")],
             [("file1", b"version2"), ("file2", b"version2")]])
        eq_(c.get_candidate_implicit_dependencies(b"partial-2"),
            [[("file1", b"version1"), ("file2", b"version1")],
             [("file1", b"version2"), ("file2", b"version1")]])

def dropping_test():
    with cache_fixture() as c:
        for i in range(5):
            with make_files(2) as files:
                c.put("final-{}".format(i).encode("ascii"),
                      b"partial",
                      files,
                      i) # It's slightly hacky to add just an integer instead of the list of dependencies, but the cache shouldn't care

        eq_(set(c.get_candidate_implicit_dependencies(b"partial")), set(range(5)))
        c.hit(b"final-0")
        eq_(set(c.get_candidate_implicit_dependencies(b"partial")), set(range(5)))

        for i in range(5, 9):
            with make_files(2) as files:
                c.put("final-{}".format(i).encode("ascii"),
                      b"partial",
                      files,
                      i)

        eq_(set(c.get_candidate_implicit_dependencies(b"partial")), {0, 8, 7, 6, 5})

def too_large_test():
    with cache_fixture() as c:
        with make_files(20) as files:
            with assert_raises(Exception):
                c.put(b"final", b"partial", files, [])
