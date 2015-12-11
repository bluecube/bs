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
        assert c.verify_state()
    finally:
        try:
            shutil.rmtree(str(directory))
        except FileNotFoundError:
            pass

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

@nottest
def create_data(c):
    with make_files(2) as files:
        c.put(b"final-1-a", b"partial-1", files, [("file1", b"version1")])
    with make_files(2) as files:
        c.put(b"final-1-b", b"partial-1", files, [("file1", b"version2"), ("file2", b"version1")])
    with make_files(2) as files:
        c.put(b"final-1-c", b"partial-1", files, [("file1", b"version2"), ("file2", b"version2")])
    with make_files(2) as files:
        c.put(b"final-2-a", b"partial-2", files, [("file1", b"version1"), ("file2", b"version1")])
    with make_files(2) as files:
        c.put(b"final-2-b", b"partial-2", files, [("file1", b"version2"), ("file2", b"version1")])

@nottest
def check_data(c):
    eq_(c.get_candidate_implicit_dependencies(b"partial-1"),
        [[("file1", b"version1")],
         [("file1", b"version2"), ("file2", b"version1")],
         [("file1", b"version2"), ("file2", b"version2")]])
    eq_(c.get_candidate_implicit_dependencies(b"partial-2"),
        [[("file1", b"version1"), ("file2", b"version1")],
         [("file1", b"version2"), ("file2", b"version1")]])

def clear_test():
    """ Test clearing the cache and clearing it twice """
    with cache_fixture() as c:
        create_data(c)
        c.clear()
        eq_(c.get_candidate_implicit_dependencies(b"partial-1"), [])
        eq_(c.get_candidate_implicit_dependencies(b"partial-2"), [])
        c.clear()

def implicit_deps_test():
    with cache_fixture() as c:
        create_data(c)
        check_data(c)

def dropping_test():
    """ Test LRU dropping unused items """
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
    """ Test cache item larger than cache itself. """
    with cache_fixture() as c:
        with make_files(20) as files:
            with assert_raises(Exception):
                c.put(b"final", b"partial", files, [])

def save_load_test():
    with cache_fixture() as c:
        create_data(c) # Reuse the previous test data

        c.save() # At this point we forget that c existed and only reuse its directory

        d = cache.Cache(c.directory, 20) # Larger cache
        check_data(d)

        d.save() # Again

        e = cache.Cache(c.directory, 8) # Smaller than the original -- remove something after next insert
        check_data(e)
        e.put(b"final-2-c", b"partial-2", [], [("file1", b"version3"), ("file2", b"version1")])

        eq_(e.get_candidate_implicit_dependencies(b"partial-1"),
            [[("file1", b"version2"), ("file2", b"version1")],
             [("file1", b"version2"), ("file2", b"version2")]])
        eq_(e.get_candidate_implicit_dependencies(b"partial-2"),
            [[("file1", b"version1"), ("file2", b"version1")],
             [("file1", b"version2"), ("file2", b"version1")],
             [("file1", b"version3"), ("file2", b"version1")]])

        c.clear() # clear c manually -- it's internal state is corrupted by the caches created over it

        c.save() # This will do nothing

def load_error_test():
    """ Test loading damaged save file. """
    with cache_fixture() as c:
        create_data(c) # Reuse the previous test data

        c.save() # At this point we forget that c existed and only reuse its directory

        # damage the save
        with (c.directory / c._save_filename).open("wb") as fp:
            fp.write(b"damaged!")

        d = cache.Cache(c.directory, 10)

        eq_(d.get_candidate_implicit_dependencies(b"partial-1"), [])
        eq_(d.get_candidate_implicit_dependencies(b"partial-2"), [])

        c.clear() # clear c manually -- it's internal state is corrupted by the caches created over it

def verify_state_test():
    """ Tests all failure states of state verification. """
    with cache_fixture() as c:
        create_data(c)
        c._partial_hashes[b"x"] = []
        assert not c.verify_state() # 1
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        c._partial_hashes[b"partial-1"].append(b"final-1-a")
        assert not c.verify_state() # 2
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        c._data[b"final-1-a"] = c._data[b"final-1-a"]._replace(partial_hash=b"xxx")
        assert not c.verify_state() # 3
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        del c._data[b"final-1-a"]
        assert not c.verify_state() # 4
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        c._data[b"x"] = cache._Item(0, b"y", [])
        assert not c.verify_state() # 5
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        (c.directory / "x").touch()
        assert not c.verify_state() # 6-file
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        (c.directory / "x").mkdir()
        assert not c.verify_state() # 6-directory
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        with (c.get_directory(b"final-1-a") / "extra-size").open("w") as fp:
            fp.write("abc")
        assert not c.verify_state() # 7
        c.clear()

    with cache_fixture() as c:
        create_data(c)
        with (c.get_directory(b"final-1-a") / "extra-size").open("w") as fp:
            fp.write("abc")
        c.size_used += 3
        assert not c.verify_state() # 8
        c.clear()
