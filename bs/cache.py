import binascii
import collections
import shutil
import pickle

_Item = collections.namedtuple("_Item", "size partial_hash implicit_dependencies")

class Cache:
    """ Caches all output files of a single application + its computed implicit
    dependencies. """

    _save_filename = "metadata.pickle"

    def __init__(self, directory, size_limit = 1000000000):
        self.directory = directory
        self.size_limit = size_limit
        self.clear(False)

    def __enter__(self):
        # Size limit is infinite for loading, we will restore it afterwards.
        size_limit = self.size_limit
        self.size_limit = float("inf")

        if not self._load():
            self.clear()
        self.size_limit = size_limit
        return self

    def __exit__(self, *exc_info):
        self.save()

    def clear(self, delete_directory = True):
        self.size_used = 0
        self._data = collections.OrderedDict()
            # MRU order
            # Key: full hash of application
            # Value: _Item

        self._partial_hashes = {}
            # Key: Partial hash
            # Value: list of full hashes

        if delete_directory:
            try:
                shutil.rmtree(str(self.directory))
            except FileNotFoundError:
                pass

    def put(self, final_hash, partial_hash, paths, implicit_dependencies):
        """ Add files to cache.
        Moves the paths to the correct directory in cache. """

        assert final_hash not in self._data
        assert final_hash not in self._partial_hashes.get(partial_hash, [])

        size = sum(path.stat().st_size for path in paths)

        self._data[final_hash] = _Item(size, partial_hash, implicit_dependencies)
        self._partial_hashes.setdefault(partial_hash, []).append(final_hash)

        self._reserve_space(size)

        directory = self.get_directory(final_hash)
        assert not directory.exists()
        directory.mkdir(parents=True)

        for path in paths:
            path.rename(directory / path.name)
        self.size_used += size

    def get_candidate_implicit_dependencies(self, partial_hash):
        """ Return list of possible implicit dependencies. """
        try:
            candidate_hashes = self._partial_hashes[partial_hash]
        except KeyError:
            return []

        return [self._data[h].implicit_dependencies for h in candidate_hashes]

    def accessed(self, final_hash):
        assert final_hash in self._data
        self._data.move_to_end(final_hash)

    def _reserve_space(self, size):
        """ Make sure there is at least size space in the cache available """
        while self.size_used + size > self.size_limit:
            if not len(self._data):
                raise RuntimeError("The cache is too small")
            self._discard_one()

    def _discard_one(self):
        final_hash, item = self._data.popitem(last=False)
        self._partial_hashes[item.partial_hash].remove(final_hash)
        if not self._partial_hashes[item.partial_hash]:
            del self._partial_hashes[item.partial_hash]

        shutil.rmtree(str(self.get_directory(final_hash)))
        self.size_used -= item.size

    def get_directory(self, final_hash):
        h = binascii.hexlify(final_hash).decode("ascii")
        # I don't think that separating the hash by the first byte is strictly necessary,
        # but hey, git does it too :-)
        return self.directory / h[:2] / h[2:]

    def save(self):
        """ Save cache metadata to a file in the cache directory. """
        if len(self._data) == 0:
            # There is no point in saving empty cache and we could get an error
            # because of nonexistent cache directory
            return

        with (self.directory / self._save_filename).open("wb") as fp:
            pickler = pickle.Pickler(fp)
            pickler.dump(1), # Version
            pickler.dump(self.size_used)
            pickler.dump(self._data)
            pickler.dump(self._partial_hashes)

    def _load(self):
        """ Try to load metadata from a file in the cache directory.
        Returns true if the load succeeded. """
        save_path = self.directory / self._save_filename
        try:
            fp = save_path.open("rb")
        except FileNotFoundError:
            return False

        with fp:
            try:
                unpickler = pickle.Unpickler(fp)
                version = unpickler.load()
                self.size_used = unpickler.load()
                self._data = unpickler.load()
                self._partial_hashes = unpickler.load()
            except pickle.PickleError:
                return False
            finally:
                save_path.unlink()

        return self.verify_state()

    def verify_state(self):
        """ Verifies the internal state invariants, returns True if state is valid. """

        accessible_full_hashes = {}
        for partial_hash, full_hashes in self._partial_hashes.items():
            if not full_hashes:
                #print(1)
                return False # Every partial hash stored needs at least one corresponding full hash

            if len(full_hashes) != len(set(full_hashes)):
                #print(2)
                return False # There can't be any duplicities in links from partial to full hashes

            for full_hash in full_hashes:
                try:
                    if self._data[full_hash].partial_hash != partial_hash:
                        #print(3)
                        return False # If a partial hash is linking to a full hash, it must link back
                except KeyError:
                    #print(4)
                    return False # There must be a corresponding full hash record if a partial hash links to it

                accessible_full_hashes[full_hash] = partial_hash

        owned_directories = set()
        for full_hash, item in self._data.items():
            if full_hash not in accessible_full_hashes:
                #print(5)
                return False # Every full hash has at least one partial hash pointing to it

            owned_directories.add(self.get_directory(full_hash))

        def check_paths(root, in_cache):
            if root in owned_directories:
                in_cache = True

            have_subdir = False
            size = 0

            for p in root.iterdir():
                if p.is_dir():
                    child_valid, child_size = check_paths(p, in_cache)
                    if not child_valid:
                        return False, 0
                    size += child_size
                    have_subdir = True
                elif in_cache:
                    size += p.stat().st_size
                else:
                    return False, 0

            return (in_cache or have_subdir), size

        if self.directory.exists():
            valid, size = check_paths(self.directory, False)
            if not valid:
                #print(6)
                return False # Invalid file or directory encounted
        else:
            size = 0

        if size != self.size_used:
            #print(7)
            return False # Calculated size must be equal to real file size

        if size > self.size_limit:
            #print(8)
            return False # Size exceeds the size limit

        return True
