import binascii
import collections
import shutil

class Cache:
    """ Caches all output files of a single application + its computed implicit
    dependencies. """

    Item = collections.namedtuple("Item", "size partial_hash implicit_dependencies")

    def __init__(self, directory, size_limit = 1000000000):
        self.directory = directory
        self.size_limit = size_limit
        self.size_used = 0
        self.data = collections.OrderedDict()
            # MRU order
            # Key: full hash of application
            # Value: Item

        self.possible_hashes = {}
            # Key: Partial hash
            # Value: list of full hashes

    def put(self, final_hash, partial_hash, paths, implicit_dependencies):
        """ Add files to cache.
        Moves the paths to the correct directory in cache. """

        assert final_hash not in self.data
        assert final_hash not in self.possible_hashes.get(partial_hash, [])

        size = sum(path.stat().st_size for path in paths)

        self.data[final_hash] = self.Item(size, partial_hash, implicit_dependencies)
        self.possible_hashes.setdefault(partial_hash, []).append(final_hash)

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
            candidate_hashes = self.possible_hashes[partial_hash]
        except KeyError:
            return []

        return [self.data[h].implicit_dependencies for h in candidate_hashes]

    def hit(self, final_hash):
        assert final_hash in self.data
        self.data.move_to_end(final_hash)

    def _reserve_space(self, size):
        """ Make sure there is at least size space in the cache available """
        while self.size_used + size > self.size_limit:
            if not len(self.data):
                raise RuntimeError("The cache is too small")
            self._discard_one()

    def _discard_one(self):
        final_hash, item = self.data.popitem(last=False)
        self.possible_hashes[item.partial_hash].remove(final_hash)

        shutil.rmtree(str(self.get_directory(final_hash)))
        self.size_used -= item.size

    def get_directory(self, final_hash):
        h = binascii.hexlify(final_hash).decode("ascii")
        # I don't think that separating the hash by the first byte is strictly necessary,
        # but hey, git does it too :-)
        return self.directory / h[:2] / h[2:]
