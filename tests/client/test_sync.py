"""Unit tests for the delta SyncEngine using a fake block-protocol API."""
import pytest

from client import chunker
from client.api_client import PreconditionFailed
from client.state import LocalIndex
from client.sync import CorruptBlock, SyncEngine


def _etag(block_hashes):
    # a deterministic token per content, standing in for the server's recipe etag
    return "etag:" + ",".join(block_hashes)


class FakeApi:
    """Simulates the server's block protocol + a B2 store, in memory.

    Presigned URLs are faked as ``fake://<hash>`` so put_block/get_block can
    recover the hash and hit the in-memory block store — mirroring how the real
    client uploads/downloads by URL, not by hash.
    """

    def __init__(self):
        self.blocks: dict[str, bytes] = {}                  # hash -> bytes (the "B2 store")
        self.recipes: dict[str, dict] = {}                  # path -> recipe (incl. "etag")
        self.commits: list[tuple] = []                      # (path, base_etag) — precondition spy

    def missing_blocks(self, hashes):
        return [h for h in hashes if h not in self.blocks]

    def upload_urls(self, hashes):
        return {h: f"fake://{h}" for h in hashes}

    def download_urls(self, path, hashes):
        return {h: f"fake://{h}" for h in hashes}

    def put_block(self, url, data):
        self.blocks[url.removeprefix("fake://")] = data

    def get_block(self, url):
        return self.blocks[url.removeprefix("fake://")]

    def commit_file(self, path, size, block_hashes, base_etag):
        self.commits.append((path, base_etag))
        missing = self.missing_blocks(block_hashes)
        if missing:
            raise AssertionError(f"commit with missing blocks: {missing}")  # server would 409
        new_etag = _etag(block_hashes)
        existing = self.recipes.get(path)
        # mirror the server's etag compare-and-swap (incl. the retry/target fold)
        if base_etag is None:
            if existing is not None and existing["etag"] != new_etag:
                raise PreconditionFailed(path)
        elif existing is None or existing["etag"] not in (base_etag, new_etag):
            raise PreconditionFailed(path)
        self.recipes[path] = {"path": path, "size": size,
                              "block_hashes": block_hashes, "etag": new_etag}
        return new_etag

    def list_files(self):
        return [{"path": p} for p in self.recipes]

    def get_recipe(self, path):
        r = self.recipes[path]
        return r, r["etag"]

    def delete_file(self, path):
        # mirrors DELETE /files?path=…: removes the recipe (blocks stay, like the real server)
        self.recipes.pop(path, None)


@pytest.fixture
def setup(tmp_path):
    folder = tmp_path / "sync"
    folder.mkdir()
    api = FakeApi()
    index = LocalIndex(tmp_path / "index.json")
    return SyncEngine(api, index, folder), api, folder


def test_push_uploads_blocks_and_commits(setup):
    engine, api, folder = setup
    (folder / "a.txt").write_bytes(b"hello world")
    pushed = engine.push()
    assert pushed == ["/a.txt"]
    assert "/a.txt" in api.recipes
    for h, _ in chunker.split(b"hello world"):
        assert h in api.blocks  # block landed in the store


def test_push_skips_a_backslash_name_and_still_pushes_the_rest(setup):
    # the server 422s on "\" in a path; one such file must not abort the push for the others
    engine, api, folder = setup
    (folder / "a\\b.txt").write_bytes(b"bad name")
    (folder / "c.txt").write_bytes(b"fine")
    assert engine.push() == ["/c.txt"]
    assert api.commits == [("/c.txt", None)]
    assert api.blocks.keys() == {h for h, _ in chunker.split(b"fine")}  # no blocks uploaded for the skipped file


def test_push_skips_unchanged_file(setup):
    engine, _, folder = setup
    (folder / "a.txt").write_bytes(b"hello")
    engine.push()
    assert engine.push() == []


def test_push_of_a_new_file_sends_no_base_etag(setup):
    # a file the index has never seen commits as a create (base_etag None -> If-None-Match: *)
    engine, api, folder = setup
    (folder / "a.txt").write_bytes(b"hello")
    engine.push()
    assert api.commits == [("/a.txt", None)]


def test_push_of_an_edited_file_sends_its_stored_base_etag(setup):
    # a second edit commits as an update off the etag the first push stored
    engine, api, folder = setup
    f = folder / "a.txt"
    f.write_bytes(b"hello")
    engine.push()
    first_etag = api.recipes["/a.txt"]["etag"]
    f.write_bytes(b"hello world")
    engine.push()
    assert api.commits[-1] == ("/a.txt", first_etag)


def test_push_reconciles_after_a_conflict_and_our_bytes_win(setup):
    # another device committed this path first (index has no base -> we try to create).
    # the create conflicts; reconcile refetches the etag and retries as an update.
    engine, api, folder = setup
    for h, b in chunker.split(b"theirs"):
        api.blocks[h] = b
    theirs = [h for h, _ in chunker.split(b"theirs")]
    api.recipes["/a.txt"] = {"path": "/a.txt", "size": 6,
                             "block_hashes": theirs, "etag": _etag(theirs)}
    (folder / "a.txt").write_bytes(b"ours")

    pushed = engine.push()

    assert pushed == ["/a.txt"]
    assert api.recipes["/a.txt"]["block_hashes"] == [h for h, _ in chunker.split(b"ours")]
    # a create that conflicted, then a reconcile retry off the current etag
    assert api.commits == [("/a.txt", None), ("/a.txt", _etag(theirs))]
    # ...and their version wasn't thrown away: it's kept locally as a conflict copy
    assert (folder / "a (conflicted copy).txt").read_bytes() == b"theirs"


def test_push_gives_up_when_the_reconcile_retry_also_conflicts(setup):
    # a second racer squeezes into the window: reconcile's retry conflicts too -> abort loudly
    engine, api, folder = setup
    for h, b in chunker.split(b"theirs"):
        api.blocks[h] = b
    theirs = [h for h, _ in chunker.split(b"theirs")]
    api.recipes["/a.txt"] = {"path": "/a.txt", "size": 6,
                             "block_hashes": theirs, "etag": _etag(theirs)}
    (folder / "a.txt").write_bytes(b"ours")
    # a different winner lands between our refetch and retry: the etag we get back is
    # already stale, so the retry's precondition fails too.
    api.get_recipe = lambda path: ({"path": path, "block_hashes": []}, "stale")

    with pytest.raises(PreconditionFailed):
        engine.push()


def test_push_only_uploads_missing_blocks(setup):
    engine, api, folder = setup
    # first file establishes a block
    (folder / "a.txt").write_bytes(b"shared content")
    engine.push()
    uploads = len(api.blocks)
    # second file with identical content: block already present -> no new upload
    (folder / "b.txt").write_bytes(b"shared content")
    engine.push()
    assert len(api.blocks) == uploads  # delta: nothing re-uploaded


def test_pull_downloads_and_reassembles(setup):
    engine, api, folder = setup
    # put a file on the "server" via another engine
    other = folder.parent / "other"
    other.mkdir()
    e2 = SyncEngine(api, LocalIndex(folder.parent / "i2.json"), other)
    (other / "remote.txt").write_bytes(b"remote payload")
    e2.push()

    pulled = engine.pull()
    assert pulled == ["/remote.txt"]
    assert (folder / "remote.txt").read_bytes() == b"remote payload"


def test_pull_skips_when_local_matches_recipe(setup):
    engine, api, folder = setup
    (folder / "a.txt").write_bytes(b"same everywhere")
    engine.push()   # now on server AND local, identical
    assert engine.pull() == []  # local already matches recipe -> no download


def test_pull_reverifies_and_raises_on_corruption(setup):
    engine, api, folder = setup
    (folder / "a.txt").write_bytes(b"trust but verify")
    engine.push()
    # poison a stored block: bytes no longer match their hash key
    some_hash = next(iter(api.blocks))
    api.blocks[some_hash] = b"tampered!"
    # a fresh engine (empty index) will try to download and must catch it
    victim = SyncEngine(api, LocalIndex(folder.parent / "v.json"), folder.parent / "victim")
    with pytest.raises(CorruptBlock):
        victim.pull()


def test_sync_round_trips_between_two_folders(tmp_path):
    api = FakeApi()
    f1, f2 = tmp_path / "dev1", tmp_path / "dev2"
    f1.mkdir()
    f2.mkdir()
    e1 = SyncEngine(api, LocalIndex(tmp_path / "i1.json"), f1)
    e2 = SyncEngine(api, LocalIndex(tmp_path / "i2.json"), f2)

    (f1 / "notes" ).mkdir()
    (f1 / "notes" / "hello.txt").write_bytes(b"from device 1")
    e1.sync()
    e2.sync()

    assert (f2 / "notes" / "hello.txt").read_bytes() == b"from device 1"


def test_sync_returns_pushed_and_pulled(setup):
    engine, api, folder = setup
    (folder / "local.txt").write_bytes(b"local")
    # a remote file placed directly on the server
    for h, b in chunker.split(b"remote"):
        api.blocks[h] = b
    remote_hashes = [h for h, _ in chunker.split(b"remote")]
    api.recipes["/remote.txt"] = {"path": "/remote.txt", "size": 6,
                                  "block_hashes": remote_hashes, "etag": _etag(remote_hashes)}
    result = engine.sync()
    assert "/local.txt" in result["pushed"]
    assert "/remote.txt" in result["pulled"]


def test_local_path_accepts_a_nested_path(setup):
    engine, _, folder = setup
    assert engine._local_path("/docs/a.txt") == (folder / "docs" / "a.txt").resolve()


@pytest.mark.parametrize("server_path", ["/../evil.txt", "/../../evil.txt", "/docs/../../evil.txt"])
def test_local_path_rejects_traversal(setup, server_path):
    engine, _, _ = setup
    with pytest.raises(ValueError, match="escapes the sync folder"):
        engine._local_path(server_path)


@pytest.mark.parametrize("server_path", ["/", "/."])
def test_local_path_rejects_the_folder_itself(setup, server_path):
    engine, _, _ = setup
    with pytest.raises(ValueError, match="sync folder itself"):
        engine._local_path(server_path)


def test_local_path_rejects_a_symlinked_escape(setup, tmp_path):
    """The string is canonical; only resolving the symlink reveals the escape.

    No server-side validation of the path text can catch this — which is why the
    containment check has to live next to the write.
    """
    engine, _, folder = setup
    outside = tmp_path / "outside"
    outside.mkdir()
    (folder / "docs").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="escapes the sync folder"):
        engine._local_path("/docs/a.txt")


def test_pull_aborts_on_an_unsafe_server_path(setup):
    engine, api, _ = setup
    api.recipes["/../evil.txt"] = {"path": "/../evil.txt", "size": 0,
                                   "block_hashes": [], "etag": _etag([])}
    with pytest.raises(ValueError, match="escapes the sync folder"):
        engine.pull()
