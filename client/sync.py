"""The sync engine: reconcile a local folder with the server via delta sync.

push()  local -> server : chunk each changed file, upload only the blocks the
                          server is missing (straight to B2), then commit the recipe.
pull()  server -> local : fetch the recipe, download its blocks (straight from B2,
                          re-verifying each), reassemble.

Only changed *blocks* cross the network, and file bytes never pass through the
app server — they go directly to/from object storage via presigned URLs.

Conflicts: when both sides edited a file, neither edit is lost. pull never
overwrites a local edit that push() hasn't sent yet, and saves the server's version
as a "(conflicted copy)" beside it; push's 412 reconcile does the same before
retrying over the server's version. The next push uploads the copy.

Deletes sync both ways through LocalIndex (the last-synced state): a path in the
index but gone from disk was deleted locally, so push deletes it on the server; a
path in the index but gone from the server was deleted remotely, so pull deletes the
local copy unless it has an unpushed edit (which then survives as a new file). An
edit made elsewhere wins over a local delete that push hasn't sent yet.

Limitations (v1): pull downloads a differing file's blocks in full (no local
block reuse); a delete that push HAS sent wins over a concurrent edit elsewhere
(the server's DELETE takes no If-Match).
"""
from pathlib import Path

from client import chunker
from client.api_client import ApiClient, PreconditionFailed
from client.state import LocalIndex


class CorruptBlock(Exception):
    """A downloaded block's bytes don't match its hash (re-verify failed)."""


class UnsafePath(ValueError):
    """A server-supplied path resolves outside the sync folder."""


class SyncEngine:
    def __init__(self, api: ApiClient, index: LocalIndex, folder: Path):
        self._api = api
        self._index = index
        self._folder = folder

    def _server_path(self, file: Path) -> str:
        return "/" + file.relative_to(self._folder).as_posix()

    def _local_path(self, server_path: str) -> Path:
        # lstrip: an absolute right-hand side would discard the folder entirely.
        # resolve: a symlinked component can escape a path that reads as canonical.
        root = self._folder.resolve()
        local = (root / server_path.lstrip("/")).resolve()
        if not local.is_relative_to(root):
            raise UnsafePath(f"path escapes the sync folder: {server_path!r}")
        if local == root:
            raise UnsafePath(f"path resolves to the sync folder itself: {server_path!r}")
        return local

    # --- push: chunk -> negotiate -> upload only missing blocks -> commit ---
    def push(self) -> list[str]:
        pushed: list[str] = []
        for file in sorted(self._folder.rglob("*")):
            if not file.is_file():
                continue
            server_path = self._server_path(file)
            if "\\" in server_path:
                continue  # the server rejects "\" in paths (422): skip it rather than abort the whole push
            data = file.read_bytes()
            if not self._index.is_changed(server_path, data):
                continue

            blocks = list(chunker.split(data))
            hashes = [h for h, _ in blocks]
            block_bytes = dict(blocks)  # hash -> bytes

            missing = self._api.missing_blocks(hashes)
            if missing:
                urls = self._api.upload_urls(missing)
                for h in missing:
                    self._api.put_block(urls[h], block_bytes[h])  # -> B2 directly

            base_etag = self._index.etag(server_path)
            try:
                new_etag = self._api.commit_file(server_path, len(data), hashes, base_etag)
            except PreconditionFailed:
                new_etag = self._reconcile(server_path, len(data), hashes)
            self._index.update(server_path, data, new_etag)
            pushed.append(server_path)

        # local deletes: synced before (in the index) but gone from disk now
        for server_path in sorted(self._index.known_paths()):
            if not self._local_path(server_path).exists():
                self._api.delete_file(server_path)
                self._index.remove(server_path)
                pushed.append(server_path)
        self._index.save()
        return pushed

    # --- pull: recipe -> download blocks (verify) -> reassemble ---
    def pull(self) -> list[str]:
        pulled: list[str] = []
        known = self._index.known_paths()  # what we'd synced before this pull
        server_paths: set[str] = set()
        for summary in self._api.list_files():
            server_path = summary["path"]
            server_paths.add(server_path)
            recipe, etag = self._api.get_recipe(server_path)
            hashes = recipe["block_hashes"]
            local = self._local_path(server_path)

            # deleted locally but push() hasn't sent the delete yet: don't download it again. If the server
            # changed it since (etag moved), fall through and download it: the edit wins over the delete.
            if not local.exists() and server_path in known and etag == self._index.etag(server_path):
                continue

            if local.exists():
                local_bytes = local.read_bytes()
                # skip download if local content already matches the recipe
                if [h for h, _ in chunker.split(local_bytes)] == hashes:
                    continue
                # local content differs from what we last synced: an edit push() hasn't sent yet.
                # Never overwrite it. If the server changed too, both sides edited -> keep the server's
                # version as a conflict copy beside ours; the next push uploads it, so neither edit is lost.
                if self._index.is_changed(server_path, local_bytes):
                    if etag != self._index.etag(server_path):
                        data = self._download_blocks(server_path, hashes)
                        copy = self._conflict_copy_path(local, data)
                        copy.write_bytes(data)
                        pulled.append(self._server_path(copy))
                    continue

            # (re-)downloads whole file
            # in ideal case, implement content-defined chunking, and pull delta blocks only
            data = self._download_blocks(server_path, hashes)
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(data)
            self._index.update(server_path, data, etag)
            pulled.append(server_path)

        # server-side deletes: synced before, gone from the server now
        for server_path in sorted(known - server_paths):
            local = self._local_path(server_path)
            if local.exists() and not self._index.is_changed(server_path, local.read_bytes()):
                local.unlink()
                pulled.append(server_path)
            # an unpushed local edit survives: with no index entry, the next push re-creates the file
            self._index.remove(server_path)
        self._index.save()
        return pulled

    def _download_blocks(self, path: str, hashes: list[str]) -> bytes:
        urls = self._api.download_urls(path, hashes)
        parts: list[bytes] = []
        for h in hashes:
            block = self._api.get_block(urls[h])  # <- from B2 directly
            if chunker.hash_block(block) != h:  # re-verify on read
                raise CorruptBlock(h)
            parts.append(block)
        return b"".join(parts)

    @staticmethod
    def _conflict_copy_path(local: Path, data: bytes) -> Path:
        # "a (conflicted copy).txt", then "a (conflicted copy 2).txt", ... — reusing a copy that already holds
        # these bytes, so repeated pulls during the same conflict don't pile up duplicates
        n = 1
        while True:
            label = "conflicted copy" if n == 1 else f"conflicted copy {n}"
            copy = local.with_name(f"{local.stem} ({label}){local.suffix}")
            if not copy.exists() or copy.read_bytes() == data:
                return copy
            n += 1

    def _reconcile(self, path, size, hashes) -> str:
        # someone committed between our read and our write. Keep their version as a
        # conflict copy beside ours (the next push uploads it), then retry once as an
        # update off the current etag, so our bytes take the path and theirs survive in
        # the copy. A second race in the tiny window re-raises PreconditionFailed and
        # aborts the run.
        recipe, current = self._api.get_recipe(path)
        theirs = self._download_blocks(path, recipe["block_hashes"])
        local = self._local_path(path)
        self._conflict_copy_path(local, theirs).write_bytes(theirs)
        return self._api.commit_file(path, size, hashes, current)

    def sync(self) -> dict[str, list[str]]:
        return {"pushed": self.push(), "pulled": self.pull()}
