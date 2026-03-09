"""nfsdict - NFS-safe persistent dictionary. Atomic file-per-key writes, in-memory read cache."""

from __future__ import annotations

import atexit
import hashlib
import json
import os
import pickle
import tempfile
import weakref
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Iterator

try:
    from platformdirs import user_data_dir
except ImportError:
    from appdirs import user_data_dir

_UNLOADED = object()

# ---- atexit: flush dirty indexes on interpreter shutdown ----

_all_instances: weakref.WeakSet = weakref.WeakSet()


def _atexit_flush() -> None:
    for d in list(_all_instances):
        try:
            d._flush_index()
        except Exception:
            pass


atexit.register(_atexit_flush)


class NfsDict(MutableMapping):
    """
    Persistent dict that survives NFS failures.

    One file per key, atomic writes via temp+rename, in-memory cache
    so reads never touch NFS after initial load.

    Corruption of one file loses that key only — never the whole store.

    A best-effort index file caches the set of known keys so that
    subsequent ``__init__`` calls can skip the expensive ``rglob``
    directory scan.  The index is advisory — exact lookups always
    fall back to disk on cache miss, so keys written by other
    processes are never truly lost.

    Parameters
    ----------
    name : str
        Namespace / subdirectory name.
    base_dir : str | Path | None
        Storage root.  Defaults to platformdirs user_data_dir.
    serializer : "pickle" | "json"
        Serialization format.
    lazy : bool
        If True, values are loaded from disk on first access rather
        than at init.  Keys are still available immediately via the index.
    """

    def __init__(
        self,
        name: str = "default",
        base_dir: str | Path | None = None,
        serializer: str = "pickle",
        lazy: bool = False,
    ) -> None:
        if base_dir is None:
            self._dir = Path(user_data_dir("nfsdict")) / name
        else:
            self._dir = Path(base_dir) / name
        self._dir.mkdir(parents=True, exist_ok=True)

        if serializer == "pickle":
            self._ext = ".pkl"
            self._dumps = pickle.dumps
            self._loads = pickle.loads
        elif serializer == "json":
            self._ext = ".json"
            self._dumps = lambda obj: json.dumps(obj, ensure_ascii=False).encode()
            self._loads = lambda blob: json.loads(blob)
        else:
            raise ValueError(f"Unknown serializer: {serializer!r}")

        self._lazy = lazy
        self._index_path = self._dir / f"_index{self._ext}"
        self._index_dirty = False

        # --- write-through cache: loaded once, kept in sync ---
        # Values are the _UNLOADED sentinel when lazy and not yet accessed.
        self._cache: dict[str, Any] = {}
        self._reload()

        _all_instances.add(self)

    # ---- disk layout ----

    def _path(self, key: str) -> Path:
        h = hashlib.sha256(key.encode()).hexdigest()
        return self._dir / h[:2] / (h + self._ext)

    # ---- index management ----

    def _save_index(self) -> bool:
        """Atomically persist the set of known keys (best-effort)."""
        try:
            blob = self._dumps(list(self._cache.keys()))
            fd, tmp = tempfile.mkstemp(dir=self._dir)
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(blob)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, str(self._index_path))
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except Exception:
            return False  # index is a speed optimisation, never fatal
        return True

    def _load_index(self) -> set[str] | None:
        """Load the key set from the index file, or None if unavailable."""
        try:
            data = self._loads(self._index_path.read_bytes())
            if isinstance(data, (set, list)):
                return set(data)
        except Exception:
            pass
        return None

    def _flush_index(self) -> None:
        """Write the index to disk if it has changed since last flush."""
        if self._index_dirty and self._save_index():
            self._index_dirty = False

    # ---- cache management ----

    def _reload(self) -> None:
        """Populate cache.  Fast path via index, slow path via full scan."""
        self._cache.clear()

        # --- fast path: read the index then fetch only listed keys ---
        keys = self._load_index()
        if keys is not None:
            if self._lazy:
                for key in keys:
                    self._cache[key] = _UNLOADED
            else:
                for key in keys:
                    try:
                        _, v = self._loads(self._path(key).read_bytes())
                        self._cache[key] = v
                    except Exception:
                        continue  # file gone / corrupted → drop key
            return

        # --- slow path: full directory scan, then rebuild index ---
        self._full_scan()

    def _full_scan(self) -> None:
        """Walk every value file, rebuild cache and index."""
        self._cache.clear()
        for p in self._dir.rglob(f"*{self._ext}"):
            if p.name.startswith("_"):
                continue  # skip _index file
            try:
                k, v = self._loads(p.read_bytes())
                self._cache[k] = _UNLOADED if self._lazy else v
            except Exception:
                continue  # corrupted file → skip, don't die
        self._index_dirty = not self._save_index()

    def sync(self) -> None:
        """Full re-read from disk.  Call if another process may have written."""
        self._full_scan()

    def close(self) -> None:
        """Flush the index to disk."""
        self._flush_index()

    # ---- MutableMapping ----

    def __setitem__(self, key: str, value: Any) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        blob = self._dumps((key, value))
        fd, tmp = tempfile.mkstemp(dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(blob)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, str(path))
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self._cache[key] = value
        self._index_dirty = True

    def __getitem__(self, key: str) -> Any:
        had_entry = key in self._cache
        v = self._cache.get(key, _UNLOADED)
        if v is _UNLOADED:
            # Cache miss or lazy — try disk.
            try:
                _, v = self._loads(self._path(key).read_bytes())
            except Exception:
                if had_entry:  # stale index entry → clean up
                    del self._cache[key]
                    self._index_dirty = True
                raise KeyError(key)
            self._cache[key] = v
            self._index_dirty = True
        return v

    def __delitem__(self, key: str) -> None:
        self[key]  # ensures key exists (disk fallback for advisory-index misses)
        try:
            self._path(key).unlink()
        except FileNotFoundError:
            pass
        self._cache.pop(key, None)
        self._index_dirty = True

    def __contains__(self, key: object) -> bool:
        return key in self._cache

    def __iter__(self) -> Iterator[str]:
        return iter(self._cache)

    def __len__(self) -> int:
        return len(self._cache)

    def __repr__(self) -> str:
        return f"NfsDict({str(self._dir)!r}, {len(self._cache)} keys)"

    __hash__ = object.__hash__  # restore hashability (MutableMapping sets it to None)

    # ---- context manager & cleanup ----

    def __enter__(self) -> NfsDict:
        return self

    def __exit__(self, *exc: Any) -> bool:
        self.close()
        return False

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass