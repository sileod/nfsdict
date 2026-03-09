"""nfsdict - NFS-safe persistent dictionary. Atomic file-per-key writes, in-memory read cache."""

from __future__ import annotations

import hashlib
import json
import os
import pickle
import tempfile
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Iterator

try:
    from platformdirs import user_data_dir
except ImportError:
    from appdirs import user_data_dir


class NfsDict(MutableMapping):
    """
    Persistent dict that survives NFS failures.

    One file per key, atomic writes via temp+rename, in-memory cache
    so reads never touch NFS after initial load.

    Corruption of one file loses that key only — never the whole store.

    Parameters
    ----------
    name : str
        Namespace / subdirectory name.
    base_dir : str | Path | None
        Storage root.  Defaults to platformdirs user_data_dir.
    serializer : "pickle" | "json"
        Serialization format.
    """

    def __init__(
        self,
        name: str = "default",
        base_dir: str | Path | None = None,
        serializer: str = "pickle",
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

        # --- write-through cache: loaded once, kept in sync ---
        self._cache: dict[str, Any] = {}
        self._reload()

    # ---- disk layout ----

    def _path(self, key: str) -> Path:
        h = hashlib.sha256(key.encode()).hexdigest()
        return self._dir / h[:2] / (h + self._ext)

    # ---- cache management ----

    def _reload(self) -> None:
        """Scan disk and populate cache. Called once at init."""
        self._cache.clear()
        for p in self._dir.rglob(f"*{self._ext}"):
            try:
                k, v = self._loads(p.read_bytes())
                self._cache[k] = v
            except Exception:
                continue  # corrupted file → skip, don't die

    def sync(self) -> None:
        """Re-read from disk. Call if another process may have written."""
        self._reload()

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
            os.rename(tmp, str(path))
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self._cache[key] = value  # update cache only after successful write

    def __getitem__(self, key: str) -> Any:
        try:
            return self._cache[key]  # ← zero NFS calls
        except KeyError:
            raise KeyError(key)

    def __delitem__(self, key: str) -> None:
        if key not in self._cache:
            raise KeyError(key)
        try:
            self._path(key).unlink()
        except FileNotFoundError:
            pass
        del self._cache[key]

    def __contains__(self, key: object) -> bool:
        return key in self._cache

    def __iter__(self) -> Iterator[str]:
        return iter(self._cache)

    def __len__(self) -> int:
        return len(self._cache)

    def __repr__(self) -> str:
        return f"NfsDict({str(self._dir)!r}, {len(self._cache)} keys)"