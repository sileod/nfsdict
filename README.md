# nfsdict

NFS-safe persistent dictionary for Python 🗃️

One file per key, atomic writes via temp + rename, in-memory read cache so reads never touch NFS after initial load. Corruption of one file loses that key only — never the whole store.

A best-effort key index speeds up init by avoiding expensive directory scans. The index is advisory — exact lookups always fall back to disk, so keys written by other processes are never lost.

## Install

```bash
pip install nfsdict
```

## Usage

```python
from nfsdict import NfsDict

# default: pickle serializer, ~/.local/share/nfsdict/default/
d = NfsDict()

# custom name and directory
d = NfsDict("my_cache", base_dir="/shared/nfs/cache")

# json serializer (human-readable, but values must be JSON-serializable)
d = NfsDict("my_cache", serializer="json")

# lazy loading: only keys are loaded at init, values fetched on first access
d = NfsDict("my_cache", lazy=True)

# use it like a normal dict
d["key"] = {"nested": [1, 2, 3]}
print(d["key"])
del d["key"]
print(len(d))

# re-read from disk (full scan, call if another process may have written)
d.sync()

# context manager flushes index on exit
with NfsDict("my_cache") as d:
    d["key"] = "value"
```

## How it works

- Each key is stored as a separate file, named by its SHA-256 hash, inside a two-character prefix subdirectory for filesystem-friendly sharding.
- Writes are atomic: data is written to a temp file, fsynced, then `os.replace`d into place.
- An in-memory cache is populated once at init and updated on every write/delete — so reads are pure dict lookups with zero NFS calls.
- If a key is not in the cache, `__getitem__` falls back to reading its file directly from disk (covers keys written by other processes).
- A best-effort key index avoids the expensive `rglob` directory scan on startup. If the index is missing or corrupted, a full scan runs automatically and rebuilds it.
- Corrupted value files are silently skipped during loading: you lose that one key, not the whole store.

## API

### `NfsDict(name="default", base_dir=None, serializer="pickle", lazy=False)`

| Parameter    | Description                                                   |
|-------------|---------------------------------------------------------------|
| `name`       | Namespace / subdirectory name                                 |
| `base_dir`   | Storage root. Defaults to `platformdirs.user_data_dir`        |
| `serializer` | `"pickle"` or `"json"`                                        |
| `lazy`       | If `True`, values are loaded on first access, not at init     |

Supports the full `MutableMapping` interface: `__getitem__`, `__setitem__`, `__delitem__`, `__contains__`, `__iter__`, `__len__`, plus:

- `.sync()` — full re-read from disk
- `.close()` — flush the key index to disk
- Context manager (`with NfsDict(...) as d:`)

## License

MIT
