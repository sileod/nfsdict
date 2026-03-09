# nfsdict

NFS-safe persistent dictionary for Python (CPython & PyPy).

One file per key, atomic writes via temp + rename, in-memory read cache so reads never touch NFS after initial load. Corruption of one file loses that key only — never the whole store.

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

# use it like a normal dict
d["key"] = {"nested": [1, 2, 3]}
print(d["key"])
del d["key"]
print(len(d))

# re-read from disk (if another process may have written)
d.sync()
```

## How it works

- Each key is stored as a separate file, named by its SHA-256 hash, inside a two-character prefix subdirectory for filesystem-friendly sharding.
- Writes are atomic: data is written to a temp file, fsynced, then renamed into place.
- An in-memory cache is populated once at init and updated on every write/delete — so reads are pure dict lookups with zero NFS calls.
- Corrupted files are silently skipped during loading: you lose that one key, not the whole store.

## API

### `NfsDict(name="default", base_dir=None, serializer="pickle")`

| Parameter    | Description                                                   |
|-------------|---------------------------------------------------------------|
| `name`       | Namespace / subdirectory name                                 |
| `base_dir`   | Storage root. Defaults to `platformdirs.user_data_dir`        |
| `serializer` | `"pickle"` or `"json"`                                        |

Supports the full `MutableMapping` interface: `__getitem__`, `__setitem__`, `__delitem__`, `__contains__`, `__iter__`, `__len__`, plus `.sync()` to re-read from disk.

## License

MIT
