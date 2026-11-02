"""The catalog on disk (docs/catalog-plan.md, Phase 3): what remote engines'
catalog levels loaded, kept across sessions under ``$QSQL_HOME/catalog/``."""

import hashlib
import os
import stat

import polars as pl
import pytest

from quicksql.catalog_store import CatalogStore, default_store

SECRET_KEY = "postgres:postgresql://reader:hunter2@db.internal/wh/sales.orders"


def frame(n: int = 3, text: str = "x") -> pl.DataFrame:
    return pl.DataFrame({"field_path": [f"c{i}" for i in range(n)], "description": [text] * n})


class Clock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def levels(root) -> list:
    return sorted(p for p in root.iterdir() if p.suffix == ".level")


def test_a_new_store_on_the_same_directory_reads_what_the_last_one_wrote(tmp_path) -> None:
    CatalogStore(tmp_path / "catalog").put("bigquery:p/ds.t", frame(), saved_at=500.0, scope="bigquery:p")
    got = CatalogStore(tmp_path / "catalog").get("bigquery:p/ds.t", scope="bigquery:p")
    assert got is not None
    assert got.frame.equals(frame()) and got.saved_at == 500.0


def test_an_unknown_key_is_a_miss(tmp_path) -> None:
    assert CatalogStore(tmp_path / "catalog").get("nothing", scope=None) is None


def test_files_are_named_by_the_hash_of_their_key_and_never_hold_it(tmp_path) -> None:
    root = tmp_path / "catalog"
    CatalogStore(root).put(SECRET_KEY, frame(), saved_at=1.0, scope="postgres:postgresql://reader:***@db.internal/wh")
    [level] = levels(root)
    assert level.stem == hashlib.sha256(SECRET_KEY.encode()).hexdigest()
    for path in root.rglob("*"):
        if path.is_file():
            assert b"hunter2" not in path.read_bytes()


def test_the_directory_is_private_and_so_is_every_file(tmp_path) -> None:
    root = tmp_path / "catalog"
    store = CatalogStore(root)
    store.put("k", frame(), saved_at=1.0, scope="s")
    store.clear(["s"], at=2.0)
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    files = [p for p in root.rglob("*") if p.is_file()]
    assert files and all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in files)


def test_a_write_that_fails_leaves_neither_a_level_nor_a_temp_file(tmp_path, monkeypatch) -> None:
    root = tmp_path / "catalog"
    store = CatalogStore(root)
    store.put("k", frame(), saved_at=1.0, scope=None)
    monkeypatch.setattr(os, "replace", lambda src, dst: (_ for _ in ()).throw(OSError("disk full")))
    store.put("k", frame(9), saved_at=2.0, scope=None)  # swallowed: the disk is a cache
    assert [p.name for p in root.iterdir() if p.is_file()] == [levels(root)[0].name]
    monkeypatch.undo()
    assert store.get("k", scope=None).saved_at == 1.0  # the old level stands, whole


def test_a_torn_or_foreign_file_is_a_miss_and_goes(tmp_path) -> None:
    root = tmp_path / "catalog"
    store = CatalogStore(root)
    store.put("a", frame(), saved_at=1.0, scope=None)
    store.put("b", frame(), saved_at=1.0, scope=None)
    a, b = (root / f"{hashlib.sha256(k.encode()).hexdigest()}.level" for k in "ab")
    a.write_bytes(a.read_bytes()[:40])  # cut short
    b.write_bytes(b'{"v": 999}\n' + b"whatever")  # another version's
    assert store.get("a", scope=None) is None and store.get("b", scope=None) is None
    assert not a.exists() and not b.exists()


def test_drop_removes_one_level(tmp_path) -> None:
    store = CatalogStore(tmp_path / "catalog")
    store.put("a", frame(), saved_at=1.0, scope=None)
    store.put("b", frame(), saved_at=1.0, scope=None)
    store.drop("a")
    store.drop("never stored")  # nothing to do
    assert store.get("a", scope=None) is None and store.get("b", scope=None) is not None


def test_clearing_a_scope_misses_its_levels_saved_before_and_survives_a_new_store(tmp_path) -> None:
    root = tmp_path / "catalog"
    store = CatalogStore(root)
    store.put("old", frame(), saved_at=10.0, scope="bigquery:p")
    store.put("other", frame(), saved_at=10.0, scope="postgres:h")
    store.clear(["bigquery:p"], at=20.0)  # a run on bigquery ended at 20
    store.put("new", frame(), saved_at=30.0, scope="bigquery:p")  # loaded after it
    again = CatalogStore(root)  # the next session
    assert again.get("old", scope="bigquery:p") is None
    assert again.get("new", scope="bigquery:p") is not None
    assert again.get("other", scope="postgres:h") is not None
    # the reader's scope decides: the LSP reads a level the TUI wrote
    assert again.get("other", scope="bigquery:p") is None


def test_clearing_a_scope_nothing_was_stored_under_writes_nothing(tmp_path) -> None:
    """A save clears its cells' outputs, which never persist: no marker
    file per cell per save."""
    root = tmp_path / "catalog"
    store = CatalogStore(root)
    store.put("k", frame(), saved_at=1.0, scope="bigquery:p")
    files = sorted(p for p in root.rglob("*") if p.is_file())
    store.clear(["a", "b", "manual"], at=2.0)
    assert sorted(p for p in root.rglob("*") if p.is_file()) == files


def test_an_older_clear_never_undoes_a_newer_one(tmp_path) -> None:
    store = CatalogStore(tmp_path / "catalog")
    store.put("k", frame(), saved_at=15.0, scope="s")
    store.clear(["s"], at=20.0)
    store.clear(["s"], at=12.0)  # another process's clock, behind
    assert store.get("k", scope="s") is None


def test_past_its_cap_the_store_drops_the_least_recently_read(tmp_path) -> None:
    clock = Clock()
    root = tmp_path / "catalog"
    probe = CatalogStore(tmp_path / "probe")
    probe.put("x", frame(200, "y" * 50), saved_at=1.0, scope=None)
    size = levels(tmp_path / "probe")[0].stat().st_size
    store = CatalogStore(root, max_bytes=int(size * 2.5), clock=clock)
    for key in "ab":
        clock.now += 1
        store.put(key, frame(200, "y" * 50), saved_at=1.0, scope=None)
    clock.now += 1
    assert store.get("a", scope=None) is not None  # a is read: b is now the oldest
    clock.now += 1
    store.put("c", frame(200, "y" * 50), saved_at=1.0, scope=None)
    assert store.get("b", scope=None) is None
    assert store.get("a", scope=None) is not None and store.get("c", scope=None) is not None
    assert store.size_bytes() <= store.max_bytes


def test_a_level_bigger_than_the_cap_is_kept_alone(tmp_path) -> None:
    root = tmp_path / "catalog"
    store = CatalogStore(root, max_bytes=10)
    store.put("a", frame(), saved_at=1.0, scope=None)
    store.put("b", frame(), saved_at=1.0, scope=None)
    assert store.get("a", scope=None) is None and store.get("b", scope=None) is not None


def test_a_store_that_cant_write_is_empty_and_never_raises(tmp_path) -> None:
    blocker = tmp_path / "catalog"
    blocker.write_text("a file where the directory goes")
    store = CatalogStore(blocker)
    store.put("k", frame(), saved_at=1.0, scope="s")
    store.clear(["s"], at=2.0)
    store.drop("k")
    assert store.get("k", scope="s") is None and store.size_bytes() == 0


def test_the_default_store_lives_under_qsql_home(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QSQL_HOME", str(tmp_path / "home"))
    store = default_store()
    assert store is not None and store.root == tmp_path / "home" / "catalog"


@pytest.mark.parametrize("value", ["0", "false", "no", "off"])
def test_qsql_catalog_persist_off_means_no_store(monkeypatch, value) -> None:
    monkeypatch.setenv("QSQL_CATALOG_PERSIST", value)
    assert default_store() is None


# ---------- warming and settings (Phase 4) ----------


def test_saved_at_reads_when_a_level_was_loaded_without_its_frame(tmp_path) -> None:
    store = CatalogStore(tmp_path / "catalog")
    store.put("k", frame(), saved_at=500.0, scope="s")
    assert store.saved_at("k", scope="s") == 500.0
    assert store.saved_at("missing", scope="s") is None


def test_saved_at_misses_a_level_its_scope_cleared_since(tmp_path) -> None:
    store = CatalogStore(tmp_path / "catalog")
    store.put("k", frame(), saved_at=500.0, scope="s")
    store.clear(["s"], at=600.0)
    assert store.saved_at("k", scope="s") is None


def test_saved_at_misses_an_unreadable_level(tmp_path) -> None:
    store = CatalogStore(tmp_path / "catalog")
    store.put("k", frame(), saved_at=500.0, scope=None)
    [path] = levels(tmp_path / "catalog")
    path.write_bytes(b"not a header\n")
    assert store.saved_at("k", scope=None) is None


def test_qsql_catalog_size_mb_caps_the_disk(monkeypatch) -> None:
    monkeypatch.setenv("QSQL_CATALOG_SIZE_MB", "10")
    assert default_store().max_bytes == 10 * 2**20


@pytest.mark.parametrize("value", ["", "lots", "-5", "0"])
def test_a_size_that_isnt_a_positive_number_keeps_the_default_cap(monkeypatch, value) -> None:
    monkeypatch.setenv("QSQL_CATALOG_SIZE_MB", value)
    assert default_store().max_bytes == 256 * 2**20
