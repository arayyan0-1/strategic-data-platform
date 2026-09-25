"""The dbt runner publishes the warehouse only after a clean build.

Tests use a fake dbt script that writes to the file in SDP_WAREHOUSE. No real
dbt runs.
"""
import datetime as dt
import os
import subprocess
import sys

import duckdb
import pytest

from sdp import daily, transform
from sdp.config import settings

FAKE_DBT = f"""#!{sys.executable}
import os
import sys
from pathlib import Path

import duckdb

path = os.environ["SDP_WAREHOUSE"]
log = os.environ.get("FAKE_DBT_LOG")
if log:
    with open(log, "a", encoding="utf-8") as f:
        f.write(path + "\\n")
if not os.environ.get("FAKE_DBT_NO_DB"):
    con = duckdb.connect(path)
    # dbt-duckdb names the database after the file stem. DuckDB cuts the name
    # at the first dot. A stem with a dot thus fails in dbt.
    if con.execute("select current_database()").fetchone()[0] != Path(path).stem:
        sys.exit(2)
    if os.environ.get("FAKE_DBT_KEEP_WAL"):
        con.execute("pragma disable_checkpoint_on_shutdown")
    con.execute("create table if not exists runs (n integer)")
    con.execute("insert into runs values (1)")
    con.close()
sys.exit(int(os.environ.get("FAKE_DBT_EXIT", "0")))
"""


@pytest.fixture
def fake_dbt(tmp_data_root, tmp_path, monkeypatch):
    """Install a fake dbt. Each run adds one row to the table `runs`."""
    script = tmp_path / "bin" / "dbt"
    script.parent.mkdir()
    script.write_text(FAKE_DBT, encoding="utf-8")
    script.chmod(0o755)
    monkeypatch.setattr(transform, "_dbt_executable", lambda: str(script))
    return script


def _seed_live() -> None:
    """Write a live warehouse with one model and one earlier run."""
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.warehouse_path)) as con:
        con.execute("create table old_model as select 1 as n")
        con.execute("create table runs as select 1 as n")


def _tables() -> dict[str, int]:
    """The row count of each table in the live warehouse."""
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        names = [t for (t,) in con.execute("show tables").fetchall()]
        return {t: con.execute(f"select count(*) from {t}").fetchone()[0] for t in names}


def _leftovers() -> list[str]:
    """Build files and WAL files that remain in the warehouse directory."""
    d = settings.warehouse_path.parent
    return sorted(p.name for p in d.iterdir() if ".build-" in p.name or p.suffix == ".wal")


def _dead_pid() -> int:
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid


class TestPublish:
    def test_a_clean_build_publishes_and_writes_the_stamp(self, fake_dbt):
        assert transform.main(["build"]) == 0
        assert _tables() == {"runs": 1}
        stamp = transform.last_build()
        assert set(stamp) == {"published_utc", "argv"}
        assert stamp["argv"] == ["build"]
        assert dt.datetime.fromisoformat(stamp["published_utc"]).tzinfo is not None
        assert _leftovers() == []

    def test_dbt_writes_a_private_file_and_not_the_live_file(self, fake_dbt, tmp_path,
                                                             monkeypatch):
        log = tmp_path / "dbt.log"
        monkeypatch.setenv("FAKE_DBT_LOG", str(log))
        transform.main(["build"])
        (target,) = log.read_text(encoding="utf-8").split()
        wh = settings.warehouse_path
        assert target == str(wh.parent / f"sdp.build-{os.getpid()}" / wh.name)

    @pytest.mark.parametrize("argv", [
        ["build"],
        ["run"],
        ["build", "--vars", "{min_dollar_volume: 1}"],
        ["build", "--vars={min_dollar_volume: 1}"],
        ["build", "--full-refresh", "--fail-fast"],
    ])
    def test_a_full_build_starts_from_an_empty_file(self, fake_dbt, argv):
        """No table of the old warehouse survives, so no free block does."""
        _seed_live()
        assert transform.main(argv) == 0
        assert _tables() == {"runs": 1}

    @pytest.mark.parametrize("argv", [
        ["build", "--select", "x"],
        ["build", "-s", "x"],
        ["build", "-sx"],
        ["build", "--select=x"],
        ["build", "--exclude", "x"],
        ["build", "--model", "x"],
        ["run", "-m", "x"],
        ["run", "--selector", "x"],
        ["build", "--threads", "2"],
    ])
    def test_a_partial_build_keeps_the_tables_of_the_live_file(self, fake_dbt, argv):
        _seed_live()
        assert transform.main(argv) == 0
        assert _tables() == {"old_model": 1, "runs": 2}

    def test_a_partial_build_with_no_live_file_starts_empty(self, fake_dbt):
        assert transform.main(["build", "--select", "x"]) == 0
        assert _tables() == {"runs": 1}

    def test_a_wal_is_written_into_the_file_before_the_publish(self, fake_dbt,
                                                               monkeypatch):
        monkeypatch.setenv("FAKE_DBT_KEEP_WAL", "1")
        checkpoints = []
        real = transform._checkpoint
        monkeypatch.setattr(transform, "_checkpoint",
                            lambda p: (checkpoints.append(p), real(p)))
        assert transform.main(["build"]) == 0
        assert len(checkpoints) == 1
        assert _tables() == {"runs": 1}
        assert _leftovers() == []

    def test_an_old_live_wal_is_not_replayed_onto_the_new_file(self, fake_dbt):
        _seed_live()
        transform._wal(settings.warehouse_path).write_bytes(b"not a wal of the new file")
        assert transform.main(["build"]) == 0
        assert _tables() == {"runs": 1}
        assert _leftovers() == []

    def test_a_reader_does_not_block_the_build(self, fake_dbt):
        _seed_live()
        reader = duckdb.connect(str(settings.warehouse_path), read_only=True)
        try:
            assert transform.main(["build"]) == 0
            # The reader keeps the old file until it closes.
            assert reader.execute("select count(*) from old_model").fetchone() == (1,)
        finally:
            reader.close()
        assert _tables() == {"runs": 1}


class TestFreshness:
    """The stamp mtime is the start of the last full build."""

    def test_a_partial_build_keeps_the_freshness_of_the_last_full_build(self, fake_dbt):
        assert transform.main(["build"]) == 0
        os.utime(transform.stamp_path(), (1000, 1000))
        assert transform.main(["build", "--select", "x"]) == 0
        assert transform.stamp_path().stat().st_mtime == 1000
        assert transform.last_build()["argv"] == ["build", "--select", "x"]

    def test_a_partial_build_alone_does_not_make_the_warehouse_fresh(self, fake_dbt):
        raw = settings.raw_dir / "x" / "data.parquet"
        raw.parent.mkdir(parents=True)
        raw.write_bytes(b"x")
        assert transform.main(["build", "--select", "x"]) == 0
        assert daily.dbt_stale() is True
        assert transform.main(["build"]) == 0
        assert daily.dbt_stale() is False


class TestNoPublish:
    @pytest.mark.parametrize("keep_wal", [False, True])
    def test_a_failed_build_leaves_the_live_file_and_the_stamp(self, fake_dbt,
                                                               monkeypatch, keep_wal):
        _seed_live()
        assert transform.main(["build", "--select", "x"]) == 0
        before = settings.warehouse_path.read_bytes()
        stamp = transform.stamp_path().read_bytes()

        monkeypatch.setenv("FAKE_DBT_EXIT", "1")
        if keep_wal:
            monkeypatch.setenv("FAKE_DBT_KEEP_WAL", "1")
        assert transform.main(["build"]) == 1
        assert settings.warehouse_path.read_bytes() == before
        assert transform.stamp_path().read_bytes() == stamp
        assert _leftovers() == []

    @pytest.mark.parametrize("argv", [
        ["test"], ["compile"], ["parse"], ["seed"], ["build", "--help"],
    ])
    def test_other_commands_never_replace_the_live_file(self, fake_dbt, argv):
        _seed_live()
        inode = settings.warehouse_path.stat().st_ino
        before = settings.warehouse_path.read_bytes()
        assert transform.main(argv) == 0
        assert settings.warehouse_path.stat().st_ino == inode
        assert settings.warehouse_path.read_bytes() == before
        assert transform.last_build() is None
        assert _leftovers() == []

    def test_a_build_that_writes_no_file_publishes_nothing(self, fake_dbt, monkeypatch):
        _seed_live()
        before = settings.warehouse_path.read_bytes()
        monkeypatch.setenv("FAKE_DBT_NO_DB", "1")
        assert transform.main(["build"]) == 0
        assert settings.warehouse_path.read_bytes() == before
        assert transform.last_build() is None

    def test_a_failed_publish_returns_one(self, fake_dbt, monkeypatch):
        _seed_live()
        before = settings.warehouse_path.read_bytes()
        monkeypatch.setenv("FAKE_DBT_KEEP_WAL", "1")

        def broken(path):
            raise duckdb.IOException("no space left on device")

        monkeypatch.setattr(transform, "_checkpoint", broken)
        assert transform.main(["build"]) == 1
        assert settings.warehouse_path.read_bytes() == before
        assert _leftovers() == []

    def test_an_interrupt_deletes_the_private_file(self, fake_dbt, monkeypatch):
        _seed_live()
        before = settings.warehouse_path.read_bytes()

        def interrupted(cmd, env):
            with duckdb.connect(env["SDP_WAREHOUSE"]) as con:
                con.execute("create table half_done as select 1 as n")
            raise KeyboardInterrupt

        monkeypatch.setattr(transform.subprocess, "call", interrupted)
        with pytest.raises(KeyboardInterrupt):
            transform.main(["build"])
        assert settings.warehouse_path.read_bytes() == before
        assert _leftovers() == []
        assert not transform._lock_dir().exists()

    @pytest.mark.parametrize("argv", [["--debug", "build"], ["retry"]])
    def test_a_command_the_runner_cannot_serve_is_refused(self, fake_dbt, tmp_path,
                                                          monkeypatch, argv):
        log = tmp_path / "dbt.log"
        monkeypatch.setenv("FAKE_DBT_LOG", str(log))
        assert transform.main(argv) == 2
        assert not log.exists(), "dbt must not start"


class TestLock:
    def test_a_live_holder_blocks_a_second_run(self, fake_dbt):
        _seed_live()
        before = settings.warehouse_path.read_bytes()
        lock = transform._lock_dir()
        lock.mkdir(parents=True)
        (lock / "pid").write_text(str(os.getppid()))
        assert transform.main(["build"]) == 1
        assert settings.warehouse_path.read_bytes() == before
        assert lock.exists(), "the lock of a live process stays"

    def test_the_lock_of_a_dead_process_is_reclaimed(self, fake_dbt):
        lock = transform._lock_dir()
        lock.mkdir(parents=True)
        (lock / "pid").write_text(str(_dead_pid()))
        assert transform.main(["build"]) == 0
        assert not lock.exists(), "the run releases the lock"

    def test_the_files_of_a_killed_run_are_deleted(self, fake_dbt):
        wh = settings.warehouse_path.parent
        wh.mkdir(parents=True)
        orphans = [wh / "sdp.build-99", wh / "sdp.build-98.duckdb",
                   wh / "sdp.build-97.duckdb.wal"]
        (orphans[0] / "sdp.duckdb.tmp").mkdir(parents=True)
        (orphans[0] / "sdp.duckdb").write_bytes(b"x")
        orphans[1].write_bytes(b"x")
        orphans[2].write_bytes(b"x")
        assert transform.main(["parse"]) == 0
        assert not any(p.exists() for p in orphans)


class TestStatus:
    def test_the_status_dates_the_build_from_the_stamp(self, fake_dbt):
        assert transform.main(["build"]) == 0
        published = transform.last_build()["published_utc"]
        dbt = daily.status_snapshot()["dbt"]
        assert dbt["built"] == published[:10]
        assert dbt["detail"] == f"built {published[:10]}"
        assert dbt["stale"] is False

    def test_a_warehouse_with_no_stamp_reads_never_built(self, fake_dbt):
        _seed_live()
        dbt = daily.status_snapshot()["dbt"]
        assert dbt["detail"] == "never built"
        assert dbt["stale"] is True

    @pytest.mark.parametrize("text", ["[]", "{}", '{"published_utc": 5}', "not json"])
    def test_a_stamp_that_is_not_valid_reads_never_built(self, fake_dbt, text):
        _seed_live()
        transform.stamp_path().write_text(text, encoding="utf-8")
        assert transform.last_build() is None
        assert daily.status_snapshot()["dbt"]["detail"] == "never built"
