# src/sdp/ingest/massive_corporate_actions.py
"""Splits and dividends, stored as an append-only snapshot log.

The two endpoints give current state. Each pull is the full present belief of
the vendor about all of history, and consecutive pulls repeat almost all of
it. Measured on 2026-08-23: the pull was byte for byte identical to the pull
of 2026-08-22, and the full-snapshot format still stored 38 MB of Parquet to
record that nothing happened.

The published partition for a pull therefore holds the change and not the
snapshot. A row with op = 'add' entered the snapshot on that pull. A row with
op = 'close' left it. A pull that changes nothing publishes a partition with
zero rows, and that partition still records that the pull happened. Replay of
the log through a pull gives the exact snapshot of that pull. dal.snapshot()
does the replay. See docs/decisions/0015-the-snapshot-log.md.

Row identity is row_hash, an md5 over every vendor column in name order. The
vendor id cannot be the identity, because the vendor regenerates ids between
pulls. The event key cannot be the identity, because it is not unique inside
one pull. Content is the only identity that reconstructs a snapshot exactly.

Two consequences follow from the log form.

1. A partition depends on every partition before it. The log therefore grows
   in strict pull order. append() refuses a pull date before the newest
   published partition.
2. The recovery path is rebuild(). It replays every vendor file in order,
   through the same audits and the same append. A doubt about one partition
   is a doubt about the chain, and the chain rebuilds from vendor/ with a
   deterministic result.

The flow of one pull is Write-Audit-Publish, with one more audit than the
event streams have:

1. Fetch the full pull into vendor/, as gzip NDJSON.
2. build() the full snapshot into _staging/. This file is the audit surface
   and the diff input. It is never published.
3. The audits run on the full snapshot.
4. append() diffs the snapshot against the replayed prior state, verifies
   that prior state plus the staged change replays back to the snapshot
   exactly, and only then publishes the change with os.replace.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import re
import shutil
from pathlib import Path

import duckdb

from sdp import dal
from sdp.config import settings
from sdp.ingest.rest import dump_ndjson

log = logging.getLogger(__name__)

SPECS = {
    "massive_splits": {
        "path": "/stocks/v1/splits",
        "params": {"limit": 5000, "sort": "execution_date.asc"},
    },
    "massive_dividends": {
        "path": "/stocks/v1/dividends",
        "params": {"limit": 5000, "sort": "ex_dividend_date.asc"},
    },
}

_BOOKKEEPING = ("pull_date", "op", "row_hash")
"""Columns that the log adds. The row hash covers the vendor columns only."""


def raw_path(dataset: str, pull_date: dt.date) -> Path:
    return settings.raw_dir / dataset / f"pull_date={pull_date:%Y-%m-%d}" / "data.parquet"


def _one(sql: str, con: duckdb.DuckDBPyConnection | None = None) -> tuple:
    row = (con or duckdb).execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"The query returned no rows: {sql[:120]}")
    return row


class AuditFailure(RuntimeError):
    pass


class ChainError(RuntimeError):
    """The pull order would break the log.

    Each partition holds the change against the partition before it, so a
    partition depends on every partition before it. A pull can only extend
    the log or replace its newest entry. To change an older entry, run
    rebuild() and replay the whole log from vendor/.
    """


# ---------- STAGE THE SNAPSHOT ----------

def build(dataset: str, vendor_file: Path, pull_date: dt.date) -> Path:
    """Stage the full snapshot of one pull. The audits read this file.

    append() then reduces it to the change against the prior state. The full
    snapshot itself is never published.
    """
    staged = settings.staging_dir / dataset / f"{pull_date:%Y-%m-%d}.snapshot.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)

    duckdb.execute(f"""
        copy (
            select
                *,
                date '{pull_date:%Y-%m-%d}' as pull_date
            from read_json(
                '{vendor_file}',
                format = 'newline_delimited',
                sample_size = -1
            )
        ) to '{staged}' (format parquet, compression zstd)
    """)
    return staged

# ---------- AUDIT ----------

MAX_NEW_BAD_TICKERS = 3   # Tickers that become defective in one pull.
RATIO_HI, RATIO_LO = 10000.0, 0.0001


def _audit_common(staged: Path, dataset: str) -> int:
    n_rows, n_null_id, n_null_ticker = _one(f"""
        select count(*),
               count(*) filter (id is null),
               count(*) filter (ticker is null)
        from read_parquet('{staged}')
    """)

    problems = []
    if n_rows == 0:
        problems.append("zero rows")
    if n_null_id:
        problems.append(f"{n_null_id} null ids")
    if n_null_ticker:
        problems.append(f"{n_null_ticker} null tickers")

    dupes = _one(
        f"select count(*) from (select id from read_parquet('{staged}') "
        f"group by id having count(*) > 1)"
    )[0]
    if dupes:
        problems.append(f"{dupes} duplicate ids")

    if problems:
        raise AuditFailure(f"{dataset}: " + ". ".join(problems) + ".")
    return n_rows


def _audit_splits(staged: Path) -> None:
    (null_past, null_future, zero_f, neg_f, null_date,
     bad_fwd, bad_rev, bad_stk, bad_from, bad_to, extreme) = _one(f"""
        select
            count(*) filter (historical_adjustment_factor is null
                             and execution_date <= current_date),
            count(*) filter (historical_adjustment_factor is null
                             and execution_date  > current_date),
            count(*) filter (historical_adjustment_factor = 0),
            count(*) filter (historical_adjustment_factor < 0),
            count(*) filter (execution_date is null),
            count(*) filter (adjustment_type = 'forward_split'
                             and split_to <= split_from),
            count(*) filter (adjustment_type = 'reverse_split'
                             and split_to >= split_from),
            count(*) filter (adjustment_type = 'stock_dividend'
                             and split_to <= split_from),
            count(*) filter (split_from is null or split_from <= 0),
            count(*) filter (split_to   is null or split_to   <= 0),
            count(*) filter (split_to / split_from > {RATIO_HI}
                             or split_to / split_from < {RATIO_LO})
        from read_parquet('{staged}')
    """)

    fatal = []
    if null_date:
        fatal.append(f"{null_date} null execution_date")
    if null_past:
        fatal.append(f"{null_past} null factors on already-executed events")
    for label, n in [("forward_split", bad_fwd), ("reverse_split", bad_rev),
                     ("stock_dividend", bad_stk)]:
        if n:
            fatal.append(f"{n} {label} rows have a ratio in the wrong direction")
    if bad_from or bad_to:
        fatal.append(f"{bad_from + bad_to} rows have an invalid split ratio")

    if fatal:
        raise AuditFailure("splits: " + ". ".join(fatal) + ".")

    unusable = zero_f + neg_f
    if unusable:
        log.warning("splits: %s factors are not positive. Staging removes these rows.",
                    unusable)
    if null_future:
        log.info("splits: %s events are pending. The vendor gives no factor for them.",
                 null_future)
    if extreme:
        log.warning("splits: %s events have a ratio outside the limits [%s, %s].",
                    extreme, RATIO_LO, RATIO_HI)


def _audit_dividends(staged: Path) -> None:
    (null_ex, bad_cash,
     null_past, null_past_tickers, null_future,
     bad_factor, bad_factor_tickers,
     non_usd, bad_freq) = _one(f"""
        select
            -- Fatal. These rows are unusable.
            count(*) filter (ex_dividend_date is null),
            count(*) filter (cash_amount is null or cash_amount < 0),

            -- Expected. The vendor has no price to compute a factor with.
            count(*) filter (historical_adjustment_factor is null
                             and ex_dividend_date <= current_date),
            count(distinct ticker) filter (historical_adjustment_factor is null
                                           and ex_dividend_date <= current_date),
            count(*) filter (historical_adjustment_factor is null
                             and ex_dividend_date > current_date),

            -- Known defects. The delta check judges these, not the count.
            count(*) filter (historical_adjustment_factor <= 0),
            count(distinct ticker) filter (historical_adjustment_factor <= 0),

            -- Schema tripwires.
            count(*) filter (currency is not null and currency <> 'USD'),
            count(*) filter (frequency is null or frequency not in (0,1,2,3,4,12,24,52,104,365))
        from read_parquet('{staged}')
    """)

    fatal = []
    if null_ex:
        fatal.append(f"{null_ex} rows have a null ex_dividend_date")
    if bad_cash:
        fatal.append(f"{bad_cash} rows have a null or negative cash_amount")
    if fatal:
        raise AuditFailure("dividends: " + ". ".join(fatal) + ".")

    # The dividend factor is different from the split factor. To compute it,
    # the vendor needs a price on the ex-date, as a product of (1 - D/P)
    # terms. A null factor therefore means that the vendor has no price for
    # that security. This is structural. Most of these rows are old or
    # delisted. It is not fatal.
    if null_past:
        log.info("dividends: %s null factors on past ex-dates, on %s tickers. "
                 "The vendor has no price to compute them with.",
                 null_past, null_past_tickers)
    if null_future:
        log.info("dividends: %s are announced and not yet ex.", null_future)
    if bad_factor:
        log.warning("dividends: %s factors are not positive, on %s tickers. "
                    "The delta check finds the new ones.",
                    bad_factor, bad_factor_tickers)
    if non_usd:
        log.info("dividends: %s rows are not in USD. Staging removes them.", non_usd)
    if bad_freq:
        log.warning("dividends: %s rows have an unexpected frequency value.", bad_freq)


def _audit_vs_previous(dataset: str, staged: Path, pull_date: dt.date, n_rows: int) -> None:
    """Compare the staged snapshot against the replayed previous state.

    The sharp check is the delta between two pulls and not the absolute
    count. An absolute threshold on a dataset that grows becomes meaningless
    or noisy over time. The delta check stays sharp for ever.
    """
    ds = dal.DATASETS[dataset]
    priors = [p for p in dal.partitions(ds) if p < pull_date]
    if not priors:
        log.info("%s: first pull. %s rows. There is no baseline.", dataset, n_rows)
        return

    c = dal.con()
    c.register("_ca_prev_state", dal._state(ds, priors[-1]))
    try:
        prev_n = _one("select count(*) from _ca_prev_state", c)[0]
        if n_rows < prev_n * 0.99:
            raise AuditFailure(
                f"{dataset}: {n_rows} rows against {prev_n} in the previous pull. "
                f"The history became smaller."
            )

        new_bad = c.execute(f"""
            select ticker, count(*) as n
            from read_parquet('{staged}')
            where historical_adjustment_factor <= 0
              and ticker not in (
                  select ticker from _ca_prev_state
                  where historical_adjustment_factor <= 0
              )
            group by ticker order by n desc
        """).fetchall()

        if len(new_bad) > MAX_NEW_BAD_TICKERS:
            raise AuditFailure(
                f"{dataset}: {len(new_bad)} tickers became defective in this pull: "
                f"{new_bad[:10]}"
            )
        if new_bad:
            log.warning("%s: these tickers have new factors that are not positive: %s",
                        dataset, new_bad)

        log.info("%s: %s rows. Change of %s against the pull of %s.",
                 dataset, n_rows, n_rows - prev_n, priors[-1])
    finally:
        c.unregister("_ca_prev_state")

# ---------- APPEND AND PUBLISH ----------

def _hash_expr(columns: list[str]) -> str:
    """Return the row_hash expression over the vendor columns, in name order.

    Name order makes the hash independent of the column order that read_json
    infers. The bookkeeping columns stay out, because the hash identifies
    content and the same content must hash the same on every pull.
    """
    keep = sorted(c for c in columns if c not in _BOOKKEEPING)
    packed = ", ".join(f'"{c}" := "{c}"' for c in keep)
    return f"md5(to_json(struct_pack({packed})))"


def append(dataset: str, snapshot: Path, pull_date: dt.date) -> Path:
    """Diff the staged snapshot against the prior state, verify, publish.

    The published partition holds the change only. Before the partition
    exists, this function replays prior state plus the staged change and
    requires the result to equal the snapshot row for row. A partition that
    would not replay correctly is never published.
    """
    ds = dal.DATASETS[dataset]
    dest = raw_path(dataset, pull_date)
    parts = dal.partitions(ds)
    later = [p for p in parts if p > pull_date]
    if later:
        raise ChainError(
            f"{dataset}: cannot publish {pull_date}. The log already has "
            f"{len(later)} newer partition(s), up to {later[-1]}. Each partition "
            f"depends on every partition before it. Run rebuild() to replay the "
            f"log from vendor/."
        )
    priors = [p for p in parts if p < pull_date]

    c = dal.con()
    cols = c.read_parquet(str(snapshot)).columns
    # The hash covers the vendor columns only, so the view drops pull_date
    # before hashing and the log write adds it back with the value of this
    # pull. build() writes the column and a test fixture may not.
    select = "* exclude (pull_date)" if "pull_date" in cols else "*"
    c.execute(f"""
        create or replace temp view _ca_pull as
        select {select}, {_hash_expr(cols)} as row_hash
        from read_parquet('{snapshot}')
    """)
    if priors:
        c.register("_ca_prior", dal._state(ds, priors[-1], keep_hash=True))
    else:
        c.execute(f"create or replace temp view _ca_prior as "
                  f"select *, date '{pull_date:%Y-%m-%d}' as pull_date "
                  f"from _ca_pull where 1 = 0")

    staged = settings.staging_dir / dataset / f"{pull_date:%Y-%m-%d}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)
    try:
        c.execute(f"""
            copy (
                select *, date '{pull_date:%Y-%m-%d}' as pull_date,
                       'add' as op
                from _ca_pull
                where row_hash not in (select row_hash from _ca_prior)
                union all by name
                select * replace (date '{pull_date:%Y-%m-%d}' as pull_date),
                       'close' as op
                from _ca_prior
                where row_hash not in (select row_hash from _ca_pull)
            ) to '{staged}' (format parquet, compression zstd)
        """)

        files = [str(ds.partition_file(p)) for p in priors] + [str(staged)]
        n_pull, n_replay, only_replay, only_pull = _one(f"""
            with recon as ({dal.replay_sql(files, keep_hash=True)})
            select
                (select count(*) from _ca_pull),
                (select count(*) from recon),
                (select count(*) from
                    ((select row_hash from recon)
                     except (select row_hash from _ca_pull))),
                (select count(*) from
                    ((select row_hash from _ca_pull)
                     except (select row_hash from recon)))
        """, c)
        if n_pull != n_replay or only_replay or only_pull:
            raise AuditFailure(
                f"{dataset}: the log does not replay to the snapshot of "
                f"{pull_date}. Snapshot {n_pull} rows, replay {n_replay} rows, "
                f"{only_pull} missing, {only_replay} extra. The partition was "
                f"not published."
            )

        n_add, n_close = _one(
            f"select count(*) filter (op = 'add'), count(*) filter (op = 'close') "
            f"from read_parquet('{staged}')", c)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, dest)
        if n_add or n_close:
            log.info("Published %s: %s added, %s closed.", dest, n_add, n_close)
        else:
            log.info("Published %s: no change against the previous pull.", dest)
        return dest
    finally:
        staged.unlink(missing_ok=True)
        c.execute("drop view if exists _ca_pull")
        if priors:
            c.unregister("_ca_prior")
        else:
            c.execute("drop view if exists _ca_prior")


def _publish_pull(dataset: str, vendor_file: Path, pull_date: dt.date) -> Path:
    """Run one pull through build, the audits and append."""
    snapshot = build(dataset, vendor_file, pull_date)
    try:
        n_rows = _audit_common(snapshot, dataset)
        (_audit_splits if dataset == "massive_splits" else _audit_dividends)(snapshot)
        _audit_vs_previous(dataset, snapshot, pull_date, n_rows)
        return append(dataset, snapshot, pull_date)
    finally:
        snapshot.unlink(missing_ok=True)


def ingest(dataset: str, pull_date: dt.date | None = None, *, force: bool = False) -> Path | None:
    pull_date = pull_date or dt.datetime.now(dt.UTC).date()
    dest = raw_path(dataset, pull_date)
    if dest.exists() and not force:
        log.info("The pull for today is already published: %s", dest)
        return dest

    spec = SPECS[dataset]
    vendor_file = dump_ndjson(dataset, spec["path"], spec["params"], pull_date,
                              force=force, compress=True)
    return _publish_pull(dataset, vendor_file, pull_date)

# ---------- REBUILD ----------

_VENDOR_NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})\.ndjson(\.gz)?$")


def vendor_pulls(dataset: str) -> list[tuple[dt.date, Path]]:
    """Return every vendor pull file with its date, in pull order."""
    vdir = settings.vendor_dir / dataset
    if not vdir.exists():
        return []
    found = {}
    for p in sorted(vdir.iterdir()):
        m = _VENDOR_NAME.match(p.name)
        if m:
            found[dt.date.fromisoformat(m.group(1))] = p
    return sorted(found.items())


def rebuild(dataset: str) -> None:
    """Build the whole log again from vendor/, in pull order.

    This is the recovery path of the log form. A partition depends on every
    partition before it, so a doubt about one partition is a doubt about the
    chain. The replay runs every pull through the same audits and the same
    append as the daily run, so a rebuilt log is identical in content to a
    log that grew one day at a time.
    """
    pulls = vendor_pulls(dataset)
    if not pulls:
        raise FileNotFoundError(
            f"{dataset}: no vendor files in {settings.vendor_dir / dataset}. "
            f"A rebuild replays vendor/ and cannot run without it."
        )
    root = settings.raw_dir / dataset
    if root.exists():
        # The guard keeps a path error from deleting outside the lake.
        assert root.is_relative_to(settings.raw_dir)
        shutil.rmtree(root)
        log.info("%s: removed the published log. The replay builds it again.", dataset)

    for pull_date, vendor_file in pulls:
        _publish_pull(dataset, vendor_file, pull_date)
    log.info("%s: replayed %s pulls from vendor/.", dataset, len(pulls))


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    names = [t for t in sys.argv[1:] if not t.startswith("-")] or list(SPECS)
    for name in names:
        if "--rebuild" in sys.argv:
            rebuild(name)
        else:
            ingest(name, force="--force" in sys.argv)
