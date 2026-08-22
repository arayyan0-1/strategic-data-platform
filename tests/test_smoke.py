"""Import every module and load Settings.

These tests find one class of error that occurred twice already. A path
resolves against the working directory and not against the repo root. dbt and
Dagster start from their own directories, so this error is not hypothetical.
"""
import importlib

import pytest

MODULES = [
    "sdp.config",
    "sdp.dal",
    "sdp.backfill",
    "sdp.ingest.rest",
    "sdp.ingest.massive_day_aggs",
    "sdp.ingest.massive_corporate_actions",
    "sdp.ingest.massive_tickers",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name):
    importlib.import_module(name)


def test_settings_paths_are_absolute_and_under_data_root():
    from sdp.config import settings

    assert settings.data_root.is_absolute()
    for path in (settings.vendor_dir, settings.raw_dir,
                 settings.staging_dir, settings.warehouse_path):
        assert path.is_absolute()
        assert settings.data_root in path.parents


def test_every_dal_dataset_has_a_known_partition_key():
    from sdp import dal

    for ds in dal.DATASETS.values():
        assert ds.key in ("date", "pull_date")


def test_backfill_targets_are_all_event_streams():
    """Check that the backfill runner cannot reach a pull_date dataset."""
    from sdp import backfill, dal

    by_module = {"day_aggs": dal.DAY_AGGS, "tickers": dal.TICKERS}
    for name in backfill.TARGETS:
        assert by_module[name].key == "date"
