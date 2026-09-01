"""Import every module and verify that Settings resolves absolute paths.
dbt and Jupyter start from their own directories, so relative paths break.
"""
import importlib
from pathlib import Path

import pytest

MODULES = [
    "sdp.config",
    "sdp.dal",
    "sdp.restatement",
    "sdp.diagnostics",
    "sdp.transform",
    "sdp.backfill",
    "sdp.ingest.rest",
    "sdp.ingest.massive_day_aggs",
    "sdp.ingest.massive_corporate_actions",
    "sdp.ingest.massive_tickers",
    "sdp.ingest.massive_short",
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


def test_every_dal_dataset_has_a_known_kind():
    """An event stream keys on 'date'. A current-state dataset has no key."""
    from sdp import dal

    for ds in dal.DATASETS.values():
        assert ds.key in ("date", None)


def test_backfill_targets_are_all_event_streams():
    """Check that the backfill runner cannot reach a current-state dataset."""
    from sdp import backfill, dal

    by_module = {
        "day_aggs": dal.DAY_AGGS,
        "tickers": dal.TICKERS,
        "short_volume": dal.SHORT_VOLUME,
        "short_interest": dal.SHORT_INTEREST,
    }
    # Every target must appear here, so that a new target cannot enter the
    # runner without a statement of its kind.
    assert set(backfill.TARGETS) == set(by_module)
    for name in backfill.TARGETS:
        assert by_module[name].key == "date"


def test_the_dbt_runner_gives_dbt_the_repo_paths():
    """dbt starts from its own directory. The lake path must not be relative."""
    from sdp import transform
    from sdp.config import settings

    e = transform.env()
    assert e["SDP_DATA_ROOT"] == str(settings.data_root)
    assert e["SDP_WAREHOUSE"] == str(settings.warehouse_path)
    assert Path(e["SDP_DATA_ROOT"]).is_absolute()
    assert transform.PROJECT_DIR.name == "transform"
