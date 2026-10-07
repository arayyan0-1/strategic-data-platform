"""Import every module and check that Settings resolves absolute paths."""
import importlib
import pkgutil
from pathlib import Path

import pytest

import sdp

# Discovered, not listed, so a new module is imported too.
MODULES = sorted(m.name for m in pkgutil.walk_packages(sdp.__path__, "sdp."))


def test_discovery_finds_the_known_modules():
    """walk_packages returns nothing, without an error, when the path is wrong."""
    assert {"sdp.dal", "sdp.backfill", "sdp.daily", "sdp.dashboard",
            "sdp.ingest.common", "sdp.ingest.massive_tickers"} <= set(MODULES)


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name):
    importlib.import_module(name)


def test_settings_paths_are_absolute_and_under_data_root():
    """dbt and Jupyter start from their own directories, so a relative path breaks."""
    from sdp.config import settings

    assert settings.data_root.is_absolute()
    for path in (settings.vendor_dir, settings.raw_dir,
                 settings.staging_dir, settings.warehouse_path):
        assert path.is_absolute()
        assert settings.data_root in path.parents


def test_every_dal_dataset_has_a_known_kind():
    from sdp import dal

    for ds in dal.DATASETS.values():
        assert ds.key in ("date", None)


def test_backfill_targets_are_all_event_streams():
    from sdp import backfill, dal

    by_module = {
        "day_aggs": dal.DAY_AGGS,
        "tickers": dal.TICKERS,
        "short_volume": dal.SHORT_VOLUME,
        "short_interest": dal.SHORT_INTEREST,
        "ticker_details": dal.TICKER_DETAILS,
    }
    # A new target must be listed here, so it states its kind.
    assert set(backfill.TARGETS) == set(by_module)
    for name in backfill.TARGETS:
        assert by_module[name].key == "date"


def test_the_dbt_runner_gives_dbt_the_repo_paths():
    from sdp import transform
    from sdp.config import settings

    e = transform.env()
    assert e["SDP_DATA_ROOT"] == str(settings.data_root)
    assert e["SDP_WAREHOUSE"] == str(settings.warehouse_path)
    assert Path(e["SDP_DATA_ROOT"]).is_absolute()
    assert transform.PROJECT_DIR.name == "transform"


def test_the_dbt_profile_reads_the_limits_of_the_runner():
    """A limit that stays in Settings and never reaches the profile has no effect."""
    from sdp import transform
    from sdp.config import settings

    e = transform.env()
    assert e["SDP_DBT_MEMORY_LIMIT"] == settings.dbt_memory_limit
    assert e["SDP_DBT_THREADS"] == str(settings.dbt_threads)
    profile = (transform.PROJECT_DIR / "profiles.yml").read_text()
    assert "env_var('SDP_DBT_MEMORY_LIMIT'" in profile
    assert "env_var('SDP_DBT_THREADS'" in profile
