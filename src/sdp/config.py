"""Settings loaded from environment variables and the env file at the repo root."""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    massive_s3_access_key_id: str
    massive_s3_secret_access_key: str
    massive_api_key: str

    massive_s3_endpoint: str = "https://files.polygon.io"
    massive_s3_bucket: str = "flatfiles"

    massive_api_base: str = "https://api.massive.com"

    # Public sources, no key needed.
    french_base_url: str = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    fred_csv_url: str = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    # The page links the file of the latest q-factors vintage.
    qfactors_page_url: str = "https://global-q.org/factors.html"

    # IBKR's public short-stock file. ftp3 accepts the connection but never greets.
    ibkr_ftp_host: str = "ftp2.interactivebrokers.com"

    # Issuer fund data. Public, no key.
    ishares_base_url: str = "https://www.ishares.com"
    ishares_screener_url: str = (
        "https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn"
        "?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/"
        "ishares-product-screener-backend-config&siteEntryPassthrough=true"
    )
    ssga_fundfinder_url: str = (
        "https://www.ssga.com/bin/v1/ssmp/fund/fundfinder"
        "?country=us&language=en&role=intermediary&product=etfs&ui=fund-finder"
    )
    ssga_fund_data_url: str = "https://www.ssga.com/library-content/products/fund-data/etfs/us"

    data_root: Path = _REPO_ROOT / "data"
    seeds_dir: Path = _REPO_ROOT / "transform" / "seeds"

    # Above this DuckDB spills to disk; higher lets macOS swap the build.
    dbt_memory_limit: str = "4GB"
    # One model already uses every core.
    dbt_threads: int = 1

    @property
    def vendor_dir(self) -> Path:
        return self.data_root / "vendor"

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def staging_dir(self) -> Path:
        return self.data_root / "_staging"

    @property
    def warehouse_path(self) -> Path:
        return self.data_root / "warehouse" / "sdp.duckdb"


settings = Settings() # type: ignore[call-arg]
