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

    data_root: Path = _REPO_ROOT / "data"

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
