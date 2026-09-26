# src/sdp/config.py
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

    # The Kenneth French Data Library. It is public and needs no key.
    french_base_url: str = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"

    data_root: Path = _REPO_ROOT / "data"

    # DuckDB in a dbt build. Above the memory limit, DuckDB writes to disk, which costs
    # little. A higher limit lets the system compress or swap the build when other
    # programs need memory, and that makes the build many times slower. One model
    # already uses every core, so one dbt thread builds fastest.
    dbt_memory_limit: str = "4GB"
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