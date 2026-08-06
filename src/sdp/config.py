# src/sdp/config.py
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    massive_s3_access_key_id: str
    massive_s3_secret_access_key: str
    massive_api_key: str = ""

    massive_s3_endpoint: str = "https://files.polygon.io"
    massive_s3_bucket: str = "flatfiles"

    data_root: Path = Path("data")

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