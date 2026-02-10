from pathlib import Path
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    # KIS API
    APP_KEY: str
    SECRET_KEY: str
    BASE_URL: str

    # Account
    CANO: str
    ACNT_PRDT_CD: str

    # Database (asset_us DB 공유)
    DB_HOST: str = "localhost"
    DB_PORT: int = 3307
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str = "asset_us"

    # FMP API
    FMP_API_KEY: str = ""

    # theme_analyzer SQLite 경로
    THEME_DB_PATH: str = "c:/theme_analyzer/data/theme_analyzer.db"

    # 분석 파라미터
    HIGH_BREAKOUT_DAYS: int = 60
    RISE_FROM_LOW_MIN_PCT: float = 50.0
    RISE_FROM_LOW_MAX_PCT: float = 100.0
    CONSOLIDATION_MIN_DAYS: int = 10
    CONSOLIDATION_MAX_RANGE_PCT: float = 10.0
    VOLUME_RATIO_MIN: float = 2.0
    VOLUME_RATIO_DAYS: int = 10
    LOOKBACK_MONTHS: int = 120

    # 실적 필터
    FUNDAMENTAL_FILTER: bool = True
    FUNDAMENTAL_GROWTH_YEARS: int = 1

    model_config = {
        "env_file": str(BASE_DIR / ".env"),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }
