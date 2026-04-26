from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from core.constants import (
    DEBOUNCE_SECONDS,
    DEFAULT_WORKER_COUNT,
    HEALTH_CHECK_INTERVAL,
    MAX_BATCH_SIZE,
    MAX_DATASET_RECORDS,
    MAX_QUEUE_SIZE,
    MAX_RETRIES,
    MAX_TRANSLATION_CHARS,
    MAX_WORKER_COUNT,
    MIN_CONFIDENCE_SCORE,
    MIN_DETECTION_CONFIDENCE,
    QUEUE_POLL_INTERVAL,
    RATE_LIMIT_SUBMIT_JOB,
    RATE_LIMIT_TRANSLATE,
    RECORDS_PER_SHARD,
    SECRET_CACHE_TTL_SECONDS,
    TRANSLATION_TIMEOUT_SECONDS,
)
from core.enums import Language, TranslationBackend
from core.exceptions import ConfigrationError

#Config loader----------------------------------------------------------

def _load_yaml(path: Path) -> dict:
    """load a yaml file from disk safely
    raises configurationerror with a clear message if file is missing
    or contains invalid YAML syntax
    """
    if not path.exists():
        raise ConfigrationError(
            f"Config file not found: {path}",
            details={"path": str(path)},
        )
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ConfigrationError(
            f"YAML syntax error in {path}",
            details={"path": str(path), "error": str(e)},
        ) from e


def _merge(base: dict, override: dict) -> dict:
    """Deep merge override on top of base
    same key: overridr wins.
    key only in base: kept as is.
    key only in override: added.
    nested dicts are merged recursively.
    """
    merged = base.copy()
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged

def load_config(env: str="dev") -> dict:
    """Load and merge config.yaml + environments/{env}.yaml.
    Returns the merged dict ready for settings validation
    """
    root = Path(__file__).parent.parent
    base = _load_yaml(root / "config" / "config.yaml")
    env_file = root / "config" / "environments" / f"{env}.yaml"
    override = _load_yaml(env_file)
    return _merge(base, override)

#setting classes--------------------------------------------------------

class PlatformSettings(BaseSettings):
    """Top-level platform identify settings"""
    name: str = "tamil-eda-platform"
    version: str = "2.0.0"
    debug: bool = False
    max_workers: int = Field(default=DEFAULT_WORKER_COUNT, ge=1)
    model_config = SettingsConfigDict(extra="ignore")

    @field_validator("max_workers")
    @classmethod
    def workers_within_limit(cls, v: int) -> int:
        if v > MAX_WORKER_COUNT:
            raise ValueError(
                f"max_workers {v} exceeds hard limit {MAX_WORKER_COUNT}"
            )
        return v

    @field_validator("debug")
    @classmethod
    def no_debug_in_prod(cls, v: bool) -> bool:
        # Additinal runtime check done in bootstrap.py
        # This validator is a safety net
        import os
        if v is True and os.getenv("ENV", "dev") == "prod":
            raise ValueError(
                "debug mode is not allowed in production -"
                "set debug: false in config/environments/prod.yaml"
            )
        return v


class QueueSettings(BaseSettings):
    """Queue configuration"""
    backend: str = "memory"
    max_size: int = Field(default=MAX_QUEUE_SIZE, ge=100)
    poll_interval_s: float = Field(default=QUEUE_POLL_INTERVAL, ge=0.01)
    drain_timeout_s: int = Field(default=30, ge=1)
    model_config = SettingsConfigDict(extra="ignore")

    @field_validator("backend")
    @classmethod
    def valid_backend(cls, v: str) -> str:
        allowed = {"memory", "redis"}
        if v not in allowed:
            raise ValueError(f"queue backend must be one of {allowed}")
        return v

class StorageSettings(BaseSettings):
    """Storage paths and private settings"""
    output_path: str = "data/output"
    parquet_path: str = "data/parquet"
    archive_path: str = "data/archive"
    compression: str = "snappy"
    partition_by: str = "ingested_date"
    records_per_shard: int = Field(default=RECORDS_PER_SHARD, ge=1000)
    max_dataset_records: int = Field(default=MAX_DATASET_RECORDS, ge=1000)
    model_config = SettingsConfigDict(extra="ignore")

    @field_validator("compression")
    @classmethod
    def valid_compression(cls, v: str) -> str:
        allowed = {"snappy", "gzip", "none"}
        if v not in allowed:
            raise ValueError(f"compression must be one of {allowed}")
        return v

class TranslationSettings(BaseSettings):
    """Translation engine settings"""
    model_config = SettingsConfigDict(extra="ignore")
    default_backend: TranslationBackend = TranslationBackend.GOOGLE
    fallback_order: list[TranslationBackend] = Field(default=[TranslationBackend.DEEPL, TranslationBackend.NLLB])
    default_target_language: Language = Language.TAMIL
    min_quality_threshold: float = Field(default=MIN_CONFIDENCE_SCORE, ge=0.0, le=1.0)
    max_chars_per_request: int = Field(default=MAX_TRANSLATION_CHARS, ge=100, le=10_000)
    timeout_s: int = Field(default=TRANSLATION_TIMEOUT_SECONDS, ge=5)
    max_batch_size: int = Field(default=MAX_BATCH_SIZE, ge=1, le=500)

    @field_validator("fallback_order")
    @classmethod
    def no_mock_in_fallback(cls, v: list[TranslationBackend]) -> list[TranslationBackend]:
        # Enforced strictly in bootstrap.py for prod
        # This is a soft warning at settings level
        if TranslationBackend.MOCK in v:
            raise ValueError("MOCK backend is not allowed in fallback_order - "
                             "use only: google, deepl, nllb")
        return v

class LanguageSettings(BaseSettings):
    """Language detection settings"""
    model_config = SettingsConfigDict(extra="ignore")
    default_target: Language = Language.TAMIL
    confidence_threshold: float = Field(default=MIN_DETECTION_CONFIDENCE, ge=0.0, le=1.0)
    min_sentence_length: int = Field(default=3, ge=1)
    max_sentence_length: int = Field(default=1000, ge=10)

class SecuritySettings(BaseSettings):
    """Security and encryption settings"""
    model_config = SettingsConfigDict(extra="ignore")
    strict_mode: bool = False
    rate_limit_enabled: bool = False
    encrypt_pii: bool = False
    secret_cache_ttl_s: int = Field(default=SECRET_CACHE_TTL_SECONDS, ge=60)

class ObservabilitySettings(BaseSettings):
    """Logging, metrics, and tracing settings"""
    model_config = SettingsConfigDict(extra="ignore")
    log_level: str = "INFO"
    metrics_port: int = Field(default=9090, ge=1024, le=65535)
    trace_enabled: bool = False
    health_check_interval_s: int = Field(default=HEALTH_CHECK_INTERVAL, ge=5)

    @field_validator("log_level")
    @classmethod
    def valid_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()

class IngestionSettings(BaseSettings):
    """File ingestion settings"""
    model_config = SettingsConfigDict(extra="ignore")
    streaming_threshold_mb: int = Field(default=100, ge=1)
    chunk_size: int = Field(default=1000, ge=10)
    retries: int = Field(default=MAX_RETRIES, ge=1, le=10)
    debounce_s: float = Field(default=DEBOUNCE_SECONDS, ge=0.1)

class APISettings(BaseSettings):
    """FastAPI server settings"""
    model_config = SettingsConfigDict(extra="ignore")
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1024, le=65535)
    rate_limit_translate: int = Field(default=RATE_LIMIT_TRANSLATE, ge=1)
    rate_limit_submit_job: int = Field(default=RATE_LIMIT_SUBMIT_JOB, ge=1)

class DatabaseSettings(BaseSettings):
    """Database connection settings"""
    model_config = SettingsConfigDict(extra="ignore")
    url: str = "sqlite:///data/dev.db"
    pool_size: int = Field(default=5, ge=1, le=20)
    max_overflow: int = Field(default=10, ge=0, le=50)
    echo: bool = False

class RedisSettings(BaseSettings):
    """Redis connection settings"""
    model_config = SettingsConfigDict(extra="ignore")
    host: str = "localhost"
    port: int = Field(default=6379, ge=1, le=65535)
    db: int = Field(default=0, ge=0, le=15)
    password: str | None = None

    @property
    def url(self) -> str:
        if self.password:
            return(
                f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
            )
        return f"redis://{self.host}:{self.port}/{self.db}"


class VaultSettings(BaseSettings):
    """Hashicorp Vault settings"""
    model_config = SettingsConfigDict(extra="ignore")
    addr: str = "http://127.0.0.1:8200"
    token: str = "root"
    cache_ttl_s: int = Field(default=SECRET_CACHE_TTL_SECONDS, ge=60)

#Root Settings-----------------------------------------------------------

class AppSettings(BaseSettings):
    """ Root settings object
        Built from merged YAML config by build_settings().
    Every file in the platform imports the singleton below.

    Usage:
        from core.settings import settings
        level = settings.observability.log_level
        threshold = settings.translation.min_quality_threshold
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )
    env: str = Field(default_factory=lambda: os.getenv("ENV", "dev"))
    platform: PlatformSettings = Field(default_factory=PlatformSettings)
    queue:QueueSettings = Field(default_factory=QueueSettings)
    translation: TranslationSettings = Field(default_factory=TranslationSettings)
    language: LanguageSettings = Field(default_factory=LanguageSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    api: APISettings = Field(default_factory=APISettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    Vault: VaultSettings = Field(default_factory=VaultSettings)

def build_settings(env: str=os.getenv("ENV","dev")) -> AppSettings:
    """Load YAML config for the given environment and build AppSettings
        Called once by bootstrap.py at startup.

    Args:
        env: environment name — dev | staging | prod

    Returns:
        Validated AppSettings object

    Raises:
        ConfigurationError: if any config file is missing or invalid
        ValidationError: if any value fails Pydantic validation
    """

    raw =load_config(env)

    return AppSettings(
        env = env,
        platform=PlatformSettings(**raw.get("platform", {})),
        queue = QueueSettings(**raw.get("queue", {})),
        storage=StorageSettings(**raw.get("storage", {})),
        translation=TranslationSettings(**raw.get("translation", {})),
        language=LanguageSettings(**raw.get("language", {})),
        security=SecuritySettings(**raw.get("security", {})),
        observability=ObservabilitySettings(**raw.get("observability", {})),
        ingestion=IngestionSettings(**raw.get("ingestion", {})),
        api=APISettings(**raw.get("api", {})),
        database=DatabaseSettings(**raw.get("database", {})),
        redis=RedisSettings(**raw.get("redis", {})),
        Vault=VaultSettings(**raw.get("vault", {})),

    )

# ── SINGLETON ─────────────────────────────────────────────────────────────────
# Default singleton loaded with dev settings.
# bootstrap.py replaces this with the correct environment at startup:
#   from core.settings import settings
#   settings = build_settings(env='prod')
#
# Every other file imports this object directly:
#   from core.settings import settings

settings: AppSettings = build_settings()

