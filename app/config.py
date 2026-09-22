"""应用配置：全部可通过环境变量覆盖。"""
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Redis 连接串
    redis_url: str = Field(
        default="redis://localhost:6379/0", validation_alias="REDIS_URL"
    )
    # Redis 键前缀，便于多环境共用一个实例
    key_prefix: str = Field(default="lease", validation_alias="LEASE_KEY_PREFIX")
    # TTL 合法区间（毫秒），超出范围的请求会被拒绝
    min_ttl_ms: int = Field(default=100, validation_alias="LEASE_MIN_TTL_MS")
    max_ttl_ms: int = Field(default=300_000, validation_alias="LEASE_MAX_TTL_MS")
    # 请求未携带 ttl_ms 时使用的默认值
    default_ttl_ms: int = Field(
        default=10_000, validation_alias="LEASE_DEFAULT_TTL_MS"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
