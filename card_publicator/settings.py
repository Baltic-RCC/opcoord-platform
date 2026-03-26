from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import SettingsConfigDict, BaseSettings
from config.integrations import ElasticSettings, RabbitMqSettings


class BusinessSettings(BaseSettings):
    rmq_queue_in: str = "test"


@dataclass(frozen=True)
class PublicatorConfig:
    elastic: ElasticSettings
    rmq: RabbitMqSettings
    publicator: BusinessSettings


@lru_cache(maxsize=1)
def get_settings() -> PublicatorConfig:
    return PublicatorConfig(
        elastic=ElasticSettings(),
        rmq=RabbitMqSettings(),
        publicator=BusinessSettings(),
    )


if __name__ == "__main__":
    conf = get_settings()
    print(conf)