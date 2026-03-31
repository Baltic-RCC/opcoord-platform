from functools import lru_cache
from dataclasses import dataclass
import uuid
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import SettingsConfigDict, BaseSettings
from config.integrations import ElasticSettings, RabbitMqSettings


class WorkerSettings(BaseSettings):
    worker_name: str = "card-publicator"
    worker_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rmq_queue_in: str = "card_publication_queue"


class BusinessSettings(BaseSettings):
    debug: bool = False


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