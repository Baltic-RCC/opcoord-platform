from functools import lru_cache
from dataclasses import dataclass
import uuid
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import SettingsConfigDict, BaseSettings
from typing import ClassVar
from config.integrations import ElasticSettings, RabbitMqSettings, MinioSettings


class WorkerSettings(BaseSettings):
    worker_name: str = "card-publicator"
    worker_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rmq_queue_in: str = "opcoord.cards.publish"


class BusinessSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.parent.joinpath("config/.env"),
                                      env_prefix="PUBLICATOR_",
                                      extra="ignore")

    cards_index: str = "dev-opcoord-cards"
    publisher: ClassVar[str] = "38X-BALTIC-RSC-H"
    enable_s3_content_storage: bool = True
    s3_bucket_name: str = "analyses"
    debug: bool = False
    enrichment_strict: bool = False
    enrichment_verbose_logging: bool = True


@dataclass(frozen=True)
class PublicatorConfig:
    elastic: ElasticSettings
    rmq: RabbitMqSettings
    minio: MinioSettings
    publicator: BusinessSettings


@lru_cache(maxsize=1)
def get_settings() -> PublicatorConfig:
    return PublicatorConfig(
        elastic=ElasticSettings(),
        rmq=RabbitMqSettings(),
        minio=MinioSettings(),
        publicator=BusinessSettings(),
    )


if __name__ == "__main__":
    conf = get_settings()
    print(conf)