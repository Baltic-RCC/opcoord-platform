from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class OperatorFabricSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.joinpath(".env"),
                                      env_prefix="OPFAB_",
                                      extra="ignore")

    host: str
    username: str
    password: SecretStr


class ElasticSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.joinpath(".env"),
                                      env_prefix="ELASTIC_",
                                      extra="ignore")

    host: str
    api_key: SecretStr
    batch_size: int = 1000


class RabbitMqSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.joinpath(".env"),
                                      env_prefix="RMQ_",
                                      extra="ignore")

    host: str
    port: int = 5672
    vhost: str = "/"
    username: str
    password: SecretStr
    heartbeat: int = 15
