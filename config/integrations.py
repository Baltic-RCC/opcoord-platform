from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional
from enum import Enum


# Used for Minio authentication mode selection
class AuthMode(str, Enum):
    LDAP = "ldap"
    API_KEY = "api_key"


class OperatorFabricSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.joinpath(".env"),
                                      env_prefix="OPFAB_",
                                      extra="ignore")

    host: str
    username: str
    password: SecretStr
    ssl_verify: bool = False


class ElasticSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.joinpath(".env"),
                                      env_prefix="ELASTIC_",
                                      extra="ignore")

    host: str
    api_key: SecretStr
    batch_size: int = 1000
    ssl_verify: bool = True


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


class MinioSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.joinpath(".env"),
        env_prefix="MINIO_",
        extra="ignore"
    )

    host: str
    username: str
    password: Optional[SecretStr] = None
    api_key: Optional[SecretStr] = None  # Optional now — only required for API key mode
    token_expiration: int = 86400
    token_renew_margin: int = 120
    maxsize: int = 50

    @model_validator(mode="after")
    def check_auth_configured(self) -> "MinioSettings":
        """Ensure at least one valid auth method is configured."""
        if not self.password and not self.api_key:
            raise ValueError(
                "No valid auth configuration found. "
                "Set MINIO_PASSWORD for LDAP mode or MINIO_API_KEY for API key mode."
            )
        return self

    @property
    def auth_mode(self) -> AuthMode:
        """LDAP takes priority if both are configured."""
        if self.password:
            return AuthMode.LDAP
        return AuthMode.API_KEY