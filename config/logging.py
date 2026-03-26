from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


class Logging(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOGS_", extra="ignore")

    format: str = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name: <40}</cyan> | "
        "<cyan>{function: <40}</cyan> | "
        "<cyan>{line: <4}</cyan> | "
        "<level>{message}</level>"
    )
    level: int = 20
    forward_std: bool = True
    exclude_loggers: List[str] = ["pika"]
    enqueue: bool = False
    catch: bool = False
    elastic_handler: bool = False
    elastic_index: str = "rao-logs"


if __name__ == "__main__":
    conf = Logging()
    print(conf)