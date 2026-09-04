from functools import lru_cache
from dataclasses import dataclass
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import Field
from pydantic_settings import SettingsConfigDict, BaseSettings
from config.integrations import ElasticSettings, OperatorFabricSettings


class WorkerSettings(BaseSettings):
    worker_name: str = "business-data-exchange"
    worker_id: str = Field(default_factory=lambda: str(uuid.uuid4()))


class BusinessSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.parent.joinpath("config/.env"),
                                      env_prefix="EXCHANGE_",
                                      extra="ignore")

    # CSA input indices in Elastic (same patterns as used by card_publicator enrichment)
    contingencies_index: str = "csa-contingencies*"
    assessed_elements_index: str = "csa-assessed-elements*"
    remedial_actions_index: str = "csa-remedial-actions*"

    # Business day selection: worker runs before 1D CROSA -> target day is tomorrow
    target_day_offset: int = 1

    # Field on CSA input documents populated by the consistency check process
    consistency_status_field: str = "status"
    # Statuses considered as validated outcome of consistency check
    validated_statuses: List[str] = ["valid", "missing"]

    # Retention policy for opfab business data resources (delete data older than N days)
    retention_days: int = 2

    # Upload an (empty, non-validated) envelope even when a dataset returned no documents
    upload_empty_datasets: bool = False

    # Optional per-dataset field whitelist for the pre-processing step.
    # Empty dict / missing key -> keep all _source fields.
    # Example: {"csa-contingencies": ["@id", "name", "EquipmentOperator"]}
    dataset_field_whitelist: Dict[str, List[str]] = {}

    # Maximum documents fetched per index
    query_size: int = 10000

    debug: bool = False


@dataclass(frozen=True)
class ExchangeConfig:
    elastic: ElasticSettings
    opfab: OperatorFabricSettings
    exchange: BusinessSettings


@lru_cache(maxsize=1)
def get_settings() -> ExchangeConfig:
    return ExchangeConfig(
        elastic=ElasticSettings(),
        opfab=OperatorFabricSettings(),
        exchange=BusinessSettings(),
    )


if __name__ == "__main__":
    conf = get_settings()
    print(conf)
