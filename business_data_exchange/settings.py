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

    # Where the CSA consistency check verdict sits inside each dataset's documents.
    # The check writes a ConsistencyStatus attribute, but at a different depth per
    # NC profile: on the child element for contingencies, on the document root for
    # assessed elements, and on each action type for remedial actions.
    consistency_status_paths: Dict[str, List[str]] = {
        "csa-contingencies": ["ContingencyEquipment.ConsistencyStatus"],
        "csa-assessed-elements": ["ConsistencyStatus"],
        "csa-remedial-actions": [
            "TopologyAction.ConsistencyStatus",
            "RotatingMachineAction.ConsistencyStatus",
            "ShuntCompensatorModification.ConsistencyStatus",
        ],
    }
    # Statuses considered as a passing outcome of the consistency check.
    # "missing" means the referenced grid element was not found in the network
    # model, so it is deliberately not a passing status.
    validated_statuses: List[str] = ["valid"]

    # Elements that did not pass are listed in the envelope so the card can name
    # them. The count is always exact; the list is capped to keep the upload small.
    failed_elements_listed: int = 100

    # The consistency check re-publishes each profile under a higher version, so an
    # index holds both the original TSO submission (no verdict) and the checked one.
    # Keep only the highest version per publisher, otherwise both are exported.
    latest_version_only: bool = True

    # TSOs expected to submit CSA input data for every business day, by party EIC
    # (see the config-areas index for the EIC to TSO mapping).
    expected_publishers: Dict[str, str] = {
        "10X1001A1001A39W": "ELERING (EE)",
        "10X1001A1001A55Y": "LITGRID (LT)",
        "10X1001A1001B54W": "AST (LV)",
    }

    # When an expected TSO submitted nothing for the business day, reuse its most
    # recent earlier submission instead of exporting an incomplete dataset. The
    # search goes back as far as needed, there is no day limit — a TSO can be
    # silent for a week or more. Data older than the threshold below is still
    # used, but reported as stale so the template can show it.
    fallback_enabled: bool = True
    fallback_stale_after_days: int = 7

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
