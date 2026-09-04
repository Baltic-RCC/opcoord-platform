from __future__ import annotations

import re
from collections import Counter
from datetime import UTC, datetime, timedelta, date
from typing import Any, Dict, List

from loguru import logger

from integrations.elastic import Elastic
from integrations.opfab import AuthenticatedSession
import settings

conf = settings.get_settings()

"""Business exchange process for CSA input data management inside OperatorFabric.

Workflow (see Baltic-RCC/opcoord-platform#24):
    1. Query CSA input indices in Elastic directly
       (csa-contingencies / csa-assessed-elements / csa-remedial-actions).
       Reuses the same query pattern as card_publicator enrichment:
       documents whose FullModel validity period overlaps the target business day.
    2. If data is not empty -> apply retention policy on opfab business data
       (delete resources older than N days, default 2).
    3. Pre-process input data into the internal OperatorFabric business data
       format (a plain JSON file accessible from handlebars templates).
    4. Flag data according to the CSA consistency check outcome:
       status in (valid, missing) -> validated; empty dataset -> non-validated.
    5. Upload to OperatorFabric via the businessconfig/businessdata API.
"""

# Business data resources are named <dataset>_<YYYY-MM-DD>, the date suffix
# is what the retention policy is applied against.
RESOURCE_DATE_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})$")


class BusinessDataExchangeHandler:

    def __init__(self, debug: bool = conf.exchange.debug):
        self.debug = debug
        self.datasets = {
            "csa-contingencies": conf.exchange.contingencies_index,
            "csa-assessed-elements": conf.exchange.assessed_elements_index,
            "csa-remedial-actions": conf.exchange.remedial_actions_index,
        }

        # Services initialization
        self.elastic = Elastic(debug=debug)
        self.opfab = AuthenticatedSession()

    # ------------------------------------------------------------------ #
    # Step 1: query CSA input indices                                    #
    # ------------------------------------------------------------------ #
    def query_period(self) -> tuple[datetime, datetime]:
        """Target business day window (UTC). Worker runs before 1D CROSA -> default tomorrow."""
        target_day = datetime.now(UTC).date() + timedelta(days=conf.exchange.target_day_offset)
        start = datetime(target_day.year, target_day.month, target_day.day, tzinfo=UTC)
        return start, start + timedelta(days=1)

    def query_dataset(self, index: str, period_start: datetime, period_end: datetime) -> List[Dict[str, Any]]:
        """Query one CSA input index for documents whose FullModel validity overlaps the period.

        Same overlap query as the already working CSA solution used by the
        card_publicator enrichment (CardDataEnricher._query_csa_indices).
        """
        query = {
            "bool": {
                "must": [
                    {"range": {"FullModel.startDate": {"lte": period_end.isoformat(), "format": "strict_date_optional_time"}}},
                    {"range": {"FullModel.endDate": {"gte": period_start.isoformat(), "format": "strict_date_optional_time"}}},
                ]
            }
        }
        logger.info(f"Querying {index} for period {period_start.isoformat()} - {period_end.isoformat()}")
        hits = self.elastic.get_docs_by_query(index=index,
                                              query=query,
                                              size=conf.exchange.query_size,
                                              return_df=False)
        docs = [hit.get("_source", {}) for hit in hits if isinstance(hit, dict)]
        logger.info(f"Retrieved {len(docs)} documents from {index}")
        return docs

    # ------------------------------------------------------------------ #
    # Step 2: retention policy on opfab business data                    #
    # ------------------------------------------------------------------ #
    def apply_retention_policy(self) -> None:
        """Delete opfab business data resources with a date suffix older than retention_days."""
        cutoff: date = datetime.now(UTC).date() - timedelta(days=conf.exchange.retention_days)
        try:
            resources = self.opfab.get_business_data_resources()
        except Exception as e:
            logger.error(f"Failed to list opfab business data resources, skipping retention: {e}")
            return

        for resource in resources:
            match = RESOURCE_DATE_PATTERN.search(resource)
            if not match:
                # Not managed by this worker (no date suffix) -> leave untouched
                continue
            resource_date = date.fromisoformat(match.group(1))
            if resource_date < cutoff:
                logger.info(f"Retention: deleting business data resource '{resource}' (older than {conf.exchange.retention_days} days)")
                try:
                    self.opfab.delete_business_data(resource)
                except Exception as e:
                    logger.error(f"Failed to delete business data resource '{resource}': {e}")

    # ------------------------------------------------------------------ #
    # Steps 3 + 4: pre-process to internal opfab format and flag         #
    # ------------------------------------------------------------------ #
    def preprocess(self, dataset: str, docs: List[Dict[str, Any]], business_day: date) -> Dict[str, Any]:
        """Build the internal opfab business data JSON envelope.

        OperatorFabric business data is free-form JSON consumed by handlebars
        templates (opfab.businessconfig businessdata endpoint), so the shape
        below is our internal contract:
        metadata (dataset, business date, validation flag, status breakdown)
        + records (the CSA input documents).
        TODO: align field selection with what the crosa templates will consume.
        """
        whitelist = conf.exchange.dataset_field_whitelist.get(dataset)
        if whitelist:
            records = [{key: doc.get(key) for key in whitelist} for doc in docs]
        else:
            records = docs

        status_field = conf.exchange.consistency_status_field
        statuses = Counter(str(doc.get(status_field)).lower() for doc in docs if doc.get(status_field) is not None)

        return {
            "metadata": {
                "dataset": dataset,
                "businessDate": business_day.isoformat(),
                "generatedAt": datetime.now(UTC).isoformat(),
                "recordCount": len(records),
                "validated": self.is_validated(docs, statuses),
                "consistencyStatuses": dict(statuses),
            },
            "records": records,
        }

    def is_validated(self, docs, statuses):
    # Input validation does not exist in the pipeline yet — calculations
    # consume all inputs regardless. 
        return False

    # ------------------------------------------------------------------ #
    # Step 5: upload to OperatorFabric                                   #
    # ------------------------------------------------------------------ #
    def upload(self, dataset: str, envelope: Dict[str, Any], business_day: date) -> None:
        resource_name = f"{dataset}_{business_day.isoformat()}"
        logger.info(f"Uploading business data resource '{resource_name}' "
                    f"({envelope['metadata']['recordCount']} records, validated={envelope['metadata']['validated']})")
        self.opfab.upload_business_data(resource_name=resource_name, data=envelope)
        logger.success(f"Business data resource '{resource_name}' uploaded to OperatorFabric")

    # ------------------------------------------------------------------ #
    # Orchestration                                                      #
    # ------------------------------------------------------------------ #
    def handle(self) -> bool:
        period_start, period_end = self.query_period()
        business_day = period_start.date()
        logger.info(f"Starting business data exchange for business day {business_day.isoformat()}")

        # Step 1: query all CSA input datasets
        data: Dict[str, List[Dict[str, Any]]] = {
            dataset: self.query_dataset(index, period_start, period_end)
            for dataset, index in self.datasets.items()
        }

        if not any(data.values()):
            logger.warning("All CSA input datasets are empty, nothing to exchange")
            return False

        # Step 2: retention policy (only applied when data is not empty)
        self.apply_retention_policy()

        # Steps 3-5 per dataset
        for dataset, docs in data.items():
            if not docs and not conf.exchange.upload_empty_datasets:
                logger.warning(f"Dataset '{dataset}' is empty, skipping upload (non-validated)")
                continue
            envelope = self.preprocess(dataset=dataset, docs=docs, business_day=business_day)
            self.upload(dataset=dataset, envelope=envelope, business_day=business_day)

        logger.success("Business data exchange process completed")
        return True


if __name__ == "__main__":
    handler = BusinessDataExchangeHandler(debug=True)
    handler.handle()
