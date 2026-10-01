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
       every record checked and every status valid -> validated; any missing
       status, unchecked record or empty dataset -> non-validated.
    5. Upload to OperatorFabric via the businessconfig/businessdata API.
"""

# Business data resources are named <dataset>_<YYYY-MM-DD>, the date suffix
# is what the retention policy is applied against.
RESOURCE_DATE_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})$")


def extract_verdicts(doc: Dict[str, Any], paths: List[str]) -> List[tuple]:
    """Consistency verdicts in a document, as (status, node) pairs.

    The verdict can sit on the document root or inside a child list (one entry
    per contingency equipment / remedial action alteration), so every path
    segment may expand into several nodes. The node carrying the verdict is
    returned with it, because that is where the element name and the reference
    to the missing equipment live.
    """
    found: List[tuple] = []
    for path in paths:
        *parents, leaf = path.split(".")
        nodes: List[Any] = [doc]
        for part in parents:
            next_nodes: List[Any] = []
            for node in nodes:
                if isinstance(node, dict) and part in node:
                    value = node[part]
                    next_nodes.extend(value if isinstance(value, list) else [value])
            nodes = next_nodes
        for node in nodes:
            if isinstance(node, dict) and node.get(leaf) is not None:
                found.append((str(node[leaf]).lower(), node))
    return found


def describe_element(doc: Dict[str, Any], node: Dict[str, Any], status: str) -> Dict[str, Any]:
    """One failed element, named the way an operator would recognise it."""
    return {
        "publisher": doc.get("FullModel.publisher"),
        "owner": (doc.get("EquipmentOperator") or doc.get("AssessedSystemOperator")
                  or doc.get("RemedialActionSystemOperator")),
        "parent": doc.get("name"),
        "parentType": doc.get("@type"),
        "element": node.get("name"),
        "equipment": node.get("Equipment") or node.get("ConductingEquipment"),
        "status": status,
    }


def profile_version(doc: Dict[str, Any]) -> int:
    """FullModel.version as a number; it is stored as text and can be zero padded."""
    try:
        return int(str(doc.get("FullModel.version", "0")).lstrip("0") or "0")
    except ValueError:
        return 0


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

        # Profiles run 22:00-22:00 UTC, so the overlap query also returns the
        # neighbouring business day. Keep only the requested one, otherwise the
        # latest version filter below could pick the wrong day.
        target_day = self.business_day_of(period_start, period_end)
        docs = [doc for doc in docs if self.doc_business_day(doc) in (target_day, None)]
        logger.info(f"Kept {len(docs)} documents for business day {target_day.isoformat()}")

        if conf.exchange.latest_version_only:
            docs = self.keep_latest_version(docs)
            logger.info(f"Kept {len(docs)} documents after selecting the latest version per publisher")

        return docs

    @staticmethod
    def keep_latest_version(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Drop superseded profile versions.

        The consistency check republishes each sender's profile under version+1,
        so the index holds the original TSO submission next to the checked one.
        Only the highest version per publisher describes the current state.
        """
        latest: Dict[str, int] = {}
        for doc in docs:
            publisher = str(doc.get("FullModel.publisher"))
            version = profile_version(doc)
            if version > latest.get(publisher, -1):
                latest[publisher] = version
        return [doc for doc in docs
                if profile_version(doc) == latest.get(str(doc.get("FullModel.publisher")), -1)]

    @staticmethod
    def publishers_of(docs: List[Dict[str, Any]]) -> set[str]:
        return {str(doc.get("FullModel.publisher")) for doc in docs}

    def apply_fallback(self, dataset: str, index: str, docs: List[Dict[str, Any]],
                       period_start: datetime, period_end: datetime) -> tuple:
        """Substitute an earlier submission for every expected TSO that sent nothing.

        A TSO that submits nothing produces no records at all, so the consistency
        check cannot flag it — the dataset would simply be exported incomplete and
        silently so. Rather than that, the most recent earlier submission is reused
        and the substitution is recorded in the envelope metadata.
        """
        used: List[Dict[str, Any]] = []
        if not conf.exchange.fallback_enabled:
            return docs, used

        business_day = period_start.date()
        missing = sorted(set(conf.exchange.expected_publishers) - self.publishers_of(docs))
        for publisher in missing:
            found = self.find_last_submission(index, publisher, before=period_start)
            if found is None:
                logger.error(f"Dataset '{dataset}': no data from {publisher} for the business day "
                             f"and no earlier submission exists at all")
                continue

            earlier_start, earlier_end = found
            source_day = self.business_day_of(earlier_start, earlier_end)
            earlier_docs = self.query_dataset(index, earlier_start, earlier_end)
            substitute = [doc for doc in earlier_docs
                          if str(doc.get("FullModel.publisher")) == publisher]
            if not substitute:
                logger.error(f"Dataset '{dataset}': last submission of {publisher} found on "
                             f"{source_day.isoformat()} but no records could be read")
                continue

            age_days = (business_day - source_day).days
            stale = age_days > conf.exchange.fallback_stale_after_days
            docs.extend(substitute)
            used.append({
                "publisher": publisher,
                "name": conf.exchange.expected_publishers.get(publisher),
                "sourceDate": source_day.isoformat(),
                "ageDays": age_days,
                "stale": stale,
                "recordCount": len(substitute),
            })
            message = (f"Dataset '{dataset}': {publisher} submitted nothing for the business day, "
                       f"reusing {len(substitute)} records from {source_day.isoformat()} "
                       f"({age_days} days old)")
            logger.error(message) if stale else logger.warning(message)

        return docs, used

    @staticmethod
    def business_day_of(start: datetime, end: datetime) -> date:
        """Business day a profile covers.

        Profiles are day aligned on local midnight (startDate 22:00 UTC in CET
        winter time), so the date of startDate is the previous day. The midpoint
        of the validity window lands inside the business day whatever the offset.
        """
        return (start + (end - start) / 2).date()

    def doc_business_day(self, doc: Dict[str, Any]) -> date | None:
        """Business day of one profile, or None when its validity window is unreadable."""
        try:
            start = self.as_utc(doc.get("FullModel.startDate"))
            end = self.as_utc(doc.get("FullModel.endDate"))
        except ValueError:
            return None
        if start is None or end is None:
            return None
        return self.business_day_of(start, end)

    def find_last_submission(self, index: str, publisher: str, before: datetime):
        """Validity window of this TSO's most recent submission before `before`.

        One query rather than walking back day by day, so a TSO that has been
        silent for a week or a month is found just as cheaply.
        """
        query = {
            "bool": {
                "must": [
                    {"term": {"FullModel.publisher.keyword": publisher}},
                    {"range": {"FullModel.startDate": {"lt": before.isoformat(),
                                                       "format": "strict_date_optional_time"}}},
                ]
            }
        }
        hits = self.elastic.get_docs_by_query(index=index, query=query, size=1, return_df=False,
                                              sort=[{"FullModel.startDate": "desc"}])
        if not hits:
            return None
        source = hits[0].get("_source", {})
        start = self.as_utc(source.get("FullModel.startDate"))
        if start is None:
            return None
        end = self.as_utc(source.get("FullModel.endDate")) or start + timedelta(days=1)
        return start, end

    @staticmethod
    def as_utc(value: str | None):
        """Parse a FullModel timestamp; they are stored without a timezone suffix."""
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

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
    def preprocess(self, dataset: str, docs: List[Dict[str, Any]], business_day: date,
                   fallback: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
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

        paths = conf.exchange.consistency_status_paths.get(dataset, [])
        passing = {status.lower() for status in conf.exchange.validated_statuses}
        statuses = Counter()
        without_verdict = 0
        failed: List[Dict[str, Any]] = []
        for doc in docs:
            verdicts = extract_verdicts(doc, paths)
            if not verdicts:
                without_verdict += 1
                continue
            statuses.update(status for status, _ in verdicts)
            failed.extend(describe_element(doc, node, status)
                          for status, node in verdicts if status not in passing)

        return {
            "metadata": {
                "dataset": dataset,
                "businessDate": business_day.isoformat(),
                "generatedAt": datetime.now(UTC).isoformat(),
                "recordCount": len(records),
                "validated": self.is_validated(docs, statuses, without_verdict),
                "consistencyStatuses": dict(statuses),
                "recordsWithoutVerdict": without_verdict,
                # What exactly drops out of the calculation, so the card can name it
                # instead of only saying "not validated".
                "failedElementCount": len(failed),
                "failedElements": failed[:conf.exchange.failed_elements_listed],
                "submitters": {
                    "expected": sorted(conf.exchange.expected_publishers),
                    "received": sorted(self.publishers_of(docs)),
                    "missing": sorted(set(conf.exchange.expected_publishers)
                                      - self.publishers_of(docs)),
                },
                "fallback": fallback or [],
            },
            "records": records,
        }

    @staticmethod
    def is_validated(docs, statuses: Counter, without_verdict: int) -> bool:
        """A dataset is validated when the CSA consistency check passed on all of it.

        That requires every record to carry a verdict (a record without one was
        never checked) and every verdict to be a passing one. Note this only
        covers what was submitted — a TSO that sent nothing produces no records
        and therefore no failing verdict.
        """
        if not docs or without_verdict:
            return False
        allowed = {status.lower() for status in conf.exchange.validated_statuses}
        return bool(statuses) and set(statuses) <= allowed

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

        # Step 1: query all CSA input datasets, substituting an earlier submission
        # for any expected TSO that sent nothing for this business day.
        data: Dict[str, List[Dict[str, Any]]] = {}
        fallbacks: Dict[str, List[Dict[str, Any]]] = {}
        for dataset, index in self.datasets.items():
            docs = self.query_dataset(index, period_start, period_end)
            docs, used = self.apply_fallback(dataset, index, docs, period_start, period_end)
            data[dataset] = docs
            fallbacks[dataset] = used

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
            envelope = self.preprocess(dataset=dataset, docs=docs, business_day=business_day,
                                       fallback=fallbacks[dataset])
            self.upload(dataset=dataset, envelope=envelope, business_day=business_day)

        logger.success("Business data exchange process completed")
        return True


if __name__ == "__main__":
    handler = BusinessDataExchangeHandler(debug=True)
    handler.handle()
