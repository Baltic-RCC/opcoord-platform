"""Enrich converted NC card payloads with reference data from Elastic.

The enrichment pipeline is profile-aware:

    converted NC payload
        -> detect SAR/RAS profile
        -> calculate the relevant query period
        -> load reference documents into temporary caches
        -> enrich the profile-specific sections
        -> clear the caches

RAS enrichment uses Elastic for reference data only. The NC report itself is
converted by the builders before this class is called.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from loguru import logger

from integrations.elastic import Elastic

DEFAULT_AREAS_INDEX = "config-areas"
DEFAULT_CONTINGENCIES_INDEX = "csa-contingencies*"
DEFAULT_REMEDIAL_ACTIONS_INDEX = "csa-remedial-actions*"

NCProfileType = Literal["RAS", "SAR"]


# ---------------------------------------------------------------------------
# Reference-field configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ContingencyFieldEnrichment:
    """Map one contingency document field to one card output field."""

    source_path: str
    output_key: str
    missing_label: str


@dataclass(frozen=True)
class RemedialActionFieldEnrichment:
    """Map one remedial-action document field to one card output field."""

    source_path: str
    output_key: str
    missing_label: str


class CardDataEnricher:
    """Add human-readable reference metadata to converted NC card payloads."""

    # These mappings define which fields are copied from the CSA documents.
    # Routing-specific fields, such as RemedialActionOperatorEIC, are included
    # here so the workflow planner can use the enriched payload afterwards.
    CONTINGENCY_FIELD_ENRICHMENTS = (
        ContingencyFieldEnrichment("name", "ContingencyName", "name"),
        ContingencyFieldEnrichment("@type", "ContingencyType", "type"),
        ContingencyFieldEnrichment(
            "EquipmentOperator",
            "ContingencyOperatorEIC",
            "operator",
        ),
    )
    CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY = "ContingencyOperatorName"
    CONTINGENCY_MATCH_FIELD = "ContingencyEquipment.Contingency"

    REMEDIAL_ACTION_FIELD_ENRICHMENTS = (
        RemedialActionFieldEnrichment("name", "RemedialActionName", "name"),
        RemedialActionFieldEnrichment("kind", "RemedialActionKind", "kind"),
        RemedialActionFieldEnrichment(
            "RemedialActionSystemOperator",
            "RemedialActionOperatorEIC",
            "operator",
        ),
    )
    REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY = "RemedialActionOperatorName"
    REMEDIAL_ACTION_MATCH_FIELD = "@id"

    # -----------------------------------------------------------------------
    # Construction and public entry points
    # -----------------------------------------------------------------------

    def __init__(
        self,
        elastic: Elastic,
        areas_index: str = DEFAULT_AREAS_INDEX,
        contingencies_index: str = DEFAULT_CONTINGENCIES_INDEX,
        remedial_actions_index: str = DEFAULT_REMEDIAL_ACTIONS_INDEX,
        enrichment_strict: bool | None = None,
        enrichment_verbose_logging: bool | None = None,
    ):
        """Store Elastic/index settings and initialize per-run lookup caches."""

        self.elastic = elastic
        self.areas_index = areas_index
        self.contingencies_index = contingencies_index
        self.remedial_actions_index = remedial_actions_index
        self.enrichment_strict = enrichment_strict
        self.enrichment_verbose_logging = enrichment_verbose_logging
        self._areas_cache: list[dict[str, Any]] = []
        self._contingencies_cache: list[dict[str, Any]] = []
        self._remedial_actions_cache: list[dict[str, Any]] = []

    def enrich(
        self,
        payload: dict[str, Any],
        card_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Enrich a payload in place and return it for fluent callers."""

        self.enrich_in_place(payload, card_fields=card_fields)
        return payload

    def enrich_in_place(
        self,
        payload: dict[str, Any],
        card_fields: dict[str, Any] | None = None,
    ) -> None:
        """Detect the profile, enrich it, and always clear temporary caches."""

        previous_process_instance_id = getattr(
            self,
            "_process_instance_id",
            "<unknown>",
        )
        self._process_instance_id = str(
            (card_fields or {}).get("processInstanceId") or "<unknown>"
        )
        profile_type = self._profile_type(payload)
        logger.info(
            f"Enriching {profile_type} profile "
            f"for processInstanceId={self._process_instance_id}"
        )

        try:
            self._prime_enrichment_cache(payload, profile_type)
            self._enrich_profile(payload, profile_type)
            logger.success(
                f"Enriched {profile_type} successfully "
                f"for processInstanceId={self._process_instance_id}"
            )
        finally:
            self._process_instance_id = previous_process_instance_id
            self._clear_enrichment_cache()

    # -----------------------------------------------------------------------
    # Profile detection and dispatch
    # -----------------------------------------------------------------------

    def _enrich_profile(
        self,
        payload: dict[str, Any],
        profile_type: NCProfileType,
    ) -> None:
        """Run the enrichment operations belonging to one NC profile."""

        if profile_type == "RAS":
            self._enrich_remedial_action_schedules(payload)
        elif profile_type == "SAR":
            self._enrich_base_case_power_flow_results(payload)
            self._enrich_contingency_power_flow_results(payload)

    def _profile_type(self, payload: dict[str, Any]) -> NCProfileType:
        """Determine the profile type through the validated FullModel path."""

        return self._validated_profile_type_from_full_model(payload)

    @staticmethod
    def _validated_profile_type_from_full_model(
        payload: dict[str, Any],
    ) -> NCProfileType:
        """Read FullModel.keyword and validate it as RAS or SAR."""

        for full_model in CardDataEnricher._section_items(payload, "FullModel"):
            keyword = full_model.get("keyword")
            keyword_values = keyword if isinstance(keyword, list) else [keyword]

            for keyword_value in keyword_values:
                normalized_keyword = CardDataEnricher._normalize_profile_keyword(
                    keyword_value
                )
                if normalized_keyword == "RAS" or normalized_keyword == "SAR":
                    return normalized_keyword

        raise ValueError(
            "FullModel keyword must be set to a supported NC profile type"
        )

    @staticmethod
    def _normalize_profile_keyword(value: Any) -> str | None:
        """Convert a profile keyword value to uppercase text or None."""

        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    # -----------------------------------------------------------------------
    # Query-period calculation and Elastic cache lifecycle
    # -----------------------------------------------------------------------

    def _prime_enrichment_cache(
        self,
        payload: dict[str, Any],
        profile_type: NCProfileType,
    ) -> None:
        """Load all reference documents needed for one enrichment run."""

        query_period_start, query_period_end = self._query_period(payload)
        self._areas_cache = self._query_areas_index(self.areas_index)
        self._contingencies_cache = self._query_csa_indices(
            self.contingencies_index,
            query_period_start=query_period_start,
            query_period_end=query_period_end,
        )

        if profile_type == "RAS":
            self._remedial_actions_cache = self._query_csa_indices(
                self.remedial_actions_index,
                query_period_start=query_period_start,
                query_period_end=query_period_end,
            )

    def _clear_enrichment_cache(self) -> None:
        """Clear all temporary enrichment caches to free memory."""

        self._areas_cache = []
        self._contingencies_cache = []
        self._remedial_actions_cache = []

    def _query_areas_index(self, index: str) -> list[dict[str, Any]]:
        """Load area and party documents from the configured areas index."""

        query = {"match_all": {}}
        self._log_query_start(index)
        hits = self.elastic.get_docs_by_query(
            index=index,
            query=query,
            size=100,
            return_df=True,
        )
        docs = self._extract_source_docs(hits)
        self._log_query_result(index, len(docs))
        return docs

    def _query_csa_indices(
        self,
        index: str,
        query_period_start: datetime,
        query_period_end: datetime,
    ) -> list[dict[str, Any]]:
        """Load CSA documents whose FullModel interval overlaps the period."""

        query = {
            "bool": {
                "must": [
                    {
                        "range": {
                            "FullModel.startDate": {
                                "lte": query_period_end,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                    {
                        "range": {
                            "FullModel.endDate": {
                                "gte": query_period_start,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ]
            }
        }
        self._log_query_start(
            index,
            f" for query period {query_period_start.isoformat()} "
            f"- {query_period_end.isoformat()}",
        )
        hits = self.elastic.get_docs_by_query(
            index=index,
            query=query,
            size=5000,
            return_df=False,
        )
        docs = self._extract_source_docs(hits)
        self._log_query_result(index, len(docs))
        return docs

    def _query_period(
        self,
        payload: dict[str, Any],
    ) -> tuple[datetime, datetime]:
        """Return the Elastic lookup period for the detected NC profile.

        SAR uses a symmetric 30-minute window around scenarioTime.
        RAS uses FullModel.startDate through FullModel.endDate.
        """

        profile_type = self._profile_type(payload)
        if profile_type == "SAR":
            return self._sar_query_period(payload)
        if profile_type == "RAS":
            return self._ras_query_period(payload)
        raise ValueError(f"Unsupported profile type for query period: {profile_type}")

    @staticmethod
    def _sar_query_period(
        payload: dict[str, Any],
    ) -> tuple[datetime, datetime]:
        """Compute a 30-minute UTC window around SAR scenarioTime.

        Only UTC time is supported.
        """
        for full_model in CardDataEnricher._section_items(payload, "FullModel"):
            scenario_time = full_model.get("scenarioTime")
            if scenario_time:
                scenario_time = datetime.fromisoformat(scenario_time)
                if scenario_time.utcoffset() != timedelta(0):
                    raise ValueError("FullModel.scenarioTime must be expressed in UTC")
                return (
                    scenario_time - timedelta(minutes=30),
                    scenario_time + timedelta(minutes=30),
                )

        raise ValueError("FullModel.scenarioTime is required for SAR query period")

    @staticmethod
    def _ras_query_period(
        payload: dict[str, Any],
    ) -> tuple[datetime, datetime]:
        """Read FullModel.startDate and FullModel.endDate for RAS."""

        for full_model in CardDataEnricher._section_items(payload, "FullModel"):
            start_date_raw = full_model.get("startDate")
            end_date_raw = full_model.get("endDate")
            if start_date_raw and end_date_raw:
                start_date = datetime.fromisoformat(start_date_raw)
                if start_date.utcoffset() != timedelta(0):
                    raise ValueError("RAS FullModel.startDate must be expressed in UTC")

                end_date = datetime.fromisoformat(end_date_raw)
                if end_date.utcoffset() != timedelta(0):
                    raise ValueError("RAS FullModel.endDate must be expressed in UTC")

                return start_date, end_date

        raise ValueError(
            "FullModel.startDate and FullModel.endDate are required "
            "for RAS query period"
        )

    def _log_query_start(self, index: str, suffix: str = "") -> None:
        """Log the beginning of a reference-data query when requested."""

        if self.enrichment_verbose_logging:
            logger.debug(
                f"[Card id={self._process_instance_id}] "
                f"Priming enrichment cache from {index}{suffix}"
            )

    def _log_query_result(self, index: str, count: int) -> None:
        """Log the number of reference documents returned."""

        if self.enrichment_verbose_logging:
            logger.debug(
                f"[Card id={self._process_instance_id}] "
                f"Loaded {count} documents from {index}"
            )

    # -----------------------------------------------------------------------
    # Profile-specific enrichment
    # -----------------------------------------------------------------------

    def _enrich_base_case_power_flow_results(
        self,
        payload: dict[str, Any],
    ) -> None:
        """Add reported area names to SAR base-case results."""

        for result in self._section_items(payload, "BaseCasePowerFlowResult"):
            self._add_area_name(result, "ReportedByRegion")

    def _enrich_contingency_power_flow_results(
        self,
        payload: dict[str, Any],
    ) -> None:
        """Add area and contingency details to SAR contingency results."""

        for result in self._section_items(payload, "ContingencyPowerFlowResult"):
            self._add_area_name(result, "ReportedByRegion")
            self._add_contingency_fields(result)

    def _enrich_remedial_action_schedules(
        self,
        payload: dict[str, Any],
    ) -> None:
        """Add area, proposer, contingency, and action details to RAS."""

        for schedule in self._section_items(payload, "RemedialActionSchedule"):
            self._add_area_name(schedule, "AssignedRegion")
            self._add_proposed_by_name(schedule, "ProposingEntity")
            self._add_contingency_fields(schedule)
            self._add_remedial_action_fields(schedule)

    # -----------------------------------------------------------------------
    # Area and party lookups
    # -----------------------------------------------------------------------

    def _add_area_name(self, item: dict[str, Any], source_key: str) -> None:
        """Resolve an area EIC and write the display AreaName."""

        area_eic = item.get(source_key)
        if not area_eic:
            self._handle_missing_field(item, "AreaName", f"missing {source_key}")
            return

        area = self._get_area_by_eic(area_eic)
        area_name = self._get_path(area, "area.name") if area else None
        if not area_name:
            self._handle_missing_field(
                item,
                "AreaName",
                f"no area.name found for area EIC {area_eic}",
            )
            return

        item["AreaName"] = area_name
        self._log_field_enriched(item, "AreaName", area_name)

    def _add_proposed_by_name(
        self,
        item: dict[str, Any],
        source_key: str,
    ) -> None:
        """Resolve a proposing party EIC and write ProposedByName."""

        party_eic = item.get("ProposingEntity")
        if not party_eic:
            self._handle_missing_field(
                item,
                "ProposedByName",
                f"missing {source_key}",
            )
            return

        party = self._get_party_by_eic(party_eic)
        party_name = self._get_path(party, "party.name") if party else None
        if not party_name:
            self._handle_missing_field(
                item,
                "ProposedByName",
                f"no party.name found for party EIC {party_eic}",
            )
            return

        item["ProposedByName"] = party_name
        self._log_field_enriched(item, "ProposedByName", party_name)

    def _get_area_by_eic(self, eic: str) -> dict[str, Any] | None:
        """Return an area document from the in-memory cache by EIC."""

        for doc in self._areas_cache:
            if self._get_path(doc, "area.eic") == eic:
                return doc
        return None

    def _get_party_by_eic(self, eic: str) -> dict[str, Any] | None:
        """Return a party document from the in-memory cache by EIC."""

        for doc in self._areas_cache:
            if self._get_path(doc, "party.eic") == eic:
                return doc
        return None

    # -----------------------------------------------------------------------
    # Contingency enrichment
    # -----------------------------------------------------------------------

    def _add_contingency_fields(self, item: dict[str, Any]) -> None:
        """Copy configured contingency fields onto an item."""

        contingency_id = item.get("Contingency")
        if not contingency_id:
            self._handle_missing_contingency_fields(item, "missing Contingency")
            return

        contingency = self._get_contingency_by_identifier(contingency_id)
        if not contingency:
            self._handle_missing_contingency_fields(
                item,
                f"no contingency document found for identifier {contingency_id}",
            )
            return

        for field in self.CONTINGENCY_FIELD_ENRICHMENTS:
            value = self._get_path(contingency, field.source_path)
            if value:
                item[field.output_key] = value
                self._log_field_enriched(item, field.output_key, value)
                if field.output_key == "ContingencyOperatorEIC":
                    self._add_contingency_operator_name(item, value)
            else:
                reason = (
                    f"no contingency {field.missing_label} "
                    f"found for identifier {contingency_id}"
                )
                self._handle_missing_field(item, field.output_key, reason)
                if field.output_key == "ContingencyOperatorEIC":
                    self._handle_missing_field(
                        item,
                        self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY,
                        reason,
                    )

    def _add_contingency_operator_name(
        self,
        item: dict[str, Any],
        operator_eic: Any,
    ) -> None:
        """Resolve a contingency operator EIC and write its display name."""

        if not operator_eic:
            self._handle_missing_field(
                item,
                self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY,
                f"missing contingency operator EIC {operator_eic}",
            )
            return

        party = self._get_party_by_eic(operator_eic)
        party_name = self._get_path(party, "party.name") if party else None
        if not party_name:
            self._handle_missing_field(
                item,
                self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY,
                f"no party.name found for party EIC {operator_eic}",
            )
            return

        item[self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY] = party_name
        self._log_field_enriched(
            item,
            self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY,
            party_name,
        )

    def _handle_missing_contingency_fields(
        self,
        item: dict[str, Any],
        reason: str,
    ) -> None:
        """Apply missing-field handling to all contingency output fields."""

        for field in self.CONTINGENCY_FIELD_ENRICHMENTS:
            self._handle_missing_field(item, field.output_key, reason)
        self._handle_missing_field(
            item,
            self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY,
            reason,
        )

    def _get_contingency_by_identifier(
        self,
        identifier: str,
    ) -> dict[str, Any] | None:
        """Return a contingency document from the cache by identifier."""

        for doc in self._contingencies_cache:
            if self._get_path(doc, self.CONTINGENCY_MATCH_FIELD) == identifier:
                return doc
        return None

    # -----------------------------------------------------------------------
    # Remedial-action enrichment
    # -----------------------------------------------------------------------

    def _add_remedial_action_fields(self, item: dict[str, Any]) -> None:
        """Copy configured remedial-action fields onto an item."""

        remedial_action_id = item.get("RemedialAction")
        if not remedial_action_id:
            self._handle_missing_remedial_action_fields(
                item,
                "missing RemedialAction",
            )
            return

        remedial_action = self._get_remedial_action_by_identifier(
            remedial_action_id
        )
        if not remedial_action:
            self._handle_missing_remedial_action_fields(
                item,
                f"no remedial action document found for identifier "
                f"{remedial_action_id}",
            )
            return

        for field in self.REMEDIAL_ACTION_FIELD_ENRICHMENTS:
            value = self._get_path(remedial_action, field.source_path)
            if value:
                item[field.output_key] = value
                self._log_field_enriched(item, field.output_key, value)
                if field.output_key == "RemedialActionOperatorEIC":
                    self._add_remedial_action_operator_name(item, value)
            else:
                reason = (
                    f"no remedial action {field.missing_label} "
                    f"found for identifier {remedial_action_id}"
                )
                self._handle_missing_field(item, field.output_key, reason)
                if field.output_key == "RemedialActionOperatorEIC":
                    self._handle_missing_field(
                        item,
                        self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY,
                        reason,
                    )

    def _add_remedial_action_operator_name(
        self,
        item: dict[str, Any],
        operator_eic: Any,
    ) -> None:
        """Resolve a remedial-action operator EIC and write its display name."""

        if not operator_eic:
            self._handle_missing_field(
                item,
                self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY,
                f"missing remedial action operator EIC {operator_eic}",
            )
            return

        party = self._get_party_by_eic(operator_eic)
        party_name = self._get_path(party, "party.name") if party else None
        if not party_name:
            self._handle_missing_field(
                item,
                self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY,
                f"no party.name found for party EIC {operator_eic}",
            )
            return

        item[self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY] = party_name
        self._log_field_enriched(
            item,
            self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY,
            party_name,
        )

    def _handle_missing_remedial_action_fields(
        self,
        item: dict[str, Any],
        reason: str,
    ) -> None:
        """Apply missing-field handling to all remedial-action outputs."""

        for field in self.REMEDIAL_ACTION_FIELD_ENRICHMENTS:
            self._handle_missing_field(item, field.output_key, reason)
        self._handle_missing_field(
            item,
            self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY,
            reason,
        )

    def _get_remedial_action_by_identifier(
        self,
        identifier: str,
    ) -> dict[str, Any] | None:
        """Return a remedial-action document from the cache by identifier."""

        for doc in self._remedial_actions_cache:
            if self._get_path(doc, self.REMEDIAL_ACTION_MATCH_FIELD) == identifier:
                return doc
        return None

    # -----------------------------------------------------------------------
    # Shared response and payload utilities
    # -----------------------------------------------------------------------

    @staticmethod
    def _extract_source_docs(response: Any) -> list[dict[str, Any]]:
        """Normalize Elastic/DataFrame response shapes into source documents."""

        if response is None:
            return []

        if isinstance(response, dict):
            if "_source" in response:
                source = response.get("_source")
                return [source] if isinstance(source, dict) else []
            hits = response.get("hits")
            if isinstance(hits, dict):
                return CardDataEnricher._extract_source_docs(hits.get("hits"))
            return [response]

        if isinstance(response, list):
            docs: list[dict[str, Any]] = []
            for item in response:
                docs.extend(CardDataEnricher._extract_source_docs(item))
            return docs

        if hasattr(response, "to_dict"):
            records = response.to_dict(orient="records")
            return [record for record in records if isinstance(record, dict)]

        return []

    @staticmethod
    def _section_items(
        payload: dict[str, Any],
        section_name: str,
    ) -> list[dict[str, Any]]:
        """Return a section as a list regardless of input shape."""

        section = payload.get(section_name)
        if isinstance(section, dict):
            return [section]
        if isinstance(section, list):
            return [item for item in section if isinstance(item, dict)]
        return []

    @staticmethod
    def _get_path(
        document: dict[str, Any] | None,
        path: str,
    ) -> Any:
        """Read a dotted path from nested dictionaries and simple lists."""

        if not document:
            return None
        if path in document:
            return document[path]

        current: Any = document
        for part in path.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and current and isinstance(current[0], dict):
                current = current[0].get(part)
            else:
                return None
        return current

    def _log_field_enriched(
        self,
        item: dict[str, Any],
        field_name: str,
        value: Any,
    ) -> None:
        """Log one enriched field when verbose logging is enabled."""

        if self.enrichment_verbose_logging:
            logger.debug(
                f"[Card id={self._process_instance_id}] "
                f"Enriched {field_name} for item {item.get('@id')}: {value}"
            )

    # -----------------------------------------------------------------------
    # Missing-data policy
    # -----------------------------------------------------------------------

    def _handle_missing_field(
        self,
        item: dict[str, Any],
        field_name: str,
        reason: str,
    ) -> None:
        """Log missing data and raise only when strict mode is enabled."""

        message = (
            f"[Card id={self._process_instance_id}] "
            f"Failed to enrich {field_name}: {reason}"
        )
        logger.warning(message)
        if self.enrichment_strict:
            logger.warning(
                f"[Card id={self._process_instance_id}] "
                "Strict enrichment mode is enabled; raising ValueError"
            )
            raise ValueError(message)


if __name__ == "__main__":
    from card_publicator.rdf_converter import convert_cim_rdf_to_json

    def enrich_nc_xml_file(
        input_path: str = (
            "C:/Users/lukas.navickas/Documents/Opcoord_testing/"
            "example_cards/SAR_20260708T2030_1D_1_"
            "a753f34b-4f07-49a3-8335-8ff9c0e8f907.xml"
        ),
        output_path: str = (
            "C:/Users/lukas.navickas/Documents/Opcoord_testing/"
            "enriched_cards/enriched_card_sar_ID_test.json"
        ),
        enrichment_strict: bool = False,
        enrichment_verbose_logging: bool = True,
        indent: int = 2,
    ) -> dict[str, Any]:
        """Convert one local NC XML card, enrich it, and write the result."""

        # This helper is only for manual testing. Production code should use
        # CardDataEnricher directly after the payload has been converted.
        input_path = Path(input_path)
        output_path = Path(output_path)

        with input_path.open("r", encoding="utf-8") as input_file:
            payload = convert_cim_rdf_to_json(
                input_file.read(),
                root_class=[
                    "BaseCasePowerFlowResult",
                    "ContingencyPowerFlowResult",
                    "RemedialActionSchedule",
                ],
                key_mode="local",
            )

        enricher = CardDataEnricher(
            elastic=Elastic(debug=enrichment_verbose_logging),
            areas_index=DEFAULT_AREAS_INDEX,
            contingencies_index=DEFAULT_CONTINGENCIES_INDEX,
            remedial_actions_index=DEFAULT_REMEDIAL_ACTIONS_INDEX,
            enrichment_strict=enrichment_strict,
            enrichment_verbose_logging=enrichment_verbose_logging,
        )
        enricher.enrich(payload)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as output_file:
            json.dump(payload, output_file, ensure_ascii=False, indent=indent)
            output_file.write("\n")

        return payload

    enrich_nc_xml_file()
