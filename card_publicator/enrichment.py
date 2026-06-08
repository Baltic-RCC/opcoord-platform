from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from loguru import logger

from integrations.elastic import Elastic

"""Card enrichment helpers for adding human-readable metadata to NC cards.

Workflow overview:
    * Detect the NC profile type from the FullModel keyword (RAS or SAR).
    * Walk the profile-specific card sections that need enrichment.
    * Resolve referenced area, party, contingency, and remedial-action documents from Elastic.
    * Copy configured source fields into the card payload and add operator/area display names.
    * Log or raise on missing enrichment data depending on strict mode.
"""

DEFAULT_AREAS_INDEX = "config-areas"
DEFAULT_CONTINGENCIES_INDEX = "csa-contingencies-*"
DEFAULT_REMEDIAL_ACTIONS_INDEX = "csa-remedial-actions-*"

NCProfileType = Literal["RAS", "SAR"]


@dataclass(frozen=True)
class ContingencyFieldEnrichment:
    """Maps one contingency document field to the card output field to enrich."""
    source_path: str
    output_key: str
    missing_label: str


@dataclass(frozen=True)
class RemedialActionFieldEnrichment:
    """Maps one remedial-action document field to the card output field to enrich."""
    source_path: str
    output_key: str
    missing_label: str


class CardDataEnricher:
    """Enriches converted NC card payloads with names and metadata from Elastic."""
    CONTINGENCY_FIELD_ENRICHMENTS = (
        ContingencyFieldEnrichment("name", "ContingencyName", "name"),
        ContingencyFieldEnrichment("@type", "ContingencyType", "type"),
        ContingencyFieldEnrichment("EquipmentOperator", "ContingencyOperatorEIC", "operator"),
    )

    CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY = "ContingencyOperatorName"

    CONTINGENCY_MATCH_FIELD = "ContingencyEquipment.Contingency"

    REMEDIAL_ACTION_FIELD_ENRICHMENTS = (
        RemedialActionFieldEnrichment("name", "RemedialActionName", "name"),
        RemedialActionFieldEnrichment("kind", "RemedialActionKind", "kind"),
        RemedialActionFieldEnrichment("RemedialActionSystemOperator", "RemedialActionOperatorEIC", "operator"),
    )

    REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY = "RemedialActionOperatorName"

    REMEDIAL_ACTION_MATCH_FIELD = "@id"

    def __init__(
        self,
        elastic: Elastic,
        areas_index: str = DEFAULT_AREAS_INDEX,
        contingencies_index: str = DEFAULT_CONTINGENCIES_INDEX,
        remedial_actions_index: str = DEFAULT_REMEDIAL_ACTIONS_INDEX,
        strict: bool = False,
        debug: bool = False,
    ):
        """Store Elastic/index settings and initialize per-run lookup caches."""
        self.elastic = elastic
        self.areas_index = areas_index
        self.contingencies_index = contingencies_index
        self.remedial_actions_index = remedial_actions_index
        self.strict = strict
        self.debug = debug
        self._area_by_eic: dict[str, dict[str, Any] | None] = {}
        self._party_by_eic: dict[str, dict[str, Any] | None] = {}
        self._contingency_by_identifier: dict[str, dict[str, Any] | None] = {}
        self._remedial_action_by_identifier: dict[str, dict[str, Any] | None] = {}

    def enrich(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Enrich a payload in place and return the same payload for fluent callers."""
        self.enrich_in_place(payload)
        return payload

    def enrich_in_place(self, payload: dict[str, Any]) -> None:
        """Dispatch enrichment to the workflow that matches the detected NC profile."""
        profile_type = self._profile_type(payload)
        logger.info(f"Enriching {profile_type} profile")
        if profile_type == "RAS":
            self._enrich_remedial_action_schedules(payload)
        elif profile_type == "SAR":
            self._enrich_base_case_power_flow_results(payload)
            self._enrich_contingency_power_flow_results(payload)
        logger.success(f"Enriched {profile_type} successfully")

    def _enrich_base_case_power_flow_results(self, payload: dict[str, Any]) -> None:
        """Add reported area names to SAR base-case power-flow results."""
        for result in self._section_items(payload, "BaseCasePowerFlowResult"):
            self._add_area_name(result, "ReportedByRegion")

    def _enrich_contingency_power_flow_results(self, payload: dict[str, Any]) -> None:
        """Add reported area and contingency details to SAR contingency results."""
        for result in self._section_items(payload, "ContingencyPowerFlowResult"):
            self._add_area_name(result, "ReportedByRegion")
            self._add_contingency_fields(result)

    def _enrich_remedial_action_schedules(self, payload: dict[str, Any]) -> None:
        """Add area, proposer, contingency, and action details to RAS schedules."""
        for schedule in self._section_items(payload, "RemedialActionSchedule"):
            self._add_area_name(schedule, "AssignedRegion")
            self._add_proposed_by_name(schedule, "ProposingEntity")
            self._add_contingency_fields(schedule)
            self._add_remedial_action_fields(schedule)

    def _add_area_name(self, item: dict[str, Any], source_key: str) -> None:
        """Resolve an area EIC from an item and write the display AreaName."""
        area_eic = item.get(source_key)
        if not area_eic:
            self._handle_missing_field(item, "AreaName", f"missing {source_key}")
            return
        area = self._get_area_by_eic(area_eic)
        area_name = self._get_path(area, "area.name") if area else None
        if not area_name:
            self._handle_missing_field(item, "AreaName", f"no area.name found for area EIC {area_eic}")
            return
        item["AreaName"] = area_name
        self._log_field_enriched(item, "AreaName", area_name)

    def _add_proposed_by_name(self, item: dict[str, Any], source_key: str) -> None:
        """Resolve the proposing party EIC and write the display ProposedByName."""
        party_eic = item.get("ProposingEntity")
        if not party_eic:
            self._handle_missing_field(item, "ProposedByName", f"missing {source_key}")
            return
        party = self._get_party_by_eic(party_eic)
        party_name = self._get_path(party, "party.name") if party else None
        if not party_name:
            self._handle_missing_field(item, "ProposedByName", f"no party.name found for party EIC {party_eic}")
            return
        item["ProposedByName"] = party_name
        self._log_field_enriched(item, "ProposedByName", party_name)

    def _add_contingency_fields(self, item: dict[str, Any]) -> None:
        """Look up the referenced contingency and copy configured fields onto an item."""
        contingency_id = item.get("Contingency")
        if not contingency_id:
            self._handle_missing_contingency_fields(item, "missing Contingency")
            return
        contingency = self._get_contingency_by_identifier(contingency_id)
        if not contingency:
            self._handle_missing_contingency_fields(item,f"no contingency document found for identifier {contingency_id}")
            return

        for field in self.CONTINGENCY_FIELD_ENRICHMENTS:
            value = self._get_path(contingency, field.source_path)
            if value:
                item[field.output_key] = value
                self._log_field_enriched(item, field.output_key, value)
                if field.output_key == "ContingencyOperatorEIC":
                    self._add_contingency_operator_name(item, value)
            else:
                reason = f"no contingency {field.missing_label} found for identifier {contingency_id}"
                self._handle_missing_field(item, field.output_key, reason)
                if field.output_key == "ContingencyOperatorEIC":
                    self._handle_missing_field(item, self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY, reason)

    def _add_contingency_operator_name(self, item: dict[str, Any], operator_eic: Any) -> None:
        """Resolve a contingency operator EIC and write ContingencyOperatorName."""
        if not operator_eic:
            self._handle_missing_field(item, self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY,f"missing contingency operator EIC {operator_eic}")
            return
        party = self._get_party_by_eic(operator_eic)
        party_name = self._get_path(party, "party.name") if party else None
        if not party_name:
            self._handle_missing_field(item, self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY,f"no party.name found for party EIC {operator_eic}",)
            return
        item[self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY] = party_name
        self._log_field_enriched(item, self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY, party_name)

    def _handle_missing_contingency_fields(self, item: dict[str, Any], reason: str) -> None:
        """Apply missing-field handling to every contingency enrichment output."""
        for field in self.CONTINGENCY_FIELD_ENRICHMENTS:
            self._handle_missing_field(item, field.output_key, reason)
        self._handle_missing_field(item, self.CONTINGENCY_OPERATOR_NAME_OUTPUT_KEY, reason)

    def _add_remedial_action_fields(self, item: dict[str, Any]) -> None:
        """Look up the referenced remedial action and copy configured fields."""
        remedial_action_id = item.get("RemedialAction")
        if not remedial_action_id:
            self._handle_missing_remedial_action_fields(item, "missing RemedialAction")
            return
        remedial_action = self._get_remedial_action_by_identifier(remedial_action_id)
        if not remedial_action:
            self._handle_missing_remedial_action_fields(item, f"no remedial action document found for identifier {remedial_action_id}")
            return

        for field in self.REMEDIAL_ACTION_FIELD_ENRICHMENTS:
            value = self._get_path(remedial_action, field.source_path)
            if value:
                item[field.output_key] = value
                self._log_field_enriched(item, field.output_key, value)
                if field.output_key == "RemedialActionOperatorEIC":
                    self._add_remedial_action_operator_name(item, value)
            else:
                reason = f"no remedial action {field.missing_label} found for identifier {remedial_action_id}"
                self._handle_missing_field(item, field.output_key, reason)
                if field.output_key == "RemedialActionOperatorEIC":
                    self._handle_missing_field(item, self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY, reason)

    def _add_remedial_action_operator_name(self, item: dict[str, Any], operator_eic: Any) -> None:
        """Resolve a remedial-action operator EIC and write its display name."""
        if not operator_eic:
            self._handle_missing_field(item, self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY, f"missing remedial action operator EIC {operator_eic}")
            return
        party = self._get_party_by_eic(operator_eic)
        party_name = self._get_path(party, "party.name") if party else None
        if not party_name:
            self._handle_missing_field(item, self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY,f"no party.name found for party EIC {operator_eic}")
            return
        item[self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY] = party_name
        self._log_field_enriched(item, self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY, party_name)

    def _handle_missing_remedial_action_fields(self, item: dict[str, Any], reason: str) -> None:
        """Apply missing-field handling to every remedial-action output."""
        for field in self.REMEDIAL_ACTION_FIELD_ENRICHMENTS:
            self._handle_missing_field(item, field.output_key, reason)
        self._handle_missing_field(item, self.REMEDIAL_ACTION_OPERATOR_NAME_OUTPUT_KEY, reason)

    def _get_area_by_eic(self, eic: str) -> dict[str, Any] | None:
        """Return an area document by EIC, using a local cache to avoid repeat queries."""
        if eic not in self._area_by_eic:
            self._area_by_eic[eic] = self._get_first_doc_by_exact_field(self.areas_index, "area.eic", eic)
        return self._area_by_eic[eic]

    def _get_party_by_eic(self, eic: str) -> dict[str, Any] | None:
        """Return a party document by EIC, using a local cache to avoid repeat queries."""
        if eic not in self._party_by_eic:
            self._party_by_eic[eic] = self._get_first_doc_by_exact_field(self.areas_index, "party.eic", eic)
        return self._party_by_eic[eic]

    def _get_contingency_by_identifier(self, identifier: str) -> dict[str, Any] | None:
        """Return a contingency document by card identifier, caching the result."""
        if identifier not in self._contingency_by_identifier:
            self._contingency_by_identifier[identifier] = self._get_first_doc_by_exact_field(self.contingencies_index, self.CONTINGENCY_MATCH_FIELD, identifier)
        return self._contingency_by_identifier[identifier]

    def _get_remedial_action_by_identifier(self, identifier: str) -> dict[str, Any] | None:
        """Return a remedial-action document by identifier, caching the result."""
        if identifier not in self._remedial_action_by_identifier:
            self._remedial_action_by_identifier[identifier] = self._get_first_doc_by_exact_field(self.remedial_actions_index, self.REMEDIAL_ACTION_MATCH_FIELD, identifier)
        return self._remedial_action_by_identifier[identifier]

    def _get_first_doc_by_exact_field(self, index: str, field: str, value: str) -> dict[str, Any] | None:
        """Query Elastic for the first document matching an exact field value."""
        query = {
            "bool": {
                "should": [
                    {"term": {field: value}},
                    {"term": {f"{field}.keyword": value}},
                ],
                "minimum_should_match": 1,
            }
        }
        if self.debug:
            logger.debug(f"Querying {index} for {field}={value}")
        hits = self.elastic.get_docs_by_query(index=index, query=query, size=1, return_df=False)
        docs = self._extract_source_docs(hits)
        return docs[0] if docs else None

    @staticmethod
    def _extract_source_docs(response: Any) -> list[dict[str, Any]]:
        """Normalize common Elastic/DataFrame response shapes into source documents."""
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

    def _profile_type(self, payload: dict[str, Any]) -> NCProfileType:
        """Determine the payload profile type through the validated FullModel path."""
        return self._validated_profile_type_from_full_model(payload)

    @staticmethod
    def _validated_profile_type_from_full_model(payload: dict[str, Any]) -> NCProfileType:
        """Read FullModel.keyword and validate it as a supported RAS or SAR type."""
        for full_model in CardDataEnricher._section_items(payload, "FullModel"):
            keyword = full_model.get("keyword")
            if isinstance(keyword, list):
                keyword_values = keyword
            else:
                keyword_values = [keyword]

            for keyword_value in keyword_values:
                normalized_keyword = CardDataEnricher._normalize_profile_keyword(keyword_value)
                if normalized_keyword == "RAS" or normalized_keyword == "SAR":
                    return normalized_keyword
        raise ValueError("FullModel keyword must be set to a supported NC profile type")

    @staticmethod
    def _normalize_profile_keyword(value: Any) -> str | None:
        """Convert a profile keyword value to uppercase text or None if blank."""
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    def _log_field_enriched(self, item: dict[str, Any], field_name: str, value: Any) -> None:
        """Emit a debug log for one enriched field when debug logging is enabled."""
        if self.debug:
            logger.debug(f"Enriched {field_name} for item {item.get('@id')}: {value}")

    @staticmethod
    def _section_items(payload: dict[str, Any], section_name: str) -> list[dict[str, Any]]:
        """Return a section as a list of item dictionaries regardless of input shape."""
        section = payload.get(section_name)
        if isinstance(section, dict):
            return [section]
        if isinstance(section, list):
            return [item for item in section if isinstance(item, dict)]
        return []

    @staticmethod
    def _get_path(document: dict[str, Any] | None, path: str) -> Any:
        """Read a dotted path from nested dictionaries, with list-first-item support."""
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

    def _handle_missing_field(self, item: dict[str, Any], field_name: str, reason: str) -> None:
        """Log missing enrichment data and optionally raise when strict mode is on."""
        item_id = item.get("@id") or "<unknown>"
        message = f"Failed to enrich {field_name} for item {item_id}: {reason}"
        logger.warning(message)
        if self.strict:
            raise ValueError(message)


if __name__ == "__main__":
    from card_publicator.rdf_converter import convert_cim_rdf_to_json

    def enrich_nc_xml_file(
        input_path: str = r"C:\Users\lukas.navickas\Documents\Opcoord_testing\example_cards\test_ras_2.xml",
        output_path: str = r"C:\Users\lukas.navickas\Documents\Opcoord_testing\enriched_cards\enriched_card_ras_2.json",
        strict: bool = False,  # If strict = True, return ValueError for missing enrichment fields and fails
        debug: bool = True,  # enable extended warning logs
        indent: int = 2,
    ) -> dict[str, Any]:
        """Convert one local NC XML card to JSON, enrich it, and write the result."""

        # This local helper is only for manual testing. Production code should use
        # CardDataEnricher directly after the card payload has already been converted.
        input_path = Path(input_path)
        output_path = Path(output_path)

        # Convert the NC RDF/XML file to the same local-key JSON shape used by the
        # card builder workflow before applying enrichment fields.
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

        # Reuse the repository Elastic integration and default enrichment indices.
        enricher = CardDataEnricher(
            elastic=Elastic(debug=debug),
            areas_index=DEFAULT_AREAS_INDEX,
            contingencies_index=DEFAULT_CONTINGENCIES_INDEX,
            remedial_actions_index=DEFAULT_REMEDIAL_ACTIONS_INDEX,
            strict=strict,
            debug=debug,
        )
        enricher.enrich(payload)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as output_file:
            json.dump(payload, output_file, ensure_ascii=False, indent=indent)
            output_file.write("\n")

        return payload

    enrich_nc_xml_file()
