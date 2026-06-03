from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from loguru import logger

from integrations.elastic import Elastic


DEFAULT_AREAS_INDEX = "config-areas"
DEFAULT_CONTINGENCIES_INDEX = "csa-contingencies-*"

NCProfileType = Literal["RAS", "SAR"]


@dataclass(frozen=True)
class EnrichmentStats:
    base_case_power_flow_results: int = 0
    contingency_power_flow_results: int = 0
    remedial_action_schedules: int = 0
    area_names_added: int = 0
    proposed_by_names_added: int = 0
    contingency_names_added: int = 0
    contingency_types_added: int = 0

    @property
    def fields_enriched(self) -> int:
        return (
            self.area_names_added
            + self.proposed_by_names_added
            + self.contingency_names_added
            + self.contingency_types_added
        )

    @property
    def fields_failed(self) -> int:
        fields_attempted = (
            self.base_case_power_flow_results
            + (self.contingency_power_flow_results * 3)
            + (self.remedial_action_schedules * 4)
        )
        return fields_attempted - self.fields_enriched


class CardDataEnricher:
    CONTINGENCY_MATCH_FIELDS = (
        "@id",
        "mRID",
        "ContingencyEquipment.Contingency",
        "ContingencyEquipment.mRID",
        "ContingencyEquipment.@id",
    )

    def __init__(
        self,
        elastic: Elastic,
        areas_index: str = DEFAULT_AREAS_INDEX,
        contingencies_index: str = DEFAULT_CONTINGENCIES_INDEX,
        strict: bool = False,
        debug: bool = False,
    ):
        self.elastic = elastic
        self.areas_index = areas_index
        self.contingencies_index = contingencies_index
        self.strict = strict
        self.debug = debug
        self._area_by_eic: dict[str, dict[str, Any] | None] = {}
        self._party_by_eic: dict[str, dict[str, Any] | None] = {}
        self._contingency_by_identifier: dict[str, dict[str, Any] | None] = {}

    def enrich(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.enrich_in_place(payload)
        return payload

    def enrich_in_place(self, payload: dict[str, Any]) -> EnrichmentStats:
        profile_type = self._profile_type(payload)
        logger.info(f"Enriching {profile_type} profile")
        stats = EnrichmentStats()
        if profile_type == "RAS":
            stats = self._enrich_remedial_action_schedules(payload, stats)
        elif profile_type == "SAR":
            stats = self._enrich_base_case_power_flow_results(payload, stats)
            stats = self._enrich_contingency_power_flow_results(payload, stats)
        logger.success(f"Enriched {profile_type} successfully, "f"{stats.fields_enriched} count of fields enriched, "f"{stats.fields_failed} count of fields failed to enrich.")
        return stats

    def _enrich_base_case_power_flow_results(self, payload: dict[str, Any], stats: EnrichmentStats) -> EnrichmentStats:
        for result in self._section_items(payload, "BaseCasePowerFlowResult"):
            area_added = self._add_area_name(result, "ReportedByRegion")
            stats = EnrichmentStats(
                base_case_power_flow_results=stats.base_case_power_flow_results + 1,
                contingency_power_flow_results=stats.contingency_power_flow_results,
                remedial_action_schedules=stats.remedial_action_schedules,
                area_names_added=stats.area_names_added + int(area_added),
                proposed_by_names_added=stats.proposed_by_names_added,
                contingency_names_added=stats.contingency_names_added,
                contingency_types_added=stats.contingency_types_added,
            )
        return stats

    def _enrich_contingency_power_flow_results(self, payload: dict[str, Any], stats: EnrichmentStats) -> EnrichmentStats:
        for result in self._section_items(payload, "ContingencyPowerFlowResult"):
            area_added = self._add_area_name(result, "ReportedByRegion")
            contingency_name_added, contingency_type_added = self._add_contingency_fields(result)
            stats = EnrichmentStats(
                base_case_power_flow_results=stats.base_case_power_flow_results,
                contingency_power_flow_results=stats.contingency_power_flow_results + 1,
                remedial_action_schedules=stats.remedial_action_schedules,
                area_names_added=stats.area_names_added + int(area_added),
                proposed_by_names_added=stats.proposed_by_names_added,
                contingency_names_added=stats.contingency_names_added + int(contingency_name_added),
                contingency_types_added=stats.contingency_types_added + int(contingency_type_added),
            )
        return stats

    def _enrich_remedial_action_schedules(self, payload: dict[str, Any], stats: EnrichmentStats) -> EnrichmentStats:
        for schedule in self._section_items(payload, "RemedialActionSchedule"):
            area_added = self._add_area_name(schedule, "AssignedRegion")
            proposed_by_added = self._add_proposed_by_name(schedule)
            contingency_name_added, contingency_type_added = self._add_contingency_fields(schedule)
            stats = EnrichmentStats(
                base_case_power_flow_results=stats.base_case_power_flow_results,
                contingency_power_flow_results=stats.contingency_power_flow_results,
                remedial_action_schedules=stats.remedial_action_schedules + 1,
                area_names_added=stats.area_names_added + int(area_added),
                proposed_by_names_added=stats.proposed_by_names_added + int(proposed_by_added),
                contingency_names_added=stats.contingency_names_added + int(contingency_name_added),
                contingency_types_added=stats.contingency_types_added + int(contingency_type_added),
            )
        return stats

    def _add_area_name(self, item: dict[str, Any], source_key: str) -> bool:
        area_eic = self._normalize_eic_reference(item.get(source_key))
        if not area_eic:
            self._handle_missing(f"Missing {source_key} in item {item.get('@id')}")
            return False
        area = self._get_area_by_eic(area_eic)
        area_name = self._get_path(area, "area.name") if area else None
        if not area_name:
            self._handle_missing(f"No area.name found for area EIC {area_eic}")
            return False
        item["AreaName"] = area_name
        self._log_field_enriched(item, "AreaName", area_name)
        return True

    def _add_proposed_by_name(self, item: dict[str, Any]) -> bool:
        party_eic = self._normalize_eic_reference(item.get("ProposingEntity"))
        if not party_eic:
            self._handle_missing(f"Missing ProposingEntity in item {item.get('@id')}")
            return False
        party = self._get_party_by_eic(party_eic)
        party_name = self._get_path(party, "party.name") if party else None
        if not party_name:
            self._handle_missing(f"No party.name found for party EIC {party_eic}")
            return False
        item["ProposedByName"] = party_name
        self._log_field_enriched(item, "ProposedByName", party_name)
        return True

    def _add_contingency_fields(self, item: dict[str, Any]) -> tuple[bool, bool]:
        contingency_id = self._normalize_identifier_reference(item.get("Contingency"))
        if not contingency_id:
            self._handle_missing(f"Missing Contingency in item {item.get('@id')}")
            return False, False
        contingency = self._get_contingency_by_identifier(contingency_id)
        if not contingency:
            self._handle_missing(f"No contingency document found for identifier {contingency_id}")
            return False, False

        name = self._first_present(contingency, "name", "ContingencyEquipment.name")
        contingency_type = self._first_present(contingency, "kind", "@type", "ContingencyEquipment.@type")
        name_added = False
        type_added = False
        if name:
            item["ContingencyName"] = name
            self._log_field_enriched(item, "ContingencyName", name)
            name_added = True
        else:
            self._handle_missing(f"No contingency name found for identifier {contingency_id}")
        if contingency_type:
            item["ContingencyType"] = contingency_type
            self._log_field_enriched(item, "ContingencyType", contingency_type)
            type_added = True
        else:
            self._handle_missing(f"No contingency type found for identifier {contingency_id}")
        return name_added, type_added

    def _get_area_by_eic(self, eic: str) -> dict[str, Any] | None:
        if eic not in self._area_by_eic:
            self._area_by_eic[eic] = self._get_first_doc_by_exact_field(self.areas_index, "area.eic", eic)
        return self._area_by_eic[eic]

    def _get_party_by_eic(self, eic: str) -> dict[str, Any] | None:
        if eic not in self._party_by_eic:
            self._party_by_eic[eic] = self._get_first_doc_by_exact_field(self.areas_index, "party.eic", eic)
        return self._party_by_eic[eic]

    def _get_contingency_by_identifier(self, identifier: str) -> dict[str, Any] | None:
        if identifier in self._contingency_by_identifier:
            return self._contingency_by_identifier[identifier]

        variants = self._identifier_variants(identifier)
        for field in self.CONTINGENCY_MATCH_FIELDS:
            for variant in variants:
                doc = self._get_first_doc_by_exact_field(self.contingencies_index, field, variant)
                if doc:
                    self._contingency_by_identifier[identifier] = doc
                    return doc
        self._contingency_by_identifier[identifier] = None
        return None

    def _get_first_doc_by_exact_field(self, index: str, field: str, value: str) -> dict[str, Any] | None:
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
        return self._validated_profile_type_from_full_model(payload)

    @staticmethod
    def _validated_profile_type_from_full_model(payload: dict[str, Any]) -> NCProfileType:
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
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    def _log_field_enriched(self, item: dict[str, Any], field_name: str, value: Any) -> None:
        if self.debug:
            logger.debug(f"Enriched {field_name} for item {item.get('@id')}: {value}")

    @staticmethod
    def _section_items(payload: dict[str, Any], section_name: str) -> list[dict[str, Any]]:
        section = payload.get(section_name)
        if isinstance(section, dict):
            return [section]
        if isinstance(section, list):
            return [item for item in section if isinstance(item, dict)]
        return []

    @staticmethod
    def _normalize_eic_reference(value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        if "/" in normalized:
            normalized = normalized.rsplit("/", 1)[-1]
        if "#" in normalized:
            normalized = normalized.rsplit("#", 1)[-1]
        return normalized or None

    @staticmethod
    def _normalize_identifier_reference(value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        if normalized.startswith("#"):
            normalized = normalized[1:]
        return normalized or None

    @staticmethod
    def _identifier_variants(identifier: str) -> list[str]:
        stripped = identifier[1:] if identifier.startswith("_") else identifier
        with_underscore = f"_{stripped}"
        variants = [identifier, with_underscore, stripped]
        return list(dict.fromkeys(value for value in variants if value))

    @staticmethod
    def _get_path(document: dict[str, Any] | None, path: str) -> Any:
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

    @staticmethod
    def _first_present(document: dict[str, Any], *paths: str) -> Any:
        for path in paths:
            value = CardDataEnricher._get_path(document, path)
            if value is not None and value != "":
                return value
        return None

    def _handle_missing(self, message: str) -> None:
        if self.strict:
            raise ValueError(message)
        if self.debug:
            logger.warning(message)


if __name__ == "__main__":
    from card_publicator.rdf_converter import convert_cim_rdf_to_json

    def enrich_nc_xml_file(
        input_path: str = r"C:\Users\lukas.navickas\Documents\Opcoord_testing\example_cards\SAR_20260520T1130_ID_1.xml",
        output_path: str = r"C:\Users\lukas.navickas\Documents\Opcoord_testing\enriched_cards\enriched_card_sar_ID.json",
        strict: bool = False, # If strict = True, return ValueError for missing enrichment fields and fails
        debug: bool = True, # enable extended warning logs
        indent: int = 2,
    ) -> dict[str, Any]:
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
