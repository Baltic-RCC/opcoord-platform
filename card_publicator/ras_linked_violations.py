"""Link a one-schedule RAS proposal to violations in an earlier SAR card."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

from loguru import logger


class RasLinkedViolationEnricher:
    """Load a matching SAR card from OperatorFabric and normalize violations."""

    def __init__(
        self,
        opfab: Any,
        *,
        sar_process: str,
        sar_state: str,
        search_size: int = 100,
    ) -> None:
        self.opfab = opfab
        self.sar_process = sar_process
        self.sar_state = sar_state
        self.search_size = search_size

    def enrich_in_place(
        self,
        payload: dict[str, Any],
        *,
        sar_card_id: str | None = None,
    ) -> None:
        """Add ``violations`` to RAS data without blocking card publication."""

        payload["violations"] = []
        try:
            schedule = self._single_schedule(payload)
            contingency_id = self._normalize_id(schedule.get("Contingency"))
            if not contingency_id:
                raise ValueError("The RAS schedule has no Contingency identifier")

            sar_cards = (
                [self._get_card(sar_card_id)]
                if sar_card_id
                else self._find_matching_sar_cards(payload, contingency_id)
            )
            if not sar_cards:
                logger.warning(
                    "No visible SAR card matches RAS contingency {} and hourly slots",
                    contingency_id,
                )
                return

            violations = [
                violation
                for sar_card in sar_cards
                for violation in self._build_violations(sar_card, contingency_id)
            ]
            linked_ids = [
                str(sar_card.get("id"))
                for sar_card in sar_cards
                if sar_card.get("id")
            ]
            if sar_card_id and not linked_ids:
                linked_ids = [sar_card_id]

            payload["violations"] = violations
            if len(linked_ids) == 1:
                payload["linkedSarCardId"] = linked_ids[0]
            elif linked_ids:
                payload["linkedSarCardIds"] = linked_ids
            logger.info(
                "Linked {} SAR violation(s) from {} SAR card(s) to RAS contingency {}",
                len(violations),
                len(sar_cards),
                contingency_id,
            )
        except Exception as error:
            logger.warning("Unable to enrich RAS linked violations: {}", error)

    def _find_matching_sar_cards(
        self,
        ras_payload: Mapping[str, Any],
        contingency_id: str,
    ) -> list[dict[str, Any]]:
        expected_scenarios = self._ras_scenario_times(ras_payload)
        selected = []
        for scenario in expected_scenarios:
            response = self.opfab.search_cards(
                [
                    self._equals_filter("process", self.sar_process),
                    self._equals_filter("state", self.sar_state),
                    self._equals_filter(
                        "startDate", str(int(scenario.timestamp() * 1000))
                    ),
                ],
                size=self.search_size,
            )
            candidates = []
            for light_card in self._response_items(response):
                # Keep validating the searchable fields because some deployed
                # OperatorFabric versions have returned unfiltered rows.
                if light_card.get("process") != self.sar_process:
                    continue
                if light_card.get("state") != self.sar_state:
                    continue
                if self._parse_datetime(light_card.get("startDate")) != scenario:
                    continue
                card_id = light_card.get("id")
                if not card_id:
                    continue
                card = self._get_card(str(card_id))
                if (
                    self._matching_sar_scenario(
                        card, ras_payload, contingency_id, [scenario]
                    )
                    == scenario
                ):
                    candidates.append(card)

            if not candidates:
                logger.warning(
                    "No SAR card matches RAS contingency {} at expected scenario {}",
                    contingency_id,
                    scenario.isoformat(),
                )
                continue
            if len(candidates) > 1:
                logger.warning(
                    "Several SAR cards match RAS contingency {} at {}; using the latest",
                    contingency_id,
                    scenario.isoformat(),
                )
            selected.append(max(candidates, key=self._card_sort_key))
        return selected

    def _matching_sar_scenario(
        self,
        sar_card: Mapping[str, Any],
        ras_payload: Mapping[str, Any],
        contingency_id: str,
        expected_scenarios: list[datetime],
    ) -> datetime | None:
        sar_data = sar_card.get("data") or {}
        if not isinstance(sar_data, Mapping):
            return None
        if not any(
            self._normalize_id(result.get("Contingency")) == contingency_id
            for result in self._items(sar_data, "ContingencyPowerFlowResult")
        ):
            return None

        ras_run = self._run_key((ras_payload.get("FullModel") or {}).get("wasGeneratedBy"))
        sar_run = self._run_key((sar_data.get("FullModel") or {}).get("wasGeneratedBy"))
        if ras_run and sar_run and ras_run != sar_run:
            return None

        sar_scenario = self._parse_datetime(
            (sar_data.get("FullModel") or {}).get("scenarioTime")
        )
        return sar_scenario if sar_scenario in expected_scenarios else None

    def _build_violations(
        self, sar_card: Mapping[str, Any], contingency_id: str
    ) -> list[dict[str, Any]]:
        data = sar_card.get("data") or {}
        base_results = self._items(data, "BaseCasePowerFlowResult")
        base_by_terminal_and_time = {
            (
                self._normalize_id(item.get("ACDCTerminal")),
                self._time_key(item.get("atTime")),
            ): item
            for item in base_results
        }
        base_by_terminal = {
            self._normalize_id(item.get("ACDCTerminal")): item
            for item in base_results
        }

        violations = []
        for result in self._items(data, "ContingencyPowerFlowResult"):
            if self._normalize_id(result.get("Contingency")) != contingency_id:
                continue
            if not self._as_bool(result.get("isViolation")):
                continue
            terminal = self._normalize_id(result.get("ACDCTerminal"))
            at_time = self._time_key(result.get("atTime"))
            base = base_by_terminal_and_time.get((terminal, at_time))
            if base is None:
                base = base_by_terminal.get(terminal, {})
            measurement_type, unit = self._measurement(result)
            violations.append(
                {
                    "id": result.get("@id") or result.get("mRID"),
                    "elementId": result.get("ACDCTerminal"),
                    "elementName": result.get("EquipmentName") or terminal,
                    "measurementType": measurement_type,
                    "initialValue": base.get("absoluteValue"),
                    "postContingencyValue": result.get("absoluteValue"),
                    "unit": unit,
                    "loadingPercent": result.get("value"),
                    "operationalLimit": result.get("OperationalLimitValue"),
                    "atTime": result.get("atTime"),
                    "contingency": result.get("Contingency"),
                }
            )
        return violations

    def _get_card(self, card_id: str | None) -> dict[str, Any]:
        if not card_id:
            raise ValueError("SAR card id is empty")
        response = self.opfab.get_card(card_id)
        card = response.json() if hasattr(response, "json") else response
        if isinstance(card, dict) and isinstance(card.get("card"), dict):
            card = card["card"]
        if not isinstance(card, dict):
            raise ValueError(f"OperatorFabric card {card_id} is not a JSON object")
        return card

    @staticmethod
    def _single_schedule(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        schedules = payload.get("RemedialActionSchedule", [])
        if isinstance(schedules, Mapping):
            schedules = [schedules]
        if not isinstance(schedules, list) or len(schedules) != 1:
            raise ValueError("Linked-violation enrichment requires exactly one RAS schedule")
        return schedules[0]

    @staticmethod
    def _items(payload: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
        value = payload.get(key, [])
        if isinstance(value, Mapping):
            value = [value]
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    @staticmethod
    def _response_items(response: Any) -> list[dict[str, Any]]:
        body = response.json() if hasattr(response, "json") else response
        if isinstance(body, list):
            items = body
        elif isinstance(body, dict):
            items = []
            for key in ("content", "cards", "items"):
                value = body.get(key)
                if isinstance(value, list):
                    items = value
                    break
        else:
            items = []

        normalized = []
        for item in items:
            if not isinstance(item, dict):
                continue
            # The deployed OperatorFabric version wraps selected fields under
            # ``latestUpdateOnly`` and exposes the card id as ``_id``.
            wrapped = item.get("latestUpdateOnly")
            if isinstance(wrapped, dict):
                item = wrapped
            normalized_item = dict(item)
            if "id" not in normalized_item and normalized_item.get("_id"):
                normalized_item["id"] = normalized_item["_id"]
            normalized.append(normalized_item)
        return normalized

    @staticmethod
    def _equals_filter(column: str, value: str) -> dict[str, Any]:
        return {
            "columnName": column,
            "matchType": "EQUALS",
            "filter": [value],
            "operation": "AND",
        }

    @classmethod
    def _ras_scenario_times(cls, payload: Mapping[str, Any]) -> list[datetime]:
        """Return the SAR midpoint expected for every complete RAS hour.

        Current RAS cards cover one hourly slot. Returning all hourly midpoints
        keeps the lookup ready for a future multi-hour RAS report without
        changing the matching contract.
        """

        full_model = payload.get("FullModel") or {}
        start = cls._parse_datetime(full_model.get("startDate"))
        end = cls._parse_datetime(full_model.get("endDate"))
        if start is None or end is None:
            raise ValueError("RAS FullModel startDate and endDate are required")
        duration = end - start
        if duration <= timedelta(0) or duration % timedelta(hours=1):
            raise ValueError("RAS FullModel interval must contain complete hourly slots")

        scenarios = []
        slot_start = start
        while slot_start < end:
            scenarios.append(slot_start + timedelta(minutes=30))
            slot_start += timedelta(hours=1)
        return scenarios

    @staticmethod
    def _card_sort_key(card: Mapping[str, Any]) -> tuple[int, int]:
        full_model = (card.get("data") or {}).get("FullModel") or {}
        version = full_model.get("version")
        try:
            version_number = int(version)
        except (TypeError, ValueError):
            version_number = 0
        publish_date = card.get("publishDate")
        try:
            publish_number = int(publish_date)
        except (TypeError, ValueError):
            publish_number = 0
        return version_number, publish_number

    @staticmethod
    def _run_key(value: Any) -> str:
        parts = str(value or "").split("-")
        if parts and parts[-1].upper() in {"RAS", "SAR"}:
            parts.pop()
        return "-".join(parts).upper()

    @staticmethod
    def _normalize_id(value: Any) -> str:
        return str(value or "").strip().lstrip("#_")

    @staticmethod
    def _measurement(result: Mapping[str, Any]) -> tuple[str, str | None]:
        if result.get("valueA") is not None:
            return "CURRENT", "A"
        if result.get("valueV") is not None:
            return "VOLTAGE", "V"
        return "POWER_FLOW", None

    @staticmethod
    def _as_bool(value: Any) -> bool:
        return value is True or str(value).strip().lower() == "true"

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000, tz=UTC)
        try:
            parsed = (
                value
                if isinstance(value, datetime)
                else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            )
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @classmethod
    def _time_key(cls, value: Any) -> str:
        parsed = cls._parse_datetime(value)
        return parsed.isoformat() if parsed else str(value or "")


if __name__ == "__main__":
    # Local OperatorFabric query harness. Change these values in VS Code and run
    # this file directly; none of this is used when the worker imports the class.
    TEST_MODE = "SEARCH"  # SEARCH or GET
    TEST_CARD_ID = None  # Required in GET mode, for example "crosa.<instance-id>"

    # Expected SAR midpoint derived from the RAS interval being investigated.
    SEARCH_SCENARIO_TIME = "2026-08-13T01:30:00Z"
    SEARCH_SIZE = 20
    FETCH_FULL_SEARCH_RESULTS = False

    from builders import config
    from integrations import opfab

    client = opfab.AuthenticatedSession()
    sar_config = config["sar"]
    enricher = RasLinkedViolationEnricher(
        client,
        sar_process=sar_config["process"],
        sar_state=sar_config["state"],
    )

    if TEST_MODE == "SEARCH":
        scenario = enricher._parse_datetime(SEARCH_SCENARIO_TIME)
        if scenario is None:
            raise ValueError("SEARCH_SCENARIO_TIME must be a valid date-time")
        filters = [
            RasLinkedViolationEnricher._equals_filter(
                "process", sar_config["process"]
            ),
            RasLinkedViolationEnricher._equals_filter("state", sar_config["state"]),
            RasLinkedViolationEnricher._equals_filter(
                "startDate", str(int(scenario.timestamp() * 1000))
            ),
        ]
        response = client.search_cards(filters, size=SEARCH_SIZE)
        body = response.json()
        print("\nRaw OperatorFabric search response:\n")
        print(json.dumps(body, ensure_ascii=False, indent=2, default=str))

        if FETCH_FULL_SEARCH_RESULTS:
            cards = []
            for light_card in enricher._response_items(body):
                card_id = light_card.get("id")
                if card_id:
                    cards.append(enricher._get_card(str(card_id)))
            print("\nFull cards returned for the search results:\n")
            print(json.dumps(cards, ensure_ascii=False, indent=2, default=str))
    elif TEST_MODE == "GET":
        if not TEST_CARD_ID:
            raise ValueError("Set TEST_CARD_ID before running in GET mode")
        response = client.get_card(TEST_CARD_ID)
        print("\nRaw OperatorFabric card response:\n")
        print(json.dumps(response.json(), ensure_ascii=False, indent=2, default=str))
    else:
        raise ValueError(f"Unsupported TEST_MODE: {TEST_MODE}")
