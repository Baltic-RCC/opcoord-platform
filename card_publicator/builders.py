from typing import Any, Dict
from models import Card
from datetime import timedelta, datetime
from rdf_converter import convert_cim_rdf_to_json
import yaml
from pathlib import Path


# Load card configuration from YAML file
path = Path(__file__).parent.joinpath("cards.yaml")
with open(path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)


class CardFactory:
    def __init__(self):
        self._builders = {
            "sar": SarProfileCardBuilder(),
            "ras": RasProfileCardBuilder(),
        }

    def build(self, card_type: str, card_fields: Dict[str, Any], data: Any) -> Card:
        builder = self._builders[card_type]
        return builder.build(content=data, card_fields=card_fields)

    def apply_post_enrichment_fields(self, card_type: str, card: Card) -> None:
        """Apply profile fields that are only available after enrichment."""

        if card_type == "ras":
            self._builders[card_type].apply_target_tso_routing(card)

class SarProfileCardBuilder:
    def __init__(self):
        pass

    def build(self, content: str, card_fields: Dict[str, Any]) -> Card:
        # Convert SAR rdfxml to json
        converted = convert_cim_rdf_to_json(
            content,
            root_class=["BaseCasePowerFlowResult", "ContingencyPowerFlowResult"],
            key_mode="local",
        )

        # Add end date field to 1 hours granularity for SAR profile
        if "startDate" in card_fields:
            end_date = datetime.fromisoformat(card_fields["startDate"]) + timedelta(hours=1)
            card_fields["endDate"] = end_date

        # Build card using config and converted data
        card = Card(**config["sar"], **card_fields, data=converted)

        return card

class RasProfileCardBuilder:
    def __init__(self):
        pass

    def build(self, content: str, card_fields: Dict[str, Any]) -> Card:
        # Convert RAS rdfxml to json
        converted = convert_cim_rdf_to_json(
            content,
            root_class=["RemedialActionSchedule"],
            key_mode="local",
        )

        # Keep static RAS presentation fields beside the native converted data.
        ras_data = {**config["ras"].get("data", {}), **converted}

        # Build card using config, runtime fields, and converted data.
        ras_config = {**card_fields, **config["ras"], "data": ras_data}
        card = Card(**ras_config)

        return card

    @staticmethod
    def apply_target_tso_routing(card: Card) -> None:
        """Route one RAS proposal to the schedule's operating TSO and the publisher.

        The current 1D RAS message contract contains one schedule. Supporting
        several schedules is intentionally left to a future builder design.
        """

        schedules = card.data["RemedialActionSchedule"][0]
        target_entity = schedules["RemedialActionOperatorEIC"]
        card.data["targetEntity"] = target_entity
        card.entityRecipients = [card.publisher, target_entity]
        card.entitiesAllowedToRespond = [target_entity]
        card.entitiesRequiredToRespond = [target_entity]

if __name__ == '__main__':
    # Testing
    from datetime import datetime
    import uuid
    content_path = Path(__file__).parent.parent.joinpath("tests/data/nc_ras.xml")
    card_field = {"startDate": datetime.now().isoformat(), "processInstanceId": str(uuid.uuid4()), "publisher": "lukas"}
    response = SarProfileCardBuilder().build(content=content_path, card_fields=card_field)
