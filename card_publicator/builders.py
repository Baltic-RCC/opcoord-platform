from typing import Any, Dict
from models import Card
from datetime import timedelta, datetime
from rdf_converter import convert_cim_rdf_to_json
import yaml
from pathlib import Path


# Load card configuration from YAML file
with open("cards.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)


class CardFactory:
    def __init__(self):
        self._builders = {
            "sar": SarProfileCardBuilder(),
            "ras": None,
        }

    def build(self, card_type: str, card_fields: Dict[str, Any], data: Any) -> Card:
        builder = self._builders[card_type]
        return builder.build(content=data, card_fields=card_fields)


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


if __name__ == '__main__':
    # Testing
    from datetime import datetime
    import uuid
    content_path = Path(__file__).parent.parent.joinpath("tests/data/nc_sar.xml")
    card_field = {"startDate": datetime.now(), "processInstanceId": str(uuid.uuid4())}
    response = SarProfileCardBuilder().build(content=content_path, card_fields=card_field)