from loguru import logger
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_serializer
from typing import Optional, List, Any, Dict, Literal


# Native OperatorFabric actions accepted by the card publication API.
CardAction = Literal[
    "PROPAGATE_READ_ACK_TO_PARENT_CARD",
    "KEEP_CHILD_CARDS",
    "KEEP_EXISTING_ACKS_AND_READS",
    "KEEP_EXISTING_PUBLISH_DATE",
    "STORE_ONLY_IN_ARCHIVES",
    "NOT_NOTIFIED",
]


class Card(BaseModel):
    # Mandatory fields - Native OperatorFabric identity and process fields.
    publisher: str
    processVersion: str
    process: str
    processInstanceId: str
    state: str
    severity: Literal["INFORMATION", "ALARM", "ACTION", "COMPLIANT"] = "INFORMATION"
    startDate: datetime
    title: Dict[str, str]
    summary: Dict[str, str]

    # Native OperatorFabric routing and response fields.
    groupRecipients: Optional[List[str]] = None
    entityRecipients: Optional[List[str]] = None
    entityRecipientsForInformation: Optional[List[str]] = None
    userRecipients: Optional[List[str]] = None
    externalRecipients: Optional[List[str]] = None
    entitiesAllowedToEdit: Optional[List[str]] = None
    entitiesAllowedToRespond: Optional[List[str]] = None
    entitiesRequiredToRespond: Optional[List[str]] = None

    # Native OperatorFabric business-time and lifecycle fields.
    endDate: Optional[datetime] = None
    expirationDate: Optional[datetime] = None
    lttd: Optional[datetime] = None
    secondsBeforeTimeSpanForReminder: Optional[int] = None
    timeSpans: Optional[List[Dict[str, int]]] = None
    rRule: Optional[Dict[str, Any]] = None

    # Native OperatorFabric publication metadata and update behavior.
    tags: Optional[List[str]] = None
    publisherType: Optional[Literal["EXTERNAL", "ENTITY"]] = None
    representative: Optional[str] = None
    representativeType: Optional[Literal["EXTERNAL", "ENTITY"]] = None
    actions: Optional[List[CardAction]] = None

    # Native OperatorFabric geodata fields.
    wktGeometry: Optional[str] = None  # "POINT (2.3498 48.8530)"
    wktProjection: Optional[str] = None  # "EPSG:4326"

    # Application-defined business payload.
    data: Optional[Any] = Field(default_factory=dict)

    @field_serializer('startDate', 'endDate', 'expirationDate', 'lttd')
    def _ser_card_date(self, v: datetime, _info):
        if v.tzinfo is None:
            logger.warning("startDate is naive datetime, assuming UTC")
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp() * 1000)


if __name__ == "__main__":
    card = Card()
    print(card)
