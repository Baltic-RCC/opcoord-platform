from loguru import logger
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_serializer
from typing import Optional, List, Any, Dict, Literal


class Card(BaseModel):
    publisher: str = Field(default="rcc-service-user")
    processVersion: str
    process: str
    processInstanceId: str
    state: str
    severity: Literal["INFORMATION", "ALARM", "ACTION", "COMPLIANT"] = "INFORMATION"
    startDate: datetime
    title: Dict[str, str]
    summary: Dict[str, str]
    # Optional fields
    groupRecipients: Optional[List[str]] = None
    entityRecipients: Optional[List[str]] = None
    userRecipients: Optional[List[str]] = None
    entitiesAllowedToRespond: Optional[List[str]] = None
    endDate: Optional[datetime] = None
    expirationDate: Optional[datetime] = None
    tag: Optional[str] = None
    lttd: Optional[datetime] = None
    secondsBeforeTimeSpanForReminder: Optional[int] = None
    publisherType: Optional[Literal["EXTERNAL", "ENTITY"]] = None
    representative: Optional[Literal["EXTERNAL", "ENTITY"]] = None
    actions: Optional[Literal["KEEP_CHILD_CARDS",
        "PROPAGATE_READ_ACK_TO_PARENT_CARD", "KEEP_EXISTING_ACKS_AND_READS", "STORE_ONLY_IN_ARCHIVES",  "NOT_NOTIFIED"]] = None
    # Geodata
    wktGeometry: Optional[str] = None  # "POINT (2.3498 48.8530)"
    wktProjection: Optional[str] = None  # "EPSG:4326"
    # Content
    data: Optional[Any] = {}

    @field_serializer('startDate', 'endDate')
    def _ser_start_date(self, v: datetime, _info):
        if v.tzinfo is None:
            logger.warning("startDate is naive datetime, assuming UTC")
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp() * 1000)


if __name__ == "__main__":
    conf = get_settings()
    print(conf)
