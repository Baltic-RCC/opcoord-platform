from loguru import logger
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict, Literal


class Card(BaseModel):
    publisher: str = Field(default="rcc-service-user")
    processVersion: str
    process: str
    processInstanceId: str
    severity: Literal["INFORMATION", "ALARM", "ACTION", "COMPLIANT"] = "INFORMATION"
    startDate: datetime
    title: Dict[str, str]
    summary: Dict[str, str]
    # Optional fields
    groupRecipients: List[str]
    entityRecipients: List[str]
    userRecipients: List[str]
    entitiesAllowedToRespond: List[str]
    endDate: Optional[datetime] = None
    expirationDate: Optional[datetime] = None
    tag: Optional[str]
    lttd: Optional[datetime]
    secondsBeforeTimeSpanForReminder: Optional[int] = None
    publisherType: Optional[Literal["EXTERNAL", "ENTITY"]] = None
    representative: Optional[Literal["EXTERNAL", "ENTITY"]] = None
    actions: Optional[Literal["KEEP_CHILD_CARDS",
        "PROPAGATE_READ_ACK_TO_PARENT_CARD", "KEEP_EXISTING_ACKS_AND_READS", "STORE_ONLY_IN_ARCHIVES",  "NOT_NOTIFIED"]]
    # Geodata
    wktGeometry: Optional[str] = None  # "POINT (2.3498 48.8530)"
    wktProjection: Optional[str] = None  # "EPSG:4326"
    # Content
    data: Optional[Any]


if __name__ == "__main__":
    conf = get_settings()
    print(conf)
