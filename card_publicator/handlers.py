import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pika import BasicProperties
import builders
from integrations import elastic, opfab
from loguru import logger
import settings


conf = settings.get_settings()


class RootPublicationHandler:

    def __init__(self, debug: bool = conf.publicator.debug):

        self.debug = debug

        # Services initialization
        try:
            self.elastic = elastic.Elastic()
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch service: {e}")

        try:
            self.opfab = opfab.AuthenticatedSession()
        except Exception as e:
            logger.error(f"Failed to initialize OperatorFabric service: {e}")

    def handle(self, message: bytes, properties: object, **kwargs):
        """
        Process received message and return data for publication.
        """
        # Get unique message-id from headers, if not there - create
        message_id = properties.headers.get('message-id', str(uuid.uuid4()))
        logger.info(f"Handling message with id: {message_id}")

        # Extract properties
        message_type = getattr(properties, "headers").get('message-type')
        message_type = getattr(properties, "headers").get('message-type')
        scenario_time = getattr(properties, "headers").get('scenario-time', datetime.now(timezone.utc))
        time_horizon = getattr(properties, "headers").get('time-horizon')
        run_id = getattr(properties, "headers").get('run-id')
        version = getattr(properties, "headers").get('version')

        # Run card builder
        instance_id = f"{time_horizon}_{run_id}_{version}_{message_id})"
        card_factory = builders.CardFactory()
        card = card_factory.build(
            card_type=message_type.lower(),
            card_fields={
                "startDate": scenario_time,
                "processInstanceId": instance_id,
            },
            data=message,
        )

        # Publish to OperatorFabric
        card_json = card.model_dump(exclude_none=True)
        response = self.opfab.post_card(card_json=card_json)

        logger.success(f"Message handling completed successfully")

        return message, properties


if __name__ == '__main__':
    # Define RMQ test message
    headers = {
        # "message-id": f"{uuid.uuid4()}",
        "message-id": f"static-uuid",
        "message-type": "SAR",
        "project-name": "RMM_X",
        "run-id": "00",
        "source-module": "CROSA",
        "scenario-time": "2026-03-30T09:30:00+00:00",
        "time-horizon": "1D",
        "version": "1",
    }
    properties = BasicProperties(
        content_type='application/octet-stream',
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=1747208205,
        headers=headers,
    )
    with open(Path(__file__).parent.parent.joinpath("tests/data/nc_sar.xml"), "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = RootPublicationHandler()
    message, properties = service.handle(message=file_bytes, properties=properties)

