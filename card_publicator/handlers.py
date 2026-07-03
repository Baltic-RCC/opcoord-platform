import os
import json
from io import BytesIO
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pika import BasicProperties
import builders
from integrations import elastic, opfab, s3_storage
from loguru import logger
import settings
from enrichment import CardDataEnricher


conf = settings.get_settings()


class RootPublicationHandler:

    def __init__(self, debug: bool = conf.publicator.debug):

        self.debug = debug

        # Services initialization
        try:
            self.elastic = elastic.Elastic()
            self.card_data_enricher = CardDataEnricher(elastic=self.elastic, debug=False)
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch service: {e}")

        try:
            self.opfab = opfab.AuthenticatedSession()
        except Exception as e:
            logger.error(f"Failed to initialize OperatorFabric service: {e}")

        try:
            self.s3 = s3_storage.S3Minio()
        except Exception as e:
            logger.error(f"Failed to initialize S3Minio service: {e}")

    def handle(self, message: bytes, properties: object, **kwargs):
        """
        Process received message and return data for publication.
        """
        # Get unique message-id from headers, if not there - create
        message_id = properties.headers.get('message-id', str(uuid.uuid4()))
        logger.info(f"Handling message with id: {message_id}")

        # Extract properties
        message_type = getattr(properties, "headers").get('message-type')
        scenario_time = getattr(properties, "headers").get('scenario-time', datetime.now(timezone.utc))
        time_horizon = getattr(properties, "headers").get('time-horizon')
        run_id = getattr(properties, "headers").get('run-id')
        version = getattr(properties, "headers").get('version')

        # Run card builder
        instance_id = f"{time_horizon}_{run_id}_{version}_{message_id}"
        card_fields = {
            "publisher": conf.publicator.publisher,
            "startDate": scenario_time,
            "processInstanceId": instance_id,
        }
        card_factory = builders.CardFactory()
        card = card_factory.build(
            card_type=message_type.lower(),
            card_fields=card_fields,
            data=message,
        )

        # Enrich the converted card
        self.card_data_enricher.enrich_in_place(payload=card.data, card_fields=card_fields)

        # Publish to OperatorFabric
        card_json = card.model_dump(exclude_none=True)
        response = self.opfab.post_card(card_json=card_json)
        logger.info(f"Card publication details: {response.json()}")

        # Upload JSON card to Object Storage (S3) for long term storage
        if conf.publicator.enable_s3_content_storage:
            s3_path = f"opcoord/cards/{response.json().get('id', uuid.uuid4().__str__())}.json"
            json_bytes = json.dumps(card_json, indent=2).encode("utf-8")
            json_buffer = BytesIO(json_bytes)
            json_buffer.name = s3_path  # required — upload_object uses file_object.name as object_name
            self.s3.upload_object(
                file_path_or_file_object=json_buffer,
                bucket_name=conf.publicator.s3_bucket_name,
            )
            logger.info(f"Uploaded JSON card to S3 at path: {s3_path}")
            card_json["contentReference"] = s3_path

        logger.success(f"Message handling completed successfully")

        return message, properties


if __name__ == '__main__':
    # Define RMQ test message
    headers = {
        # "message-id": f"{uuid.uuid4()}",
        "message-id": f"static-uuid",
        "message-type": "RAS",
        "project-name": "RMM_X",
        "run-id": "00",
        "source-module": "CROSA",
        "scenario-time": "2026-07-02T09:30:00+00:00",
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
    with open(Path(__file__).parent.parent.joinpath("tests/data/nc_ras.xml"), "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = RootPublicationHandler()
    message, properties = service.handle(message=file_bytes, properties=properties)

