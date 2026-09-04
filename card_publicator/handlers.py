import json
import uuid
from io import BytesIO
from pathlib import Path

from pika import BasicProperties

import builders
from integrations import elastic, opfab, s3_storage
from loguru import logger
import settings
from enrichment import CardDataEnricher
from ras_linked_violations import RasLinkedViolationEnricher


conf = settings.get_settings()


class RootPublicationHandler:
    """Coordinate card construction and publication for the card-publicator worker.

    The worker uses :meth:`handle` as its RabbitMQ callback.  The executable
    section at the bottom of this file is a local development/test harness and
    is not used by the production worker.
    """

    def __init__(
        self,
        debug: bool = conf.publicator.debug,
        enrichment_strict: bool = conf.publicator.enrichment_strict,
        enrichment_verbose_logging: bool = conf.publicator.enrichment_verbose_logging,
        card_data_enricher: CardDataEnricher | None = None,
        ras_linked_violation_enricher: RasLinkedViolationEnricher | None = None,
        enable_s3_content_storage: bool | None = None,
    ):

        self.debug = debug
        self.enrichment_strict = enrichment_strict
        self.enrichment_verbose_logging = enrichment_verbose_logging
        self.s3 = None
        self.opfab = None
        self.card_data_enricher = card_data_enricher
        self.ras_linked_violation_enricher = ras_linked_violation_enricher
        self.enable_s3_content_storage = (
            conf.publicator.enable_s3_content_storage
            if enable_s3_content_storage is None
            else enable_s3_content_storage
        )

        # Production worker dependencies: card enrichment, OperatorFabric, and
        # optional S3 storage are initialized when the worker starts.
        if self.card_data_enricher is None:
            try:
                self.elastic = elastic.Elastic()
                self.card_data_enricher = CardDataEnricher(
                    elastic=self.elastic,
                    enrichment_strict=self.enrichment_strict,
                    enrichment_verbose_logging=self.enrichment_verbose_logging,
                )
            except Exception as e:
                logger.error(f"Failed to initialize Elasticsearch service: {e}")

        try:
            self.opfab = opfab.AuthenticatedSession()
        except Exception as e:
            logger.error(f"Failed to initialize OperatorFabric service: {e}")

        if self.ras_linked_violation_enricher is None and self.opfab is not None:
            sar_config = builders.config["sar"]
            self.ras_linked_violation_enricher = RasLinkedViolationEnricher(
                self.opfab,
                sar_process=sar_config["process"],
                sar_state=sar_config["state"],
            )

        if self.enable_s3_content_storage:
            try:
                self.s3 = s3_storage.S3Minio()
            except Exception as e:
                logger.error(f"Failed to initialize S3Minio service: {e}")

    def build_card(self, message: bytes, properties: object):
        """Build and enrich an OperatorFabric card from an incoming NC message.

        This is the first production-worker step after RabbitMQ delivery.
        """
        # Read the message metadata supplied by RabbitMQ (or by local BUILD
        # mode) and create an id when the incoming message has none.
        headers = getattr(properties, "headers", {}) or {}
        message_id = headers.get("message-id", str(uuid.uuid4()))
        logger.info(f"Handling message with id: {message_id}")

        # Extract metadata required to select the card type and identify the
        # resulting process instance.
        message_type = headers.get("message-type")
        if not message_type:
            raise ValueError("Incoming message is missing the 'message-type' header")

        scenario_time = headers.get("scenario-time")
        if not scenario_time:
            raise ValueError("Incoming message is missing the 'scenario-time' header")
        time_horizon = headers.get("time-horizon")
        run_id = headers.get("run-id")
        version = headers.get("version")

        # Convert the incoming NC payload into the card type selected above.
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

        # Add enrichment data before the card is handed to the publisher.
        if self.card_data_enricher is None:
            raise RuntimeError("Card enrichment service is not available")
        self.card_data_enricher.enrich_in_place(payload=card.data, card_fields=card_fields)

        # A RAS references the contingency whose violated elements were already
        # published in an earlier SAR card. Link those results before the final
        # one-RA card recipients are derived.
        if message_type.lower() == "ras" and self.ras_linked_violation_enricher:
            self.ras_linked_violation_enricher.enrich_in_place(
                card.data,
                sar_card_id=headers.get("sar-card-id"),
            )

        # RAS recipients depend on the operator resolved during enrichment.
        # Post-enrichment routing also enforces the one-schedule-per-card contract.
        card_factory.apply_post_enrichment_fields(message_type.lower(), card)

        return card

    def publish_card(self, card_json: dict):
        """Publish a prepared card and optionally archive it in S3.

        This is the production-worker publication step.  It accepts the
        serialized result of :meth:`build_card`, posts it to OperatorFabric,
        and stores the same JSON in S3 when content storage is enabled.
        """
        if self.opfab is None:
            raise RuntimeError("OperatorFabric service is not available")
        response = self.opfab.post_card(card_json=card_json)
        logger.info(f"Card publication details: {response.json()}")

        if self.enable_s3_content_storage and self.s3 is not None:
            s3_path = f"opcoord/cards/{response.json().get('id', uuid.uuid4().__str__())}.json"
            json_bytes = json.dumps(card_json, indent=2).encode("utf-8")
            json_buffer = BytesIO(json_bytes)
            json_buffer.name = s3_path
            self.s3.upload_object(
                file_path_or_file_object=json_buffer,
                bucket_name=conf.publicator.s3_bucket_name,
            )
            logger.info(f"Uploaded JSON card to S3 at path: {s3_path}")

        logger.success("Message handling completed successfully")
        return response

    def handle(self, message: bytes, properties: object, **kwargs):
        """Handle one RabbitMQ message in the production worker.

        The callback builds, enriches, and publishes the card, then returns the
        original message and properties expected by the consumer.
        """
        card = self.build_card(message=message, properties=properties)
        self.publish_card(card.model_dump(exclude_none=True))

        return message, properties

    # Local development helper; the production worker uses handle() instead.
    def send_prebuilt_card(self, card_json: dict):
        """Publish an already-built JSON card from the local test harness."""
        logger.info(
            f"Sending prebuilt card: process={card_json.get('process')} "
            f"state={card_json.get('state')} "
            f"instance={card_json.get('processInstanceId')}"
        )
        return self.publish_card(card_json)


if __name__ == "__main__":
    # Local development/test harness only. 
    TEST_MODE = "BUILD"  # BUILD or PREBUILT
    TEST_PROFILE = "RAS"  # SAR or RAS, used in BUILD mode
    TEST_PUBLISH = True  # Set False to build/save locally without calling OperatorFabric
    TEST_SAR_CARD_ID = None  # Optional: "crosa.<processInstanceId>"

    project_root = Path(__file__).parent.parent
    NC_INPUT_PATHS = {
        "SAR": project_root / "tests" / "kevin" / "xml" / "SAR_EXCO_test.xml",
        # "RAS": project_root / "tests" / "kevin" / "xml" / "ras1D_test.xml",
        "RAS": project_root / "tests" / "kevin" / "xml" / "RAS_EXCO.xml",

    }
    prebuilt_card_path = (
        project_root / "tests" / "kevin" / "payload" / "ras_built.json"
    )

    save_built_card = True
    built_card_output_dir = project_root / "tests" / "kevin" / "payload"
    built_card_output_name = f"{TEST_PROFILE.lower()}_built.json"

    service = RootPublicationHandler(
        enable_s3_content_storage=False,
    )

    if TEST_MODE == "BUILD":
        try:
            nc_input_path = NC_INPUT_PATHS[TEST_PROFILE.upper()]
        except KeyError as error:
            raise ValueError(f"Unsupported TEST_PROFILE: {TEST_PROFILE}") from error

        headers = {
            "message-id": str(uuid.uuid4()),
            "message-type": TEST_PROFILE,
            "project-name": "RMM_X",
            "run-id": "00",
            "source-module": "CROSA",
            # Local RabbitMQ metadata used for the top-level card date. Linked
            # SAR lookup derives hourly midpoints from the RAS XML interval.
            "scenario-time": "2026-08-13T01:30:00+00:00",
            "time-horizon": "1D",
            "version": "1",
        }
        if TEST_SAR_CARD_ID:
            headers["sar-card-id"] = TEST_SAR_CARD_ID
        properties = BasicProperties(
            content_type="application/octet-stream",
            delivery_mode=2,
            priority=4,
            message_id=str(uuid.uuid4()),
            timestamp=1747208205,
            headers=headers,
        )
        with nc_input_path.open("rb") as file:
            file_bytes = file.read()

        card = service.build_card(
            message=file_bytes,
            properties=properties,
        )
        card_json = card.model_dump(exclude_none=True)

        if save_built_card:
            built_card_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = built_card_output_dir / built_card_output_name
            output_path.write_text(
                json.dumps(card_json, ensure_ascii=False, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
            logger.info(f"Saved built card to {output_path}")

        if TEST_PUBLISH:
            service.publish_card(card_json)
    elif TEST_MODE == "PREBUILT":
        with prebuilt_card_path.open(encoding="utf-8") as file:
            card_json = json.load(file)
        if TEST_PUBLISH:
            service.send_prebuilt_card(card_json)
    else:
        raise ValueError(f"Unsupported TEST_MODE: {TEST_MODE}")
