import json
from integrations import elastic
from loguru import logger
import settings


conf = settings.get_settings()


class RootRetrievingHandler:

    def __init__(self, debug: bool = conf.retriever.debug):

        self.debug = debug

        # Services initialization
        try:
            self.elastic = elastic.Elastic()
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch service: {e}")

    def handle(self, message: bytes, properties: object, **kwargs):
        """
        Process forward all cards to Elasticsearch.
        """
        # Convert light card to json
        card_json = json.loads(message)

        # Publish to Elasticsearch
        response = self.elastic.send_to_elastic(index=conf.retriever.cards_index,
                                                json_message=card_json,
                                                id=card_json.get("cardId"))

        logger.success(f"Message handling completed successfully")

        return message, properties


if __name__ == '__main__':
    pass

