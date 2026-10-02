# Card retriever

Stores OpFab cards in Elasticsearch so they can be searched and reported on
outside OpFab.

- **Entry point:** `card_retriever/worker.py`
- **Input:** RabbitMQ queue `opcoord.cards.retrieve.elastic-storage` (setting `RMQ_QUEUE_IN`)
- **Output:** one Elasticsearch document per card in `RETRIEVER_CARDS_INDEX`
- **Talks to:** RabbitMQ, Elasticsearch

## Processing

[`RootRetrievingHandler.handle`](../reference/card_retriever/handlers.md#card_retriever.handlers.RootRetrievingHandler.handle)
parses the message body as card JSON and indexes it with the card's `cardId`
as the document id. A card that is sent again therefore overwrites its earlier
document instead of creating a duplicate.
