# Card publicator

Consumes NC messages from RabbitMQ, builds an OpFab card for each one, and
publishes it.

- **Entry point:** `card_publicator/worker.py`
- **Input:** RabbitMQ queue `opcoord.cards.publish` (setting `RMQ_QUEUE_IN`)
- **Output:** a card posted to OpFab, plus a JSON copy in MinIO when S3 storage is enabled
- **Talks to:** RabbitMQ, Elasticsearch, OperatorFabric, MinIO

## Message contract

The message body is the NC RDF/XML document. These AMQP headers are read:

| Header | Required | Used for |
|---|---|---|
| `message-type` | yes | Profile selection: `SAR` or `RAS` (case-insensitive) |
| `scenario-time` | yes | Card `startDate` (SAR) |
| `message-id` | no | Part of `processInstanceId`; generated when missing |
| `time-horizon` | no | `processInstanceId` and the feed `cycle` parameter |
| `run-id` | no | `processInstanceId` and the feed `cycle` parameter |
| `version` | no | `processInstanceId` |

`processInstanceId` is `{time-horizon}_{run-id}_{version}_{message-id}`. When
`time-horizon` or `run-id` are missing, the `cycle` parameter falls back to
`FullModel.wasGeneratedBy` (for example `CSA-1D-00-RAS`).

## Processing steps

[`RootPublicationHandler.handle`](../reference/card_publicator/handlers.md#card_publicator.handlers.RootPublicationHandler.handle)
runs these steps:

1. **Build.** `CardFactory` picks the SAR or RAS builder. The builder converts the
   RDF/XML to JSON and fills the static fields from `cards.yaml`.
2. **Enrich.** `CardDataEnricher` detects the profile from `FullModel`, loads
   the reference documents it needs from Elasticsearch into temporary caches,
   and adds names and operators to each result or schedule:

    | Index (default) | Adds |
    |---|---|
    | `config-areas` | Area and party names from EIC codes |
    | `csa-contingencies*` | `ContingencyName`, `ContingencyType`, `ContingencyOperatorEIC`/`Name` |
    | `csa-remedial-actions*` | `RemedialActionName`, `RemedialActionKind`, `RemedialActionOperatorEIC`/`Name` |
    | `csa-branch-results-*` | RAS only: `violations`, the loading violations for the schedule's contingency |

    With `PUBLICATOR_ENRICHMENT_STRICT=true`, a missing reference field raises
    and the message is rejected. Otherwise the field is left out, a warning
    is logged, and the card is still published.

3. **Feed parameters.** `title.parameters` and `summary.parameters` get `cycle`
   (for example `1D-00`) and `period` (for example `03 Oct 00:00 UTC – 04 Oct 00:00 UTC`).
4. **Routing (RAS only).** Recipients, `entitiesAllowedToRespond` and
   `entitiesRequiredToRespond` are set to the remedial action's operating TSO.
5. **Publish.** The card is posted to OpFab, then archived as
   `opcoord/cards/<card id>.json` in the `PUBLICATOR_S3_BUCKET_NAME` bucket.

## Local test harness

Running `card_publicator/handlers.py` directly starts a local harness (the
`if __name__ == "__main__"` block). It is not used by the deployed worker.

- `TEST_MODE = "BUILD"` builds a card from a local NC XML file, faking the
  RabbitMQ headers from the file's `FullModel`, and can save the result as JSON.
- `TEST_MODE = "PREBUILT"` publishes an already-built card JSON.
- `TEST_PUBLISH = False` builds without calling OpFab.

`card_publicator/enrichment.py` can also be run on its own to enrich an NC XML
file without publishing.
