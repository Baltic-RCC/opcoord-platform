# SAR process

Security Analysis Result (SAR): the outcome of the CSA security analysis for
one scenario hour. It is published to OpFab as an informational card.

## Overview

```mermaid
flowchart LR
    CSA[CSA process] -- "SAR NC message" --> RMQ[(RabbitMQ)]
    RMQ --> PUB[Card publicator]
    PUB --> OPFAB[OpFab card<br/>crosa / sar]
    OPFAB --> RET[Card retriever]
    RET --> ES[(Elasticsearch)]
```

## Steps

1. **Input.** CSA publishes a SAR NC message (one per scenario hour).
2. **Conversion.** The card publicator converts the XML into card data.
3. **Enrichment.** Reference data from Elasticsearch is added to the results.
4. **Publication.** The card is posted to OpFab.
5. **Storage.** The card retriever stores the published card in Elasticsearch.
6. **Reuse.** RAS proposal cards look up SAR cards to show linked violations.
   See [RAS process](ras.md).

## OpFab card

| | |
|---|---|
| Process / state | `crosa` / `sar` |
| Recipients | Group `ADMIN` |
| Response | None (information only) |
| Bundle | `opcoordplatform`: `process_bundles/crosa` |

## Open points

- _TBD_
