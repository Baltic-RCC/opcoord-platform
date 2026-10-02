# Configuration

All settings are [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
classes, read from environment variables. An env var name is the class's
prefix plus the field name in upper case, for example `PUBLICATOR_` +
`enrichment_strict` = `PUBLICATOR_ENRICHMENT_STRICT`. A variable without the
right prefix is ignored silently.

For local runs, the connection and business settings are also read from
`config/.env`. Copy `config/.env.example` to start one. Never commit it.

## Connections (`config/integrations.py`)

Shared by all workers. Fields without a default are required.

| Prefix | Variable | Default |
|---|---|---|
| `ELASTIC_` | `ELASTIC_HOST`, `ELASTIC_API_KEY` | required |
| | `ELASTIC_BATCH_SIZE` | `1000` |
| | `ELASTIC_SSL_VERIFY` | `true` |
| `RMQ_` | `RMQ_HOST`, `RMQ_USERNAME`, `RMQ_PASSWORD` | required |
| | `RMQ_PORT` | `5672` |
| | `RMQ_VHOST` | `/` |
| | `RMQ_HEARTBEAT` | `15` |
| `OPFAB_` | `OPFAB_HOST`, `OPFAB_USERNAME`, `OPFAB_PASSWORD` | required |
| | `OPFAB_SSL_VERIFY` | `false` |
| `MINIO_` | `MINIO_HOST`, `MINIO_USERNAME` | required |
| | `MINIO_PASSWORD` (LDAP mode) or `MINIO_API_KEY` (API key mode) | one is required; LDAP wins when both are set |
| | `MINIO_TOKEN_EXPIRATION`, `MINIO_TOKEN_RENEW_MARGIN`, `MINIO_MAXSIZE` | `86400`, `120`, `50` |

## Logging (`config/logging.py`)

| Variable | Default | Meaning |
|---|---|---|
| `LOGS_LEVEL` | `20` (INFO) | Log level as a number |
| `LOGS_ELASTIC_HANDLER` | `true` | Also ship logs to Elasticsearch |
| `LOGS_ELASTIC_INDEX` | `dev-opcoord-logs` | Index for shipped logs |
| `LOGS_FORWARD_STD` | `true` | Include records from standard `logging` loggers |
| `LOGS_EXCLUDE_LOGGERS` | `["pika"]` | Loggers to silence |

## Worker settings

Every worker has a `WorkerSettings` class without a prefix, plus a
`BusinessSettings` class with a worker-specific prefix.

### Card publicator (`PUBLICATOR_`)

| Variable | Default |
|---|---|
| `RMQ_QUEUE_IN` | `opcoord.cards.publish` |
| `PUBLICATOR_CARDS_INDEX` | `dev-opcoord-cards` |
| `PUBLICATOR_ENABLE_S3_CONTENT_STORAGE` | `true` |
| `PUBLICATOR_S3_BUCKET_NAME` | `analyses` |
| `PUBLICATOR_ENRICHMENT_STRICT` | `false` |
| `PUBLICATOR_ENRICHMENT_VERBOSE_LOGGING` | `true` |
| `PUBLICATOR_DEBUG` | `false` |

!!! note
    The card `publisher` is fixed in code as `38X-BALTIC-RSC-H`. It is a
    class constant, so `PUBLICATOR_PUBLISHER` has no effect.

### Card retriever (`RETRIEVER_`)

| Variable | Default |
|---|---|
| `RMQ_QUEUE_IN` | `opcoord.cards.retrieve.elastic-storage` |
| `RETRIEVER_CARDS_INDEX` | `dev-opcoord-cards` |
| `RETRIEVER_DEBUG` | `false` |

### Business data exchange (`EXCHANGE_`)

| Variable | Default |
|---|---|
| `EXCHANGE_CONTINGENCIES_INDEX` | `csa-contingencies*` |
| `EXCHANGE_ASSESSED_ELEMENTS_INDEX` | `csa-assessed-elements*` |
| `EXCHANGE_REMEDIAL_ACTIONS_INDEX` | `csa-remedial-actions*` |
| `EXCHANGE_TARGET_DAY_OFFSET` | `1` (tomorrow) |
| `EXCHANGE_VALIDATED_STATUSES` | `["valid"]` |
| `EXCHANGE_FAILED_ELEMENTS_LISTED` | `100` |
| `EXCHANGE_LATEST_VERSION_ONLY` | `true` |
| `EXCHANGE_EXPECTED_PUBLISHERS` | Elering, Litgrid and AST EICs |
| `EXCHANGE_FALLBACK_ENABLED` | `true` |
| `EXCHANGE_FALLBACK_STALE_AFTER_DAYS` | `7` |
| `EXCHANGE_RETENTION_DAYS` | `2` |
| `EXCHANGE_UPLOAD_EMPTY_DATASETS` | `false` |
| `EXCHANGE_DATASET_FIELD_WHITELIST` | `{}` (keep all fields) |
| `EXCHANGE_QUERY_SIZE` | `10000` |
| `EXCHANGE_DEBUG` | `false` |

List and dict settings are passed as JSON, for example
`EXCHANGE_VALIDATED_STATUSES='["valid"]'`.

## Adding a setting

A new setting only reaches a deployed worker when it is also added to that
worker's deployment configuration (Helm chart ConfigMap or Secret). Mention
new env vars in the pull request so the deployment can be updated.
