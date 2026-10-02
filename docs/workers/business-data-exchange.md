# Business data exchange

Exports the CSA input data for the next business day from Elasticsearch into
OpFab **business data**, so card templates can show it. It runs once and
exits, and is meant to be scheduled once a day, after the CSA consistency
check and before the day-ahead (1D) CROSA run.

- **Entry point:** `business_data_exchange/worker.py`
- **Input:** Elasticsearch indices `csa-contingencies*`, `csa-assessed-elements*`, `csa-remedial-actions*`
- **Output:** one OpFab business data resource per dataset, named `<dataset>_<YYYY-MM-DD>`
- **Talks to:** Elasticsearch, OperatorFabric
- **Exit code:** `1` when the run fails, so the scheduler marks it failed

## Processing steps

[`BusinessDataExchangeHandler.handle`](../reference/business-data-exchange.md#business_data_exchange.handlers.BusinessDataExchangeHandler.handle)
runs these steps:

1. **Query.** For each dataset, fetch the documents whose `FullModel` validity
   period overlaps the target business day (the UTC day `EXCHANGE_TARGET_DAY_OFFSET`
   days after today, tomorrow by default).
   With `EXCHANGE_LATEST_VERSION_ONLY`, only the highest version per publisher
   is kept, because the consistency check re-publishes each profile under a
   higher version.
2. **Fallback.** For each TSO in `EXCHANGE_EXPECTED_PUBLISHERS` that sent nothing
   for that day, reuse its most recent earlier submission. Submissions older
   than `EXCHANGE_FALLBACK_STALE_AFTER_DAYS` are still used but flagged as stale.
3. **Retention.** If any data was found, delete business data resources older
   than `EXCHANGE_RETENTION_DAYS`. If every dataset is empty, the run stops here
   and nothing is deleted.
4. **Pre-process.** Wrap each dataset in an envelope with metadata: record
   count, consistency status counts, failed elements (the list is capped at
   `EXCHANGE_FAILED_ELEMENTS_LISTED`, the count is exact), expected, received
   and missing submitters, and the fallbacks used. `EXCHANGE_DATASET_FIELD_WHITELIST`
   can limit which fields each record keeps.
5. **Validate.** A dataset is *validated* only when every record carries a
   consistency verdict and every verdict is in `EXCHANGE_VALIDATED_STATUSES`
   (default `valid`). `missing` means a referenced grid element was not found
   in the network model, so it does not count as passing.
6. **Upload.** Post each envelope to the OpFab `businessconfig/businessdata`
   API. Empty datasets are skipped unless `EXCHANGE_UPLOAD_EMPTY_DATASETS=true`.

## Where the consistency verdict sits

The consistency check writes `ConsistencyStatus` at a different depth for each
NC profile. `EXCHANGE_CONSISTENCY_STATUS_PATHS` holds the paths:

| Dataset | Path |
|---|---|
| `csa-contingencies` | `ContingencyEquipment.ConsistencyStatus` |
| `csa-assessed-elements` | `ConsistencyStatus` (document root) |
| `csa-remedial-actions` | `TopologyAction.ConsistencyStatus`, `RotatingMachineAction.ConsistencyStatus`, `ShuntCompensatorModification.ConsistencyStatus` |
