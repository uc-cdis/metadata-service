# Aggregate Metadata Service

The Metadata Service can be configured to aggregate metadata from multiple other Metadata Service instances.

## Aggregation APIs

The aggregated MDS APIs and scripts copy metadata from one or many metadata services into a single data store. This enables a metadata service to act as a central API for browsing Metadata using clients such as the Ecosystem browser.

The aggregate metadata APIs and migrations are disabled by default unless `USE_AGG_MDS=true` is specified. The `AGG_MDS_NAMESPACE` should also be defined for shared Elasticserach environments so that a unique index is used per-instance.

The aggregate cache is built using Elasticsearch. See the `docker-compose.yaml` file (specifically the `aggregate_migration` service) for details regarding how aggregate data is populated.

# Metadata adapters

See the [Metadata Adapters documentation](metadata_adapters.md).
