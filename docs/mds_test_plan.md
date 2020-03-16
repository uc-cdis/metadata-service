# Metadata Service (mds) test plan

### One-liner overview of the service
The MDS is a blobstore used to store JSON-formatted data, any schemaless data.

### RESTful API
Here's one of its swagger pages:
https://nci-crdc-staging.datacommons.io/mds/docs

### Authentication

Users can interact with MDS through basic auth, e.g.:
```
% curl --location --request POST 'https://${GEN3_HOSTNAME}/mds/metadata/someGUID2' \
--header 'Content-Type: application/json' \
--header "Authorization: Basic ${BASIC_AUTH_MDS}" \
--data-raw '{
        "bar": 789
}'
{"bar":789}
```
Or with a Gen3 Access Token, e.g.:
```
% curl -X POST 'https://${GEN3_HOSTNAME}/mds-admin/metadata/foo' \
--header 'Content-Type: application/json' \
--header "Authorization: Bearer ${ACCESS_TOKEN}" \
--data-raw '{ "foo": 123 }'
{"foo":123}
```

### Tests

The initial coverage comprises the following scenarios:

1. Create records against the metadata svc blobstore - positive test
2. Get existing keys from metadata records - positive test
3. Query metadata for matching records - positive test

### Load Tests

#### Performance benchmarking
The service is currently being load tested with the following criteria:

#### Soak test
This test involves a continuous 4hs run, constantly creating random entries and running queries with 200 virtual users.

