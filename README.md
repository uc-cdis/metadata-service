# Metadata Service

[![Docker release](https://img.shields.io/github/v/release/uc-cdis/metadata-service?logo=docker&logoColor=white)](https://quay.io/repository/cdis/metadata-service)
[![GitHub workflow](https://img.shields.io/github/workflow/status/uc-cdis/metadata-service/CI%20Workflow?logo=github)](https://github.com/uc-cdis/metadata-service/actions?query=workflow%3A%22CI+Workflow%22)
[![Coverage Status](https://coveralls.io/repos/github/uc-cdis/metadata-service/badge.svg?branch=master)](https://coveralls.io/github/uc-cdis/metadata-service?branch=master)
[![Dependabot Badge](https://img.shields.io/badge/Dependabot-active-brightgreen?logo=dependabot)](https://dependabot.com/)
[![License](https://img.shields.io/github/license/uc-cdis/metadata-service?logo=apache)](https://github.com/uc-cdis/metadata-service/blob/master/LICENSE)

The Metadata Service provides an API for retrieving JSON metadata of GUIDs. It is a flexible option for "semi-structured" data (key:value mappings).

The GUID (the key) can be any string that is unique within the instance. The value is the metadata associated with the GUID, it’s a JSON blob whose structure is not enforced on the server side.

The server is built with [FastAPI](https://fastapi.tiangolo.com/) and packaged with
[Poetry](https://poetry.eustace.io/).

## Key documentation

The documentation can be browsed in the [docs](docs) folder, and key documents are linked below.

* [Detailed API Documentation](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/metadata-service/master/docs/openapi.yaml)
* [Development and deployment](docs/dev.md)
* [Aggregate Metadata Service](docs/agg_mds.md)
