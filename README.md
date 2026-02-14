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

## Creating appropriate resources/roles/policies in the user.yaml

The MDS and all MDS records are public (requiring no login to read) by design to ensure FAIR data management. Therefore, the user.yaml does not need to provide any read access to the MDS for any users.  

However, you must create resource/role/policy to permit admin users to create/read/update/delete (CRUD) records in the MDS. Here is what you should add to your user.yaml to permit creation of your MDS.  

```
  resources:
    - name: 'mds_gateway'
      description: 'commons /mds-admin'

  roles:
    - id: 'mds_crud'
      permissions:
        - id: 'mds_access'
          action:
            service: 'mds_gateway'
            method: 'access'
  policies:
    - id: 'mds_admin'
      description: 'be able to CRUD records in metadata service'
      resource_paths: ['/mds_gateway']
      role_ids: ['mds_crud']
```
