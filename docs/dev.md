# Development and deployment

* [Installation](#installation)
* [Development](#development)
* [Run tests](#run-tests)
* [Develop with Docker](#develop-with-docker)
* [Work with Aggregate MDS](#work-with-aggregate-mds)
* [Deployment](#deployment)
* [Helm](#Quickstart with Helm)

## Installation

Install required software:

* [PostgreSQL](PostgreSQL) 9.6 or above
* [Python](https://www.python.org/downloads/) 3.9 or above
* [Poetry](https://poetry.eustace.io/docs/#installation)

Then use `poetry install` to install the dependencies. Before that,
a [virtualenv](https://virtualenv.pypa.io/) is recommended.
If you don't manage your own, Poetry will create one for you
during `poetry install`, and you must activate it by:

```bash
poetry shell
```

## Development

Create a file `.env` in the root directory of the checkout:
(uncomment to override the default)

```python
# USE_AGG_MDS = True                  # default: False
# DB_HOST = "..."                     # default: localhost
# DB_PORT = ...                       # default: 5432
# DB_USER = "..."                     # default: current user
# DB_PASSWORD = "..."                 # default: empty
# DB_DATABASE = "..."                 # default: current user
# AGG_MDS_NAMESPACE = "..."           # default: default_namespace
# GEN3_ES_ENDPOINT = "..."            # default: empty
# INDEXING_SERVICE_ENDPOINT = "..."   # default: http://indexd-service
# DATA_ACCESS_SERVICE_ENDPOINT= "..." # default: http://fence-service
```

Run database schema migration:

```bash
alembic upgrade head
```

Run the server with auto-reloading:

```bash
python run.py
```

Try out the API at: <http://localhost:8000/docs>.

## Run tests

Please note that the name of the test database is prepended with "test_", you
need to create that database first:

```bash
psql
CREATE DATABASE test_metadata;
```

```bash
pytest --cov=src --cov=migrations/versions tests
```

## Develop with Docker

Use Docker compose:

```bash
docker-compose up
```

Run database schema migration as well:

```bash
docker-compose exec app alembic upgrade head
```

Run tests:

```bash
docker-compose exec app pytest --cov=src --cov=migrations/versions tests
```

## Work with Aggregate MDS
testing populate:
```bash
python src/mds/populate.py --config <config file> --hostname localhost --port 9200
```
view the loaded data
```bash
http://localhost:8000/aggregate/metadata?limit=1000
```

## Deployment

For production, use [gunicorn](https://gunicorn.org/):

```bash
gunicorn mds.asgi:app -k uvicorn.workers.UvicornWorker -c gunicorn.conf.py
```

Or use the Docker image built from the `Dockerfile`, using environment variables
with the same name to configure the server.

Other than database configuration, please also set:

```bash
DEBUG=0
ADMIN_LOGINS=alice:123,bob:456
```

Except that, don't use `123` or `456` as the password.

## Quickstart with Helm

You can now deploy individual services via Helm!

If you are looking to deploy all Gen3 services, that can be done via the Gen3 Helm chart.
Instructions for deploying all Gen3 services with Helm can be found [here](https://github.com/uc-cdis/gen3-helm#readme).

To deploy the metadata service:
```bash
helm repo add gen3 https://helm.gen3.org
helm repo update
helm upgrade --install gen3/metadata
```
These commands will add the Gen3 helm chart repo and install the metadata service to your Kubernetes cluster.

Deploying metadata this way will use the defaults that are defined in this [values.yaml file](https://github.com/uc-cdis/gen3-helm/blob/master/helm/metadata/values.yaml)

You can learn more about these values by accessing the metadata [README.md](https://github.com/uc-cdis/gen3-helm/blob/master/helm/metadata/README.md)

If you would like to override any of the default values, simply copy the above values.yaml file into a local file and make any changes needed.

To deploy the service independant of other services (for testing purposes), you can set the .postgres.separate value to "true". This will deploy the service with its own instance of Postgres:
```bash
  postgres:
    separate: true
```

You can then supply your new values file with the following command:
```bash
helm upgrade --install gen3/metadata -f values.yaml
```

If you are using Docker Build to create new images for testing, you can deploy them via Helm by replacing the .image.repository value with the name of your local image.
You will also want to set the .image.pullPolicy to "never" so kubernetes will look locally for your image.
Here is an example:
```bash
image:
  repository: <image name from docker image ls>
  pullPolicy: Never
  # Overrides the image tag whose default is the chart appVersion.
  tag: ""
```

Re-run the following command to update your helm deployment to use the new image:
```bash
helm upgrade --install gen3/metadata
```

You can also store your images in a local registry. Kind and Minikube are popular for their local registries:
- https://kind.sigs.k8s.io/docs/user/local-registry/
- https://minikube.sigs.k8s.io/docs/handbook/registry/#enabling-insecure-registries


## Additional Notes

When using the Metadata Service as a backend to retrieve results for the Discovery Page, query response times can increase if the database contains a large number of records. To improve performance in such cases, one recommended approach is to manually add an index on the `data->>_guid_type` field in the PostgreSQL database.

```SQL
create index metadata_guid_type on public.metadata((data->>'_guid_type'));
```
