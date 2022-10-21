# Development and deployment

* [Installation](#installation)
* [Development](#development)
* [Run tests](#run-tests)
* [Develop with Docker](#develop-with-docker)
* [Deployment](#deployment)

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
