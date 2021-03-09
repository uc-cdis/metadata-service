# DCFS Metadata Service

[![Docker release](https://img.shields.io/github/v/release/uc-cdis/metadata-service?logo=docker&logoColor=white)](https://quay.io/repository/cdis/metadata-service)
[![GitHub workflow](https://img.shields.io/github/workflow/status/uc-cdis/metadata-service/CI%20Workflow?logo=github)](https://github.com/uc-cdis/metadata-service/actions?query=workflow%3A%22CI+Workflow%22)
[![Codacy coverage](https://img.shields.io/codacy/coverage/5acc2d70109f451ab8047b2691ffcffe?logo=codacy)](https://www.codacy.com/gh/uc-cdis/metadata-service)
[![Codacy grade](https://img.shields.io/codacy/grade/5acc2d70109f451ab8047b2691ffcffe?logo=codacy)](https://www.codacy.com/gh/uc-cdis/metadata-service)
[![Dependabot Badge](https://img.shields.io/badge/Dependabot-active-brightgreen?logo=dependabot)](https://dependabot.com/)
[![License](https://img.shields.io/github/license/uc-cdis/metadata-service?logo=apache)](https://github.com/uc-cdis/metadata-service/blob/master/LICENSE)

The Metadata Service provides API for retrieving JSON metadata of GUIDs.

The server is built with [FastAPI](https://fastapi.tiangolo.com/) and packaged with
[Poetry](https://poetry.eustace.io/).

[View API Documentation](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/metadata-service/master/docs/openapi.yaml)

## Installation

Install required software:

* [PostgreSQL](PostgreSQL) 9.6 or above
* [Python](https://www.python.org/downloads/) 3.7 or above
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
# DB_HOST = "..."     # default: localhost
# DB_PORT = ...       # default: 5432
# DB_USER = "..."     # default: current user
# DB_PASSWORD = "..." # default: empty
# DB_DATABASE = "..." # default: current user
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

```bash
pytest --cov=src --cov=migrations/versions tests
```

Please note that, the name of the test database is prepended with "test_", you
need to create that database too.

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
