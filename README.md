# DCFS Metadata Service

[![License Badge](https://img.shields.io/github/license/uc-cdis/metadata-service?logo=apache)](https://github.com/uc-cdis/metadata-service/blob/master/LICENSE)
[![GitHub Action Badge](https://img.shields.io/github/workflow/status/uc-cdis/metadata-service/pytest?logo=github)](https://github.com/uc-cdis/metadata-service/actions?query=workflow%3Apytest)
[![GitHub release Bacge](https://img.shields.io/github/v/release/uc-cdis/metadata-service?logo=docker&logoColor=white)](https://github.com/uc-cdis/metadata-service/packages/79876)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/34e8e8c4502444afac0f48a7d2a592ea)](https://www.codacy.com/manual/fantix/metadata-service?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=uc-cdis/metadata-service&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/34e8e8c4502444afac0f48a7d2a592ea)](https://www.codacy.com/manual/fantix/metadata-service?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=uc-cdis/metadata-service&amp;utm_campaign=Badge_Coverage)
[![Dependabot Badge](https://badgen.net/dependabot/uc-cdis/metadata-service/?icon=dependabot)](https://dependabot.com/)

The Metadata Service provides API for retrieving JSON metadata of GUIDs.

The server is built with [FastAPI](https://fastapi.tiangolo.com/) and packaged with
[Poetry](https://poetry.eustace.io/).

## Installation

Install required software:

*   [PostgreSQL](PostgreSQL) 9.6 or above
*   [Python](https://www.python.org/downloads/) 3.7 or above
*   [Poetry](https://poetry.eustace.io/docs/#installation)

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
uvicorn mds.app:app --reload
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
docker-compose exec mds poetry run alembic upgrade head
```

Create test database:

```bash
docker-compose exec db createdb -U mds test_mds
```

Run tests:

```bash
docker-compose exec mds poetry run pytest --cov=src --cov=migrations/versions tests
```

## Deployment

For production, use [gunicorn](https://gunicorn.org/):

```bash
gunicorn mds.app:app -k uvicorn.workers.UvicornWorker
```

Or use the Docker image built from the `Dockerfile`, using environment variables
with the same name to configure the server.

Other than database configuration, please also set:

```bash
DEBUG=0
ADMIN_LOGINS=alice:123,bob:456
```

Except that, don't use `123` or `456` as the password.
