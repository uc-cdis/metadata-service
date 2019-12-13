# DCFS Metadata Service

The Metadata Service provides API for retrieving JSON metadata of GUIDs.

The server is built with [FastAPI](https://fastapi.tiangolo.com/) and packaged with
[Poetry](https://poetry.eustace.io/).


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

```
# DB_HOST = "..."     # default: localhost
# DB_PORT = ...       # default: 5432
# DB_USER = ...       # default: current user
# DB_PASSWORD = ...   # default: empty
# DB_DATABASE = ...   # default: current user
```

Run the server with auto-reloading:

```bash
uvicorn mds.app:app --reload
```

Try out the API at: http://localhost:8000/docs.

To run tests:

```bash
pytest --cov=mds tests
```
