#!/bin/bash

nginx
poetry run opentelemetry-instrument \
    gunicorn -c "/mds/deployment/wsgi/gunicorn.conf.py" mds.asgi:app
