#!/bin/bash

nginx
poetry run opentelemetry-instrument \
    --traces_exporter otlp
    gunicorn -c "/mds/deployment/wsgi/gunicorn.conf.py" mds.asgi:app
