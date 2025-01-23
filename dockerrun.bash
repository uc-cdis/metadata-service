#!/bin/bash

nginx
poetry run gunicorn -c "/mds/deployment/wsgi/gunicorn.conf.py" mds.asgi:app
