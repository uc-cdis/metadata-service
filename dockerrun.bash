#!/bin/bash

nginx
exec gunicorn -c "/mds/deployment/wsgi/gunicorn.conf.py" mds.asgi:app
