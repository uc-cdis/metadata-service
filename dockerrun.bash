#!/bin/bash

nginx
gunicorn -c "/src/deployment/wsgi/gunicorn.conf.py"
