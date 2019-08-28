#!/usr/bin/env bash

gunicorn --workers 3 --bind 0.0.0.0:8000 -m 000 wsgi
