#!/usr/bin/env bash

# Start Automated ETL
cron
env > /etc/environment
# Start webserver
gunicorn --workers 3 --bind 0.0.0.0:8000 -m 000 bart_ridership.app.wsgi
