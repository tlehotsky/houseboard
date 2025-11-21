#!/usr/bin/env bash
set -e

# Log environment so we can compare systemd vs interactive if needed
env | sort > /tmp/houseboard-gunicorn-env.txt

exec /home/tlehotsky/.local/bin/gunicorn core.wsgi:application \
  --bind 0.0.0.0:8821 \
  --workers 3
