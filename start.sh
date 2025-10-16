#!/usr/bin/env bash
# start.sh - portable start script for Uvicorn
PORT=${PORT:-8000}
exec uvicorn app.main:app --host 0.0.0.0 --port $PORT --proxy-headers
