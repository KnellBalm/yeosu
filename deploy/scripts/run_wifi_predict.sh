#!/bin/bash
set -euo pipefail

cd /app
exec python module/wifi_predict.py

