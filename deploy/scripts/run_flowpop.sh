#!/bin/bash
set -euo pipefail
echo "$FLOWPOP_YM"
: "${FLOWPOP_YM:?FLOWPOP_YM 환경 변수를 설정해주세요 (예: 202501)}"

cd /app
exec python module/flowpop.py "${FLOWPOP_YM}"

