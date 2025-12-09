#!/bin/bash
set -euo pipefail

: "${LOCALECONOMY_TARGET:?LOCALECONOMY_TARGET 환경 변수를 설정해주세요 (kcb 또는 local)}"

cd /app
exec python module/localeco.py "${LOCALECONOMY_TARGET}"

