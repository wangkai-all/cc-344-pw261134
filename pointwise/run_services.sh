#!/usr/bin/env bash
# Start all four independent services in the background with fresh databases.
# For the full automated scenario use: python3 -I demo.py [--keep]
set -euo pipefail
cd "$(dirname "$0")"

DATA_DIR="${DATA_DIR:-data}"
LOG_DIR="${LOG_DIR:-logs}"
mkdir -p "$DATA_DIR" "$LOG_DIR"

python3 -I source.py    --db "$DATA_DIR/source.db"    --seed "${SEED:-400}" >"$LOG_DIR/source.log" 2>&1 &
python3 -I scheduler.py --db "$DATA_DIR/scheduler.db"                      >"$LOG_DIR/scheduler.log" 2>&1 &
python3 -I receiver.py  --db "$DATA_DIR/receiver.db"                       >"$LOG_DIR/receiver.log" 2>&1 &
python3 -I web.py       --db "$DATA_DIR/web.db"                            >"$LOG_DIR/web.log" 2>&1 &

echo "services starting... open http://127.0.0.1:8080"
for port in 8091 8092 8093 8080; do
  for _ in $(seq 1 30); do
    curl -sf -o /dev/null "http://127.0.0.1:$port/health" && break
    sleep 0.3
  done
  echo "  port $port ready"
done
echo "done. tail logs in $LOG_DIR/ ; stop with: pkill -f 'source.py|scheduler.py|receiver.py|web.py'"
