#!/usr/bin/with-contenv bashio
set -euo pipefail

STATE_DIR="/data/ClipHouse"
CONFIG_PATH="/data/config.ini"
mkdir -p "$STATE_DIR"

MAX_TEXT_CHARS="$(bashio::config 'max_text_chars')"
if ! [[ "$MAX_TEXT_CHARS" =~ ^[0-9]+$ ]]; then
  MAX_TEXT_CHARS=100000
fi
if (( MAX_TEXT_CHARS < 1 || MAX_TEXT_CHARS > 1000000 )); then
  MAX_TEXT_CHARS=100000
fi

# Home Assistant OS has no interactive desktop clipboard. The Windows clients
# keep capturing their own clipboard; this always-on node hosts the web page,
# stores history persistently, and syncs with the other ClipHouse peers.
cat > "$CONFIG_PATH" <<EOF_CONFIG
[server]
http_port = 8787
discovery_port = 8788

[storage]
history_dir = /data/ClipHouse

[clipboard]
auto_capture = false
max_text_chars = ${MAX_TEXT_CHARS}

[web]
local_name = clipboard
EOF_CONFIG

export CLIPHOUSE_CONFIG_PATH="$CONFIG_PATH"
export CLIPHOUSE_STATE_DIR="$STATE_DIR"
export PYTHONUNBUFFERED=1

echo "ClipHouse HA app: постоянные данные и файлы находятся в ${STATE_DIR}"
echo "Открой в домашней сети: http://<IP-Home-Assistant>:8787/"
exec python3 /app/ClipHouse.py