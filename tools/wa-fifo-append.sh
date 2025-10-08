# /home/kidders/apps/wa-hub/wa-fifo-append
#!/usr/bin/env bash
set -euo pipefail

CFG="${1:-/home/kidders/apps/wa-hub/config/wa-hub.json}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required on the server" >&2
  exit 127
fi

# Resolve FIFO path from config: prefer fifo_path, else base_dir/fifo_name
fifo="$(jq -r '
  if (.fifo_path // "") != "" then .fifo_path
  else ((.base_dir // "") + "/" + (.fifo_name // "send.fifo")) end
' "$CFG")"

if [[ -z "$fifo" || "$fifo" == "null" ]]; then
  echo "FIFO path not found in $CFG" >&2
  exit 2
fi
if [[ ! -p "$fifo" ]]; then
  echo "FIFO not found: $fifo" >&2
  exit 3
fi

# Read stdin to a temp file, ensure newline at end, then write to FIFO
tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
cat - > "$tmp"
if [[ ! -s "$tmp" ]]; then exit 0; fi
# Append newline if last byte != '\n' (10)
if ! [[ "$(tail -c1 "$tmp" | od -An -t u1 | tr -d ' ')" == "10" ]]; then
  echo >> "$tmp"
fi
cat "$tmp" > "$fifo"
