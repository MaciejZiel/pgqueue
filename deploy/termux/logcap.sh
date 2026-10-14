#!/data/data/com.termux/files/usr/bin/bash
# Appends stdin to a log file and keeps it bounded: when the file passes MAX_BYTES it is
# renamed to FILE.1 (FILE.1 to FILE.2, ...) and the oldest of KEEP old files is dropped.
# Disk use is therefore at most about (KEEP + 1) * MAX_BYTES.
#   start.sh 2>&1 | logcap.sh ~/.local/state/pgqueue-demo.log [MAX_BYTES=1048576] [KEEP=2]
set -u

FILE="${1:?usage: logcap.sh FILE [MAX_BYTES] [KEEP]}"
MAX_BYTES="${2:-1048576}"
KEEP="${3:-2}"
CHECK_EVERY=50

rotate() {
  i="$KEEP"
  rm -f "$FILE.$i"
  while [ "$i" -gt 1 ]; do
    if [ -f "$FILE.$((i - 1))" ]; then mv -f "$FILE.$((i - 1))" "$FILE.$i"; fi
    i=$((i - 1))
  done
  if [ "$KEEP" -gt 0 ]; then mv -f "$FILE" "$FILE.1"; else : >"$FILE"; fi
}

n=0
while IFS= read -r line || [ -n "$line" ]; do
  printf '%s\n' "$line" >>"$FILE"
  n=$((n + 1))
  if [ "$n" -ge "$CHECK_EVERY" ]; then
    n=0
    size=$(wc -c <"$FILE" 2>/dev/null || echo 0)
    if [ "$size" -gt "$MAX_BYTES" ]; then rotate; fi
  fi
done
exit 0
