#!/usr/bin/env bash
# node-cost.sh — measure live per-node cost of a RUNNING committee-sampling sim.
# Run on the server WHILE the sim is loaded. Paste whole output back.
#   bash node-cost.sh                 # auto-match procs named 'committee-sampling'
#   bash node-cost.sh mybinaryname    # override match pattern
set -u

PAT="${1:-committee-sampling}"

# pids of node procs (exclude this script + grep)
PIDS=$(pgrep -f "$PAT" | tr '\n' ' ')
N=$(printf '%s\n' $PIDS | grep -c . )

echo "=== node-cost report ==="
printf '%-22s %s\n' "date"        "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
printf '%-22s %s\n' "match pattern" "$PAT"
printf '%-22s %s\n' "node procs found" "$N"
printf '%-22s %s\n' "load avg" "$(cut -d' ' -f1-3 /proc/loadavg)"

if [ "$N" -eq 0 ]; then echo "no procs match '$PAT' — pass binary name as arg 1"; exit 1; fi

# RSS from /proc/PID/status (VmRSS, robust), %cpu from ps per pid
for p in $PIDS; do
  rss=$(awk '/^VmRSS:/{print $2}' /proc/"$p"/status 2>/dev/null)
  cpu=$(ps -o pcpu= -p "$p" 2>/dev/null | tr -d ' ')
  [ -n "$rss" ] && printf '%s %s\n' "$rss" "${cpu:-0}"
done | awk '
  { rss+=$1; cpu+=$2; c++
    if($1>maxr)maxr=$1; if(minr==0||$1<minr)minr=$1 }
  END{
    if(c==0){print "no readable VmRSS (perms?)"; exit}
    printf "\n--- RAM (RSS, incl shared text) ---\n"
    printf "%-22s %d\n","procs measured",c
    printf "%-22s %.1f GB\n","total RSS",rss/1048576
    printf "%-22s %.1f MB\n","mean RSS/node",rss/c/1024
    printf "%-22s %.1f MB\n","min RSS/node",minr/1024
    printf "%-22s %.1f MB\n","max RSS/node",maxr/1024
    printf "\n--- CPU ---\n"
    printf "%-22s %.1f %%\n","total cpu (of 1 core)",cpu
    printf "%-22s %.2f %%\n","mean cpu/node",cpu/c
  }'

# fd count per node (sample first matched pid; needs perms)
FIRST=$(printf '%s' "$PIDS" | awk '{print $1}')
FDS=$(ls /proc/"$FIRST"/fd 2>/dev/null | wc -l)
echo
echo "--- FDs ---"
printf '%-22s %s\n' "fds (sampled pid $FIRST)" "$FDS"

# total fds across all node procs
TOTFD=0
for p in $PIDS; do c=$(ls /proc/"$p"/fd 2>/dev/null | wc -l); TOTFD=$((TOTFD+c)); done
printf '%-22s %s\n' "total fds (all nodes)" "$TOTFD"

# parent harness fds (python launcher) — the 1024 risk
PPID_HARNESS=$(pgrep -f "run_simulations" | head -1)
if [ -n "${PPID_HARNESS:-}" ]; then
  HF=$(ls /proc/"$PPID_HARNESS"/fd 2>/dev/null | wc -l)
  printf '%-22s %s (pid %s, ulimit-n=%s)\n' "harness fds" "$HF" "$PPID_HARNESS" "$(ulimit -n)"
fi
echo "=== end ==="
