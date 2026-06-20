#!/usr/bin/env bash
# server-capacity.sh — dump host stats needed to size committee-sampling node count.
# No deps. Run on the remote server, paste the whole output back.
#   bash server-capacity.sh
set -u

line() { printf '%s\n' "------------------------------------------------------------"; }
kv()   { printf '%-26s %s\n' "$1" "$2"; }

echo "=== committee-sampling server capacity report ==="
kv "date" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
kv "hostname" "$(hostname 2>/dev/null)"
kv "uname" "$(uname -srm 2>/dev/null)"

line; echo "[ OS / DISTRO ]"
if [ -r /etc/os-release ]; then . /etc/os-release; kv "distro" "${PRETTY_NAME:-unknown}"; fi

line; echo "[ CPU ]"
if command -v nproc >/dev/null 2>&1; then kv "logical cores (nproc)" "$(nproc)"; fi
if [ -r /proc/cpuinfo ]; then
  kv "model" "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
  kv "physical cores" "$(grep -c ^processor /proc/cpuinfo)"
fi
kv "load avg (1/5/15m)" "$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"

line; echo "[ MEMORY ]"
if [ -r /proc/meminfo ]; then
  awk '/^MemTotal:/{printf "%-26s %.1f GB\n","total",$2/1048576}
       /^MemAvailable:/{printf "%-26s %.1f GB\n","available",$2/1048576}
       /^SwapTotal:/{printf "%-26s %.1f GB\n","swap total",$2/1048576}' /proc/meminfo
fi

line; echo "[ LIMITS (current shell) ]"
kv "ulimit -n (open files)" "$(ulimit -n)"
kv "ulimit -u (max procs)"  "$(ulimit -u)"

line; echo "[ KERNEL CEILINGS ]"
[ -r /proc/sys/fs/file-max ]      && kv "fs.file-max"          "$(cat /proc/sys/fs/file-max)"
[ -r /proc/sys/fs/file-nr ]       && kv "fs.file-nr (used)"    "$(awk '{print $1}' /proc/sys/fs/file-nr)"
[ -r /proc/sys/kernel/pid_max ]   && kv "kernel.pid_max"       "$(cat /proc/sys/kernel/pid_max)"
[ -r /proc/sys/kernel/threads-max ] && kv "kernel.threads-max" "$(cat /proc/sys/kernel/threads-max)"
if command -v sysctl >/dev/null 2>&1; then
  kv "net.ipv4 port range" "$(sysctl -n net.ipv4.ip_local_port_range 2>/dev/null)"
fi

line; echo "[ DISK (cwd filesystem) ]"
df -h . 2>/dev/null | sed -n '1,2p'

line; echo "[ CURRENT PROC COUNT ]"
kv "running processes" "$(ps -e 2>/dev/null | wc -l)"

line; echo "=== end report ==="
