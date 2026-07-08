#!/bin/bash
#
# Kafka Broker Diagnostic Script
# Run on each broker during or after an incident
# Output: /var/log/kafka/diagnostic-<broker-name>-<timestamp>.txt
#

set -euo pipefail

BROKER_NAME=$(hostname)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="/var/log/kafka"
OUTFILE="${OUTDIR}/diagnostic-${BROKER_NAME}-${TIMESTAMP}.txt"

mkdir -p "$OUTDIR"
exec > >(tee "$OUTFILE") 2>&1

cat << 'HEADER'
================================================================================
  KAFKA BROKER DIAGNOSTIC REPORT
================================================================================
HEADER

echo "Broker: $(hostname)"
echo "Date:   $(date)"
echo "Uptime: $(uptime)"
echo

# ============================================================================
# 1. KAFKA PROCESS HEALTH
# ============================================================================
echo "=============================================================================="
echo "  1. KAFKA PROCESS HEALTH"
echo "=============================================================================="

KAFKA_PID=$(pgrep -f "kafka.Kafka" | head -1)
if [ -z "$KAFKA_PID" ]; then
    echo "ERROR: Kafka process not found!"
    exit 1
fi

echo "PID: $KAFKA_PID"
echo "Command: $(cat /proc/$KAFKA_PID/cmdline | tr '\0' ' ')"
echo
echo "--- Process Status ---"
ps -p $KAFKA_PID -o pid,ppid,%cpu,%mem,vsz,rss,stat,start,time,etime,cmd | head -2

echo
echo "--- File Descriptors ---"
echo "Open FDs: $(ls /proc/$KAFKA_PID/fd 2>/dev/null | wc -l)"
echo "FD limit: $(ulimit -n)"
echo "FD usage: $(echo "scale=1; $(ls /proc/$KAFKA_PID/fd 2>/dev/null | wc -l) * 100 / $(ulimit -n)" | bc)%"
echo
echo "FD Type Breakdown:"
ls -l /proc/$KAFKA_PID/fd 2>/dev/null | awk '{print $NF}' | sed 's/->.*//' | sort | uniq -c | sort -rn | head -10

echo
echo "--- Open Files Detail (socket connections) ---"
ls /proc/$KAFKA_PID/fd 2>/dev/null | while read fd; do
    readlink /proc/$KAFKA_PID/fd/$fd 2>/dev/null
done | grep socket | wc -l | xargs -I{} echo "Socket FDs: {}"

# ============================================================================
# 2. JVM / GC METRICS
# ============================================================================
echo
echo "=============================================================================="
echo "  2. JVM GC METRICS"
echo "=============================================================================="

echo "--- JVM Heap Settings ---"
jinfo $KAFKA_PID 2>/dev/null | grep -iE "maxheap|heapsize|gc" | head -10
echo

echo "--- GC Status (sampled 5x, 1s apart) ---"
jstat -gcutil $KAFKA_PID 1000 5 2>/dev/null || echo "jstat not available or PID issue"
echo
echo "   S0=S0,S1=S1,E=Eden,S=Survivor,O=Old,MC=Metaspace,PC=PC,YGCT=Young GC Time,FGCT=Full GC Time,GT=GCTime"

echo
echo "--- GC Time Summary ---"
GC_TIME=$(jstat -gc $KAFKA_PID 1000 1 2>/dev/null | awk 'NR==2{printf "Young GC: %.2fs, Full GC: %.2fs, Total: %.2fs\n", $6, $7, $8}')
echo "   $GC_TIME"

echo
echo "--- Recent Full GC Events (last 2 hours) ---"
find /var/log/kafka/ -name "gc.log*" -o -name "gcservice.log*" 2>/dev/null | while read logfile; do
    if [ -f "$logfile" ]; then
        echo "   File: $logfile"
        grep -i "full gc\|gc pause" "$logfile" 2>/dev/null | tail -10 | while read line; do
            echo "     $line"
        done
    fi
done

# ============================================================================
# 3. NETWORK CONNECTIONS
# ============================================================================
echo
echo "=============================================================================="
echo "  3. NETWORK CONNECTIONS"
echo "=============================================================================="

echo "--- Total Connections to Kafka Ports ---"
for port in 9093 9092 9091; do
    count=$(ss -tn 2>/dev/null | grep ":${port} " | wc -l)
    echo "   Port $port: $count connections"
done

echo
echo "--- Top 20 Connection Sources (9093) ---"
ss -tn 2>/dev/null | grep ":9093 " | awk '{print $5}' | sed 's/:[0-9]*$//' | sort | uniq -c | sort -rn | head -20 | awk '{printf "   %5d  %s\n", $1, $2}'

echo
echo "--- Connection State Summary (9093) ---"
ss -tn 2>/dev/null | grep ":9093 " | awk '{print $1}' | sort | uniq -c | sort -rn
echo "   (ESTAB=established, TIME-WAIT=waiting, CLOSE-WAIT=waiting on app, SYN-SENT=sending)"

echo
echo "--- Connection Count Over Time (10-second snapshots) ---"
echo "   Time                 Connections"
for i in 1 2 3 4 5; do
    sleep 2
    ts=$(date '+%H:%M:%S')
    cnt=$(ss -tn 2>/dev/null | grep ":9093 " | wc -l)
    echo "   $ts                $cnt"
done

echo
echo "--- Network Socket Limits ---"
echo "   somaxconn:    $(cat /proc/sys/net/core/somaxconn 2>/dev/null)"
echo "   tcp_max_syn_backlog: $(cat /proc/sys/net/ipv4/tcp_max_syn_backlog 2>/dev/null)"
echo "   tcp_tw_reuse:  $(cat /proc/sys/net/ipv4/tcp_tw_reuse 2>/dev/null)"
echo "   tcp_fin_timeout: $(cat /proc/sys/net/ipv4/tcp_fin_timeout 2>/dev/null)"
echo "   max user processes: $(ulimit -u)"

# ============================================================================
# 4. SYSTEM RESOURCES
# ============================================================================
echo
echo "=============================================================================="
echo "  4. SYSTEM RESOURCES"
echo "=============================================================================="

echo "--- CPU Usage (10-second sample) ---"
top -bn1 2>/dev/null | head -5 | awk '/^%Cpu|/^ CPU/' | sed 's/^/   /'

echo
echo "--- Per-CPU Usage (1-second sample) ---"
mpstat 1 3 2>/dev/null | tail -1 | awk '{for(i=12;i<=NF;i++) printf "%s ", $i; print ""}' | sed 's/^/   /'

echo
echo "--- Memory ---"
free -h | awk '/Mem:/{printf "   Total: %s, Used: %s, Free: %s, Buff/Cache: %s\n", $2,$3,$4,$6}'
free -h | awk '/^Swap:/{printf "   Swap: Total: %s, Used: %s, Free: %s\n", $2,$3,$4}'
echo
echo "--- Kafka Process Memory ---"
ps -p $KAFKA_PID -o pid,rss,vsz,ni,pri | awk 'NR==2{printf "   RSS: %dMB, VSZ: %dMB, NI: %d\n", $2/1024, $3/1024, $4}'

echo
echo "--- Disk I/O (5-second sample) ---"
iostat -x 1 3 2>/dev/null | grep -E "^[a-z]|Device|avg-cpu" | head -20 | sed 's/^/   /'

echo
echo "--- Kafka Data Disk Usage ---"
du -sh /var/kafka/data 2>/dev/null || du -sh /var/lib/kafka/data 2>/dev/null || echo "   Kafka data directory not found"
echo
echo "--- Top 20 Largest Topics/Partitions ---"
find /var/kafka/data /var/lib/kafka/data -name "partition" -type d 2>/dev/null -exec dirname {} \; | while read dir; do
    size=$(du -sh "$dir" 2>/dev/null | awk '{print $1}')
    echo "$size $dir"
done | sort -rh | head -20 | sed 's/^/   /'

echo
echo "--- Filesystem Status ---"
df -h /var/kafka/data /var/lib/kafka/data 2>/dev/null | tail -n +2 | sed 's/^/   /'

# ============================================================================
# 5. DISK I/O DETAIL
# ============================================================================
echo
echo "=============================================================================="
echo "  5. DISK I/O DETAIL"
echo "=============================================================================="

echo "--- Disk Latency (10 samples) ---"
iostat -dx 1 10 2>/dev/null | grep -E "^[a-z]|Device" | head -20 | sed 's/^/   /'

echo
echo "--- I/O Scheduler ---"
for dev in /sys/block/*/queue/scheduler; do
    devname=$(basename $(dirname $dev))
    echo "   $devname: $(cat $dev 2>/dev/null)"
done

echo
echo "--- Kafka Log Segments (sample of active partitions) ---"
find /var/kafka/data /var/lib/kafka/data -name "*.log" -type f 2>/dev/null | head -20 | while read logfile; do
    size=$(du -h "$logfile" | awk '{print $1}')
    mod=$(stat -c %y "$logfile" 2>/dev/null | cut -d. -f1)
    echo "   $size  $mod  $logfile"
done | sort | head -20

# ============================================================================
# 6. KAFKA NETWORK THREADS
# ============================================================================
echo
echo "=============================================================================="
echo "  6. KAFKA NETWORK THREADS"
echo "=============================================================================="

echo "--- Network Thread Stack Traces (snapshot) ---"
echo "   (Capturing 3 network threads...)"
jstack $KAFKA_PID 2>/dev/null | grep -A 15 "KafkaServer" | head -60 | sed 's/^/   /'

echo
echo "--- Thread Summary ---"
echo "   Total threads: $(jstack $KAFKA_PID 2>/dev/null | grep "java.lang.Thread" | wc -l)"
echo "   RUNNABLE:   $(jstack $KAFKA_PID 2>/dev/null | grep -c "RUNNABLE")"
echo "   WAITING:    $(jstack $KAFKA_PID 2>/dev/null | grep -c "WAITING")"
echo "   TIMED_WAIT: $(jstack $KAFKA_PID 2>/dev/null | grep -c "TIMED_WAITING")"
echo "   BLOCKED:    $(jstack $KAFKA_PID 2>/dev/null | grep -c "BLOCKED")"

echo
echo "--- Blocked Threads (if any) ---"
jstack $KAFKA_PID 2>/dev/null | grep -B 3 "BLOCKED" | head -30 | sed 's/^/   /'

# ============================================================================
# 7. KAFKA METRICS (JMX)
# ============================================================================
echo
echo "=============================================================================="
echo "  7. KAFKA METRICS (via jcmd)"
echo "=============================================================================="

echo "--- Request Metrics (via jcmd if available) ---"
jcmd $KAFKA_PID JMX.help kafka.server:type=RequestMetrics 2>/dev/null | head -50 | sed 's/^/   /' || echo "   jcmd JMX not available, trying jmxremote..."

echo
echo "--- Fetch Request Rate (approximation) ---"
# Use jmxremote as fallback
if command -v jmxremote &>/dev/null; then
    jmxremote 2>/dev/null | grep -i "fetch" | head -10 | sed 's/^/   /'
else
    echo "   jmxremote not configured - check JMX settings in jvm-server.properties"
fi

# ============================================================================
# 8. KAFKA BROKER CONFIG
# ============================================================================
echo
echo "=============================================================================="
echo "  8. KAFKA BROKER CONFIG (key settings)"
echo "=============================================================================="

KAFKA_CONF=$(find /opt /usr -name "server.properties" -path "*kafka*" 2>/dev/null | head -1)
if [ -z "$KAFKA_CONF" ]; then
    KAFKA_CONF="/etc/kafka/server.properties"
fi

if [ -f "$KAFKA_CONF" ]; then
    echo "Config file: $KAFKA_CONF"
    echo
    for key in num.network.threads num.io.threads socket.receive.buffer.bytes socket.send.buffer.bytes \
               socket.listen.backlog max.connections.per.ip num.partitions default.replication.factor \
               log.retention.hours log.segment.bytes log.dirs replica.socket.timeout.ms \
               replica.fetch.max.bytes request.timeout.ms connections.max.reauthAttempts; do
        val=$(grep "^${key}" "$KAFKA_CONF" 2>/dev/null | head -1 | cut -d= -f2 | xargs)
        if [ -n "$val" ]; then
            echo "   $key = $val"
        fi
    done
else
    echo "   Config file not found"
fi

# ============================================================================
# 9. NETWORK ERROR STATISTICS
# ============================================================================
echo
echo "=============================================================================="
echo "  9. NETWORK ERROR STATISTICS"
echo "=============================================================================="

echo "--- Network Interface Errors ---"
for iface in $(ip -o link show 2>/dev/null | awk -F': ' '{print $2}' | awk '{print $1}'); do
    rx_err=$(cat /proc/net/dev 2>/dev/null | grep "$iface" | awk '{print $3}')
    tx_err=$(cat /proc/net/dev 2>/dev/null | grep "$iface" | awk '{print $11}')
    rx_drop=$(cat /proc/net/dev 2>/dev/null | grep "$iface" | awk '{print $4}')
    tx_drop=$(cat /proc/net/dev 2>/dev/null | grep "$iface" | awk '{print $12}')
    echo "   $iface: RX errors=$rx_err, TX errors=$tx_err, RX drops=$rx_drop, TX drops=$tx_drop"
done

echo
echo "--- TCP Errors (from /proc/net/snmp) ---"
cat /proc/net/snmp 2>/dev/null | grep -E "^Tcp:" | awk '{printf "   InErrs=%s, OutRsts=%s, RetransSegs=%s, InTooBig=%s, InCsumErrors=%s\n", $7, $16, $17, $9, $8}'

echo
echo "--- Kernel Network Messages (last 50 lines) ---"
dmesg 2>/dev/null | grep -iE "net|tcp|dropped|buffer|out of memory|oom" | tail -50 | sed 's/^/   /'

# ============================================================================
# 10. KAFKA LOG ERRORS
# ============================================================================
echo
echo "=============================================================================="
echo "  10. KAFKA SERVER LOG ERRORS (last 200 lines)"
echo "=============================================================================="

KAFKA_LOG=$(find /var/log/kafka /var/log/kafka.log /opt/kafka/logs -name "server.log" 2>/dev/null | head -1)
if [ -n "$KAFKA_LOG" ]; then
    echo "Log file: $KAFKA_LOG"
    echo
    echo "--- Recent Errors ---"
    grep -iE "error|exception|warn" "$KAFKA_LOG" 2>/dev/null | tail -50 | sed 's/^/   /'
    echo
    echo "--- Timeout / Refused Messages ---"
    grep -iE "timeout|refused|timed out|too many" "$KAFKA_LOG" 2>/dev/null | tail -30 | sed 's/^/   /'
    echo
    echo "--- Connection Accept Errors ---"
    grep -iE "accept.*error|failed.*accept|bind" "$KAFKA_LOG" 2>/dev/null | tail -20 | sed 's/^/   /'
else
    echo "   Kafka log file not found"
fi

echo
echo "--- Recent GC Pauses in Server Log ---"
grep -iE "full gc|gc pause|gc overhead" "$KAFKA_LOG" 2>/dev/null | tail -20 | sed 's/^/   /' || echo "   No GC events in server log"

# ============================================================================
# 11. UNDER-REPLICATED PARTITIONS & ISR
# ============================================================================
echo
echo "=============================================================================="
echo "  11. UNDER-REPLICATED PARTITIONS & ISR"
echo "=============================================================================="

KAFKA_CLI=$(find /opt /usr -name "kafka-topics.sh" -path "*kafka*" 2>/dev/null | head -1)
KAFKA_BROKER_PORT=9093

if [ -n "$KAFKA_CLI" ] && [ -x "$KAFKA_CLI" ]; then
    echo "--- Under-Replicated Partitions ---"
    $KAFKA_CLI --bootstrap-server localhost:$KAFKA_BROKER_PORT --describe --under-replicated-partitions 2>/dev/null | head -30 | sed 's/^/   /' || echo "   Unable to query"

    echo
    echo "--- Unavailable Partitions ---"
    $KAFKA_CLI --bootstrap-server localhost:$KAFKA_BROKER_PORT --describe --unavailable-partitions 2>/dev/null | head -30 | sed 's/^/   /' || echo "   Unable to query"
else
    echo "   kafka-topics.sh not found or not executable"
fi

# ============================================================================
# 12. SUMMARY
# ============================================================================
echo
echo "=============================================================================="
echo "  12. SUMMARY"
echo "=============================================================================="

echo "Report generated: $(date)"
echo "Broker: $(hostname)"
echo "Kafka PID: $KAFKA_PID"
echo "Report file: $OUTFILE"
echo
echo "Check the following for issues:"
echo "  - High %iowait (>50%) = disk bottleneck"
echo "  - FD usage >80% = file descriptor exhaustion"
echo "  - Full GC time >5% = GC pressure"
echo "  - Connections >1000 from single IP = connection flood"
echo "  - Network errors >0 = network fabric issues"
echo "  - Under-replicated partitions >0 = broker/partition issues"
echo
echo "================================================================================
  END OF DIAGNOSTIC REPORT
================================================================================"
