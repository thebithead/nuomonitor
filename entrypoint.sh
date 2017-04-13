#!/bin/sh
set -e

if [ "${1:0:1}" = '-' ]; then
    set -- python nuomonitor.py "$@"
elif [ "${1}" = "batch" ]; then
    shift 
    set -- python store-monitor.py "$@"
fi

if [ $# -eq 0 ]; then
    # connect to local broker using password in default.properties
    HOST="$(netstat -nr | awk '$1 ~ /0.0.0.0/ { print $2; }')"
    python nuomonitor.py -b ${HOST}
else
    exec "$@"
fi
