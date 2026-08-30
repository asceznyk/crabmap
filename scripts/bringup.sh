#!/bin/bash

DIR="$(pwd)"

pkill -f nginx 2>/dev/null || true

PORT=4001 "$DIR/scripts/volume" "/tmp/volume1" &
PID1=$!
PORT=4002 "$DIR/scripts/volume" "/tmp/volume2" &
PID2=$!
PORT=4003 "$DIR/scripts/volume" "/tmp/volume3" &
PID3=$!
PORT=4004 "$DIR/scripts/volume" "/tmp/volume4" &
PID4=$!
PORT=4005 "$DIR/scripts/volume" "/tmp/volume5" &
PID5=$!

echo "Started:"
echo "4001 -> $PID1"
echo "4002 -> $PID2"
echo "4003 -> $PID3"
echo "4004 -> $PID4"
echo "4005 -> $PID5"

sleep 2

ps -p "$PID1,$PID2,$PID3,$PID4,$PID5" -o pid,cmd

