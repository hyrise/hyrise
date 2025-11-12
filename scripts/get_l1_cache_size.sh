#!/bin/bash

# Path to the L1 cache size file
L1_CACHE_FILE="/sys/devices/system/cpu/cpu0/cache/index1/size"
L1_CACHE_SIZE_KB=$(cat "$L1_CACHE_FILE" | sed 's/K//')
L1_CACHE_SIZE_BYTES=$((L1_CACHE_SIZE_KB * 1024))
   
# Output the result
echo "$L1_CACHE_SIZE_BYTES"
