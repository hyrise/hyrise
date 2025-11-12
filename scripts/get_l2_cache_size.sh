#!/bin/bash

# Path to the L2 cache size file
L2_CACHE_FILE="/sys/devices/system/cpu/cpu0/cache/index2/size"
L2_CACHE_SIZE_KB=$(cat "$L2_CACHE_FILE" | sed 's/K//')
L2_CACHE_SIZE_BYTES=$((L2_CACHE_SIZE_KB * 1024))
   
# Output the result
echo "$L2_CACHE_SIZE_BYTES"