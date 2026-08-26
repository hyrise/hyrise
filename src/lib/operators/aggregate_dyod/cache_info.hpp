#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace hyrise {

/**
 * Per-core data cache sizes of the executing machine.
 *
 * l1d_bytes and l2_bytes are per-core; l3_bytes is the slice reported by the OS, which on multi-CCX processors
 * is the per-CCX slice rather than the socket total. llc_sharing_cpus counts the CPUs competing for that
 * slice. The cache-dependent constants in aggregate_dyod_config.hpp are derived from these values.
 */
struct CacheSizes {
  size_t l1d_bytes;
  size_t l2_bytes;
  size_t l3_bytes;
  size_t llc_sharing_cpus;
};

/**
 * Values assumed when the OS reports none: 32 KiB L1d, 512 KiB L2, 16 MiB L3 shared by 4 CPUs.
 */
constexpr CacheSizes FALLBACK_CACHE_SIZES = {.l1d_bytes = size_t{32} * 1024,
                                             .l2_bytes = size_t{512} * 1024,
                                             .l3_bytes = size_t{16} * 1024 * 1024,
                                             .llc_sharing_cpus = 4};

/**
 * Number of CPUs named by a sysfs cpu list such as "0-3" or "0-3,64-67".
 */
size_t parse_cpu_list_count(std::string_view cpu_list);

/**
 * CPUs sharing cpu0's last-level cache, from /sys/devices/system/cpu/cpu0/cache/index3/shared_cpu_list.
 */
size_t sysfs_llc_sharing_cpu_count();

/**
 * Turns raw cache values into ones safe to derive tuning parameters from.
 */
CacheSizes sanitize_cache_sizes(int64_t l1d_bytes, int64_t l2_bytes, int64_t l3_bytes, size_t llc_sharing_cpus);

/**
 * The executing machine's cache values, queried from the OS once and sanitized.
 */
const CacheSizes& cache_sizes();

}  // namespace hyrise
