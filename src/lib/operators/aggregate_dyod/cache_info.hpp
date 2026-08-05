#pragma once

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace hyrise {

/**
 * Per-core data cache sizes of the executing machine (unit: bytes per level).
 *
 * l1d_bytes and l2_bytes are per-core; l3_bytes is the slice reported by the OS, which on multi-CCX processors (e.g.
 * AMD Rome) is the per-CCX slice rather than the socket total. The cache-dependent tuning values in
 * aggregate_dyod_config.hpp are derived from these sizes.
 *
 * @see cache_sizes() for the queried-and-sanitized instance, sanitize_cache_sizes() for the sanitization rules.
 */
struct CacheSizes {
  size_t l1d_bytes;
  size_t l2_bytes;
  size_t l3_bytes;
};

/**
 * Cache sizes assumed when the OS reports none: 32 KiB L1d, 512 KiB L2, 16 MiB L3.
 *
 * These are the sizes the tuning constants were originally hand-tuned against, so a machine without cache reporting
 * behaves exactly as before the sizes became hardware-derived.
 */
constexpr CacheSizes FALLBACK_CACHE_SIZES = {32 * 1024, 512 * 1024, 16 * 1024 * 1024};

/**
 * Turns raw per-level cache sizes into values safe to derive tuning parameters from.
 *
 * A level that is unknown (<= 0, as sysconf reports on incomplete kernels or in containers) falls back to
 * FALLBACK_CACHE_SIZES; a reported value is clamped to a plausible per-level range (L1d 4 KiB..1 MiB,
 * L2 64 KiB..64 MiB, L3 1 MiB..1 GiB) so a misreporting hypervisor cannot push a derived parameter to an extreme.
 * Levels are then ordered: L2 is raised to at least L1d, L3 to at least L2.
 *
 * @param l1d_bytes  Raw L1 data cache size, e.g. sysconf(_SC_LEVEL1_DCACHE_SIZE).
 * @param l2_bytes   Raw L2 cache size, e.g. sysconf(_SC_LEVEL2_CACHE_SIZE).
 * @param l3_bytes   Raw L3 cache size, e.g. sysconf(_SC_LEVEL3_CACHE_SIZE).
 * @return Sanitized sizes with l1d_bytes <= l2_bytes <= l3_bytes.
 */
inline CacheSizes sanitize_cache_sizes(const int64_t l1d_bytes, const int64_t l2_bytes, const int64_t l3_bytes) {
  const auto sanitize_level = [](const int64_t reported_bytes, const size_t fallback_bytes, const size_t minimum_bytes,
                                 const size_t maximum_bytes) {
    if (reported_bytes <= 0) {
      return fallback_bytes;
    }
    return std::clamp(static_cast<size_t>(reported_bytes), minimum_bytes, maximum_bytes);
  };

  auto sizes = CacheSizes{};
  sizes.l1d_bytes = sanitize_level(l1d_bytes, FALLBACK_CACHE_SIZES.l1d_bytes, 4 * 1024, 1024 * 1024);
  sizes.l2_bytes = sanitize_level(l2_bytes, FALLBACK_CACHE_SIZES.l2_bytes, 64 * 1024, 64 * 1024 * 1024);
  sizes.l3_bytes = sanitize_level(l3_bytes, FALLBACK_CACHE_SIZES.l3_bytes, 1024 * 1024, 1024 * 1024 * 1024);
  sizes.l2_bytes = std::max(sizes.l2_bytes, sizes.l1d_bytes);
  sizes.l3_bytes = std::max(sizes.l3_bytes, sizes.l2_bytes);
  return sizes;
}

/**
 * The executing machine's cache sizes, queried from the OS once and sanitized.
 *
 * Queried via sysconf on first use and cached for the process lifetime; always safe to derive from (see
 * sanitize_cache_sizes()). Lives here rather than in Hyrise's Topology only because the aggregate is the sole
 * consumer so far; Topology is the eventual home.
 *
 * @return The same sanitized CacheSizes instance on every call.
 */
inline const CacheSizes& cache_sizes() {
#ifdef __linux__
  static const auto sizes = sanitize_cache_sizes(sysconf(_SC_LEVEL1_DCACHE_SIZE), sysconf(_SC_LEVEL2_CACHE_SIZE),
                                                 sysconf(_SC_LEVEL3_CACHE_SIZE));
#else
  static const auto sizes = FALLBACK_CACHE_SIZES;
#endif
  return sizes;
}

}  // namespace hyrise
