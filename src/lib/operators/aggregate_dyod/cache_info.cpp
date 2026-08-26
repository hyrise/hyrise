#include "cache_info.hpp"

#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <system_error>

namespace hyrise {

size_t parse_cpu_list_count(const std::string_view cpu_list) {
  const auto* position = cpu_list.data();
  const auto* const end = position + cpu_list.size();
  auto count = size_t{0};
  while (position != end) {
    auto first = uint64_t{0};
    auto parsed = std::from_chars(position, end, first);
    if (parsed.ec != std::errc{}) {
      return 0;
    }
    position = parsed.ptr;
    auto last = first;
    if (position != end && *position == '-') {
      parsed = std::from_chars(position + 1, end, last);
      if (parsed.ec != std::errc{} || last < first) {
        return 0;
      }
      position = parsed.ptr;
    }
    count += last - first + 1;
    if (position == end) {
      break;
    }
    if (*position != ',' || position + 1 == end) {
      return 0;
    }
    ++position;
  }
  return count;
}

size_t sysfs_llc_sharing_cpu_count() {
  auto list_file = std::ifstream{"/sys/devices/system/cpu/cpu0/cache/index3/shared_cpu_list"};
  auto cpu_list = std::string{};
  std::getline(list_file, cpu_list);
  return parse_cpu_list_count(cpu_list);
}

CacheSizes sanitize_cache_sizes(const int64_t l1d_bytes, const int64_t l2_bytes, const int64_t l3_bytes,
                                const size_t llc_sharing_cpus) {
  const auto sanitize_level = [](const int64_t reported_bytes, const size_t fallback_bytes, const size_t minimum_bytes,
                                 const size_t maximum_bytes) {
    if (reported_bytes <= 0) {
      return fallback_bytes;
    }
    return std::clamp(static_cast<size_t>(reported_bytes), minimum_bytes, maximum_bytes);
  };

  auto sizes = CacheSizes{};
  sizes.l1d_bytes = sanitize_level(l1d_bytes, FALLBACK_CACHE_SIZES.l1d_bytes, size_t{4} * 1024, size_t{1024} * 1024);
  sizes.l2_bytes = sanitize_level(l2_bytes, FALLBACK_CACHE_SIZES.l2_bytes, size_t{64} * 1024, size_t{64} * 1024 * 1024);
  sizes.l3_bytes =
      sanitize_level(l3_bytes, FALLBACK_CACHE_SIZES.l3_bytes, size_t{1024} * 1024, size_t{1024} * 1024 * 1024);
  sizes.l2_bytes = std::max(sizes.l2_bytes, sizes.l1d_bytes);
  sizes.l3_bytes = std::max(sizes.l3_bytes, sizes.l2_bytes);
  sizes.llc_sharing_cpus =
      llc_sharing_cpus == 0 ? FALLBACK_CACHE_SIZES.llc_sharing_cpus : std::min(llc_sharing_cpus, size_t{1024});
  return sizes;
}

const CacheSizes& cache_sizes() {
#ifdef __linux__
  static const auto sizes = sanitize_cache_sizes(sysconf(_SC_LEVEL1_DCACHE_SIZE), sysconf(_SC_LEVEL2_CACHE_SIZE),
                                                 sysconf(_SC_LEVEL3_CACHE_SIZE), sysfs_llc_sharing_cpu_count());
#else
  static const auto sizes = FALLBACK_CACHE_SIZES;
#endif
  return sizes;
}

}  // namespace hyrise
