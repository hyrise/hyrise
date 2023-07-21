#pragma once

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#include <numaif.h>
#endif
#ifdef __AVX512VL__
#include <x86intrin.h>
#endif
#include <sys/mman.h>
#include <unistd.h>
#include "utils/assert.hpp"

namespace hyrise {

inline std::byte* mmap_region(const size_t num_bytes) {
#ifdef __APPLE__
  const int flags = MAP_PRIVATE | MAP_ANON | MAP_NORESERVE;
#elif __linux__
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif
  const auto mapped_memory = static_cast<std::byte*>(mmap(NULL, num_bytes, PROT_READ | PROT_WRITE, flags, -1, 0));

  if (mapped_memory == MAP_FAILED) {
    const auto error = errno;
    Fail("Failed to map volatile pool region: " + strerror(error));
  }

  return mapped_memory;
}

inline void munmap_region(std::byte* region, const size_t num_bytes) {
  if (munmap(region, num_bytes) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(error));
  }
}

inline void explicit_move_pages(void* mem, size_t size, int node) {
#if HYRISE_NUMA_SUPPORT
  auto nodes = numa_allocate_nodemask();
  numa_bitmask_setbit(nodes, node);
  if (mbind(mem, size, MPOL_BIND, nodes ? nodes->maskp : NULL, nodes ? nodes->size + 1 : 0,
            MPOL_MF_MOVE | MPOL_MF_STRICT) != 0) {
    numa_bitmask_free(nodes);

    Fail("Move pages failed");
  }
  numa_bitmask_free(nodes);
#endif
}

inline void simulate_store(std::byte* ptr) {
#ifdef __AVX512VL__
  // using a non-temporal memory hint
  // _mm512_stream_si512(reinterpret_cast<__m512*>(ptr), _mm512_set1_epi8(0x1));
#endif
}

inline void simulate_cacheline_read(std::byte* ptr) {
#ifdef __AVX512VL__

  auto v = _mm512_load_si512(ptr);
  benchmark::DoNotOptimize(v);
#endif
}

inline void simulate_page_read(std::byte* ptr, int num_bytes) {
  for (int i = 0; i < num_bytes; i += 64) {
    simulate_cacheline_read(ptr + i);
  }
}
}  // namespace hyrise