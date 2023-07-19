#pragma once

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#include <numaif.h>
#endif
#ifdef __AVX512VL__
#include <x86intrin.h>
#endif
namespace hyrise {
inline void explicit_move_pages(void* mem, size_t size, int node) {
#if HYRISE_NUMA_SUPPORT
  auto nodes = numa_allocate_nodemask();
  numa_bitmask_setbit(nodes, node);
  if (mbind(mem, size, MPOL_BIND, nodes ? nodes->maskp : NULL, nodes ? nodes->size + 1 : 0,
            MPOL_MF_MOVE | MPOL_MF_STRICT) != 0) {
    const auto error = errno;
    numa_bitmask_free(nodes);

    Fail("Move pages failed: " + strerror(error));
  }
  numa_bitmask_free(nodes);
#endif
}

inline void simulate_cacheline_read(std::byte* ptr) {
#ifdef __AVX512VL__
  benchmark::DoNotOptimize(_mm512_load_si512(ptr));
#endif
}

inline void simulate_page_read(std::byte* ptr, int num_bytes) {
  for (int i = 0; i < num_bytes; i += 64) {
    simulate_cacheline_read(ptr + (num_bytes * i));
  }
}
}  // namespace hyrise