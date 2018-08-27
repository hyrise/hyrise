#include "proxy_chunk.hpp"

#include "chunk.hpp"

// Hyrise only supports x86-64 CPUs, therefore relying on RDTSC is fine.
// Although RDTSCP would provide more accurate cycles counts, the precision
// is not required for the chunk access tracking.
uint64_t rdtsc() {
#ifdef __s390__
  uint64_t tsc;
  __asm__ __volatile__("stckf %0" : "=Q"(tsc) : : "cc");
  return tsc;
#else
  unsigned int lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return (static_cast<uint64_t>(hi) << 32u) | lo;
#endif
}

namespace opossum {

ProxyChunk::ProxyChunk(const std::shared_ptr<Chunk>& chunk) : _chunk(chunk), _begin_rdtsc(rdtsc()) {}

ProxyChunk::~ProxyChunk() {
  if (const auto& access_counter = _chunk->access_counter()) {
    access_counter->increment(rdtsc() - _begin_rdtsc);
  }
}

}  // namespace opossum
