#include "proxy_chunk.hpp"

#include "chunk.hpp"

// TODO(normanrz): rdtsc is fine. Why not rdtscp?
uint64_t rdtsc() {
  unsigned int lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

namespace opossum {

ProxyChunk::ProxyChunk(const Chunk& chunk) : _chunk(chunk), _begin_rdtsc(rdtsc()) {}

ProxyChunk::~ProxyChunk() {
  if (const auto& access_counter = _chunk.access_counter()) {
    access_counter->increment(rdtsc() - _begin_rdtsc);
  }
}

}  // namespace opossum
