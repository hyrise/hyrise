#include "proxy_chunk.hpp"

#include "chunk.hpp"

uint64_t rdtsc() {
  unsigned int lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return (static_cast<uint64_t>(hi) << 32) | lo;
}

namespace opossum {

ProxyChunkConst::ProxyChunkConst(const Chunk &chunk) : _chunk(chunk), begin_rdtsc(rdtsc()) {}

ProxyChunkConst::~ProxyChunkConst() {
  if (const auto access_counter = _chunk.access_counter()) {
    access_counter->increment(rdtsc() - begin_rdtsc);
  }
}

ProxyChunk::ProxyChunk(Chunk &chunk) : _chunk(chunk), begin_rdtsc(rdtsc()) {}
ProxyChunk::~ProxyChunk() {
  if (const auto access_counter = _chunk.access_counter()) {
    access_counter->increment(rdtsc() - begin_rdtsc);
  }
}

}  // namespace opossum
