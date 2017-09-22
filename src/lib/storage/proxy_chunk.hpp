#pragma once

#include <algorithm>
#include <atomic>
#include <memory>

#include "chunk.hpp"

namespace opossum {

class ProxyChunkConst {
 public:
  explicit ProxyChunkConst(const Chunk &chunk);
  ~ProxyChunkConst();

  const Chunk &operator*() const { return _chunk; }

  const Chunk *operator->() const { return &_chunk; }

  operator const Chunk &() const { return _chunk; }

  bool operator==(const ProxyChunkConst &rhs) const { return &_chunk == &rhs._chunk; }

 protected:
  const Chunk &_chunk;
  const uint64_t begin_rdtsc;
};

class ProxyChunk {
 public:
  explicit ProxyChunk(Chunk &chunk);
  ~ProxyChunk();

  Chunk &operator*() { return _chunk; }

  Chunk *operator->() { return &_chunk; }

  operator Chunk &() { return _chunk; }

 protected:
  Chunk &_chunk;
  const uint64_t begin_rdtsc;
};

}  // namespace opossum
