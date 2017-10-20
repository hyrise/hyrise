#pragma once

#include <algorithm>
#include <atomic>
#include <memory>

#include "chunk.hpp"

namespace opossum {

// TODO(normanrz): Class comment what this is good for
class ProxyChunk {
 public:
  explicit ProxyChunk(const Chunk& chunk);
  ~ProxyChunk();

  const Chunk& operator*() const { return _chunk; }

  const Chunk* operator->() const { return &_chunk; }

  operator const Chunk&() const { return _chunk; }

  bool operator==(const ProxyChunk& rhs) const { return &_chunk == &rhs._chunk; }

 protected:
  const Chunk& _chunk;
  const uint64_t _begin_rdtsc;
};

}  // namespace opossum
