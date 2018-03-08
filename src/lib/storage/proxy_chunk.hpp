#pragma once

#include <algorithm>
#include <atomic>
#include <memory>

#include "chunk.hpp"

namespace opossum {

// The ProxyChunk class wraps chunk objects and implements the RAII pattern
// to track the time a particular chunk has been in scope. These times are
// measured using the RDTSC instructions and are stored in the Chunk's
// ChunkAccessCounter.
class ProxyChunk {
 public:
  explicit ProxyChunk(const std::shared_ptr<Chunk>& chunk);
  ~ProxyChunk();

  const std::shared_ptr<Chunk> operator*() const { return _chunk; }

  const std::shared_ptr<Chunk> operator->() const { return _chunk; }

  operator const std::shared_ptr<Chunk>&() const { return _chunk; }

  bool operator==(const ProxyChunk& rhs) const { return _chunk == rhs._chunk; }

 protected:
  const std::shared_ptr<Chunk> _chunk;
  const uint64_t _begin_rdtsc;
};

}  // namespace opossum
