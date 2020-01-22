#pragma once

#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class AbstractPosList {
 public:

  virtual ~AbstractPosList() = default;

  // Returns whether the single ChunkID has been given (not necessarily, if it has been met)
  virtual bool references_single_chunk() const = 0;

  // For chunks that share a common ChunkID, returns that ID.
  virtual ChunkID common_chunk_id() const = 0;

  // Capacity
  virtual bool empty() const = 0;
  virtual size_t size() const = 0;

  virtual bool operator==(const AbstractPosList& other) = 0;
};

}  // namespace opossum
