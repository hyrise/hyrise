#pragma once

#include "types.hpp"

namespace opossum {

template <typename T>
class ExpressionPosListWriter {
 public:
  explicit ExpressionPosListWriter(const ChunkID chunk_id);

  void resize(const size_t size);

  void write(const ChunkOffset chunk_offset, const T& value, const bool is_null);

  PosList row_ids;
};

class ExpressionResultWriter {
 public:
};

}  // namespace opossum
