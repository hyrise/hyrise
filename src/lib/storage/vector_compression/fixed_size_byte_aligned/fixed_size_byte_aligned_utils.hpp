#pragma once

#include "storage/vector_compression/compressed_vector_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

inline bool is_fixed_size_byte_aligned(CompressedVectorType type) {
  switch (type) {
    case CompressedVectorType::FixedSize4ByteAligned:
    case CompressedVectorType::FixedSize2ByteAligned:
    case CompressedVectorType::FixedSize1ByteAligned:
      return true;
    default:
      return false;
  }
}

inline size_t byte_width_for_fixed_size_byte_aligned_type(CompressedVectorType type) {
  DebugAssert(is_fixed_size_byte_aligned(type), "Type must be one of the fixed-size byte-aligned types.");

  switch (type) {
    case CompressedVectorType::FixedSize4ByteAligned:
      return 4u;
    case CompressedVectorType::FixedSize2ByteAligned:
      return 2u;
    case CompressedVectorType::FixedSize1ByteAligned:
      return 1u;
    default:
      return 0u;
  }
}

}  // namespace opossum
