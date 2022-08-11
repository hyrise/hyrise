#pragma once

#include "storage/vector_compression/compressed_vector_type.hpp"
#include "utils/assert.hpp"

namespace hyrise {

inline bool is_fixed_width_integer(CompressedVectorType type) {
  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
    case CompressedVectorType::FixedWidthInteger2Byte:
    case CompressedVectorType::FixedWidthInteger1Byte:
      return true;
    case CompressedVectorType::BitPacking:
      return false;
  }

  Fail("GCC thinks this is reachable");
}

inline size_t byte_width_for_fixed_width_integer_type(CompressedVectorType type) {
  DebugAssert(is_fixed_width_integer(type), "Type must be one of the Fixed-width integer types.");

  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      return 4u;
    case CompressedVectorType::FixedWidthInteger2Byte:
      return 2u;
    case CompressedVectorType::FixedWidthInteger1Byte:
      return 1u;
    case CompressedVectorType::BitPacking:
      return 0u;
  }

  Fail("GCC thinks this is reachable");
}

}  // namespace hyrise
