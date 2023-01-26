#include "compressed_vector_type.hpp"

namespace hyrise {

std::ostream& operator<<(std::ostream& stream, const CompressedVectorType compressed_vector_type) {
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedWidthInteger4Byte: {
      stream << "FixedWidthInteger4Byte";
      break;
    }
    case CompressedVectorType::FixedWidthInteger2Byte: {
      stream << "FixedWidthInteger2Byte";
      break;
    }
    case CompressedVectorType::FixedWidthInteger1Byte: {
      stream << "FixedWidthInteger1Byte";
      break;
    }
    case CompressedVectorType::BitPacking: {
      stream << "BitPacking";
      break;
    }
    default:
      break;
  }
  return stream;
}

}  // namespace hyrise
