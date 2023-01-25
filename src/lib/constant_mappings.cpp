#include "constant_mappings.hpp"

#include <unordered_map>

#include <boost/bimap.hpp>
#include <boost/hana/fold.hpp>
#include <magic_enum.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/function_expression.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/make_bimap.hpp"

namespace hyrise {

std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function) {
  return stream << aggregate_function_to_string.left.at(aggregate_function);
}

std::ostream& operator<<(std::ostream& stream, const FunctionType function_type) {
  return stream << function_type_to_string.left.at(function_type);
}

std::ostream& operator<<(std::ostream& stream, const DataType data_type) {
  return stream << data_type_to_string.left.at(data_type);
}

std::ostream& operator<<(std::ostream& stream, const EncodingType encoding_type) {
  return stream << magic_enum::enum_name(encoding_type);
}

std::ostream& operator<<(std::ostream& stream, const VectorCompressionType vector_compression_type) {
  return stream << vector_compression_type_to_string.left.at(vector_compression_type);
}

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
