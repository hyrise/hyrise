#pragma once

#include <map>
#include <sstream>
#include <string>

#include "storage/column_encoding_utils.hpp"

namespace opossum {

inline std::string to_string(EncodingType encoding_type) {
  static const auto string_for_type = std::map<EncodingType, std::string>{
    { EncodingType::Unencoded, "Unencoded" },
    { EncodingType::Dictionary, "Dictionary" },
    { EncodingType::DeprecatedDictionary, "Dictionary (Deprecated)" },
    { EncodingType::RunLength, "Run Length" },
    { EncodingType::FrameOfReference, "FOR" }};

  return string_for_type.at(encoding_type);
}

inline std::string to_string(VectorCompressionType type) {
  static const auto string_for_type = std::map<VectorCompressionType, std::string>{
    { VectorCompressionType::FixedSizeByteAligned, "Fixed-size byte-aligned" },
    { VectorCompressionType::SimdBp128, "SIMD-BP128" }};

  return string_for_type.at(type);
}

inline std::string to_string(const ColumnEncodingSpec& spec) {
  auto stream = std::stringstream{};
  stream << to_string(spec.encoding_type);
  if (spec.vector_compression_type) {
    stream << " (" << to_string(*spec.vector_compression_type) << ")";
  }
  return stream.str();
}

}  // namespace opossum
