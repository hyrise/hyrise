#pragma once

#include <optional>
#include <unordered_map>

#include "nlohmann/json.hpp"

#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

using DataTypeEncodingMapping = std::unordered_map<DataType, SegmentEncodingSpec>;

// Map<TABLE_NAME, Map<column_name, SegmentEncoding>>
using TableSegmentEncodingMapping =
    std::unordered_map<std::string, std::unordered_map<std::string, SegmentEncodingSpec>>;

// View EncodingConfig::description to see format of encoding JSON
class EncodingConfig {
 public:
  EncodingConfig();
  EncodingConfig(const SegmentEncodingSpec& default_encoding_spec, DataTypeEncodingMapping type_encoding_mapping,
                 TableSegmentEncodingMapping encoding_mapping);
  explicit EncodingConfig(const SegmentEncodingSpec& default_encoding_spec);

  static EncodingConfig unencoded();

  SegmentEncodingSpec default_encoding_spec;
  DataTypeEncodingMapping type_encoding_mapping;
  TableSegmentEncodingMapping custom_encoding_mapping;

  static SegmentEncodingSpec encoding_spec_from_strings(const std::string& encoding_str,
                                                        const std::string& compression_str);
  static EncodingType encoding_string_to_type(const std::string& encoding_str);
  static std::optional<VectorCompressionType> compression_string_to_type(const std::string& compression_str);

  nlohmann::json to_json() const;

  static const char* description;
};

}  // namespace opossum
