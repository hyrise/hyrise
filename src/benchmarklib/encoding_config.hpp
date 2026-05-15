#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include "nlohmann/json_fwd.hpp"

#include "storage/encoding_type.hpp"

namespace hyrise {

using DataTypeEncodingMapping = std::unordered_map<DataType, SegmentEncodingSpec>;

// Map<TABLE_NAME, Map<column_name, SegmentEncoding>>
using TableSegmentEncodingMapping =
    std::unordered_map<std::string, std::unordered_map<std::string, SegmentEncodingSpec>>;

// View EncodingConfig::description to see format of encoding JSON
class EncodingConfig {
 public:
  explicit EncodingConfig(const std::optional<SegmentEncodingSpec>& init_preferred_encoding_spec = {},
                          DataTypeEncodingMapping init_type_encoding_mapping = {},
                          TableSegmentEncodingMapping init_encoding_mapping = {});

  static EncodingConfig unencoded();

  std::optional<SegmentEncodingSpec> preferred_encoding_spec;
  DataTypeEncodingMapping type_encoding_mapping;
  TableSegmentEncodingMapping custom_encoding_mapping;

  static std::optional<SegmentEncodingSpec> encoding_spec_from_strings(const std::string& encoding_str,
                                                                       const std::string& compression_str);
  static std::optional<EncodingType> encoding_string_to_type(const std::string& encoding_str);
  static std::optional<VectorCompressionType> compression_string_to_type(const std::string& compression_str);

  nlohmann::json to_json() const;

  static const char* const description;
};

}  // namespace hyrise
