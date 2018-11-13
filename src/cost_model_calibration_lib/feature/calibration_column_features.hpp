#pragma once

#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

struct CalibrationColumnFeatures {
  // Assumes that all segments have the same encoding (only looking at the first chunk yet)
  std::string column_encoding = "undefined";
  // TODO(Sven): change feature extractor
  bool is_any_segment_reference_segment = false;
  std::string column_data_type = "undefined";
  size_t column_memory_usage_bytes = 0;
  // TODO(Sven): How to calculate from segment_distinct_value_count?
  size_t column_distinct_value_count = 0;

  static const std::vector<std::string> feature_names;
  static const std::vector<std::string> feature_names_with_prefix(const std::optional<std::string>& prefix);
  static const std::vector<AllTypeVariant> serialize(const std::optional<CalibrationColumnFeatures>& features);
};

inline const std::vector<std::string> CalibrationColumnFeatures::feature_names(
    {"segment_encoding", "is_segment_reference_segment", "segment_data_type", "segment_memory_usage_bytes",
     "segment_distinct_value_count"});

inline const std::vector<std::string> CalibrationColumnFeatures::feature_names_with_prefix(
    const std::optional<std::string>& prefix) {
  auto copied_feature_names = feature_names;

  if (prefix) {
    std::for_each(copied_feature_names.begin(), copied_feature_names.end(),
                  [prefix](auto& s) { s.insert(0, *prefix + "_"); });
  }
  return copied_feature_names;
}

inline const std::vector<AllTypeVariant> CalibrationColumnFeatures::serialize(
    const std::optional<CalibrationColumnFeatures>& features) {
  if (!features) {
    return {NullValue{}, NullValue{}, NullValue{}, NullValue{}, NullValue{}};
  }

  return {features->column_encoding, features->is_any_segment_reference_segment, features->column_data_type,
          static_cast<int32_t>(features->column_memory_usage_bytes),
          static_cast<int32_t>(features->column_distinct_value_count)};
}

}  // namespace opossum
