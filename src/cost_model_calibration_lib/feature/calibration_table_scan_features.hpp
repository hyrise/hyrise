#pragma once

#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

struct CalibrationTableScanFeatures {
  EncodingType scan_segment_encoding = EncodingType::Unencoded;
  bool is_scan_segment_reference_segment = false;
  DataType scan_segment_data_type = DataType::Int;  // Just any default
  size_t scan_segment_memory_usage_bytes = 0;
  size_t scan_segment_distinct_value_count = 0;
  bool uses_second_segment = false;
  EncodingType second_scan_segment_encoding = EncodingType::Unencoded;
  bool is_second_scan_segment_reference_segment = false;
  DataType second_scan_segment_data_type = DataType::Int;  // Just any default
  size_t second_scan_segment_memory_usage_bytes = 0;
  size_t second_scan_segment_distinct_value_count = 0;
  PredicateCondition scan_operator_type = PredicateCondition::Equals;
  std::string scan_operator_description = "";

  static const std::vector<std::string> columns;
};

inline const std::vector<std::string> CalibrationTableScanFeatures::columns(
    {"scan_segment_encoding", "is_scan_segment_reference_segmen", "scan_segment_data_type",
     "scan_segment_memory_usage_bytes", "scan_segment_distinct_value_count", "uses_second_segment",
     "second_scan_segment_encoding", "is_second_scan_segment_reference_segment", "second_scan_segment_data_type",
     "second_scan_segment_memory_usage_bytes", "second_scan_segment_distinct_value_count", "scan_operator_type",
     "scan_operator_description"});

inline std::vector<AllTypeVariant> serialize(const std::optional<CalibrationTableScanFeatures>& features) {
  if (!features) {
    return {NullValue{}, NullValue{}, NullValue{}, NullValue{}, NullValue{}, NullValue{}, NullValue{},
            NullValue{}, NullValue{}, NullValue{}, NullValue{}, NullValue{}, NullValue{}};
  }
  return {static_cast<int32_t>(features->scan_segment_encoding),
          features->is_scan_segment_reference_segment,
          static_cast<int32_t>(features->scan_segment_data_type),
          static_cast<int32_t>(features->scan_segment_memory_usage_bytes),
          static_cast<int32_t>(features->scan_segment_distinct_value_count),
          features->uses_second_segment,
          static_cast<int32_t>(features->second_scan_segment_encoding),
          features->is_second_scan_segment_reference_segment,
          static_cast<int32_t>(features->second_scan_segment_data_type),
          static_cast<int32_t>(features->second_scan_segment_memory_usage_bytes),
          static_cast<int32_t>(features->second_scan_segment_distinct_value_count),
          static_cast<int32_t>(features->scan_operator_type),
          features->scan_operator_description};
}

}  // namespace opossum
