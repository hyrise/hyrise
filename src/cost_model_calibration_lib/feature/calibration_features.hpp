#pragma once

#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct CalibrationFeatures {
  std::string operator_type;
  uint64_t execution_time_ns = 0;
  float input_table_size_ratio = 0.0;

  size_t left_input_row_count = 0;
  size_t left_input_chunk_count = 0;
  size_t left_input_memory_usage_bytes = 0;
  size_t left_input_chunk_size = 0;

  size_t right_input_row_count = 0;
  size_t right_input_chunk_count = 0;
  size_t right_input_memory_usage_bytes = 0;
  size_t right_input_chunk_size = 0;

  size_t output_row_count = 0;
  size_t output_chunk_count = 0;
  size_t output_memory_usage_bytes = 0;
  size_t output_chunk_size = 0;
  float output_selectivity = 0.0;

  std::string operator_description;

  static const std::vector<std::string> feature_names;
  static const std::vector<AllTypeVariant> serialize(const CalibrationFeatures& features);
};

inline const std::vector<std::string> CalibrationFeatures::feature_names(
    {"operator_type", "input_table_size_ratio", "left_input_row_count", "left_input_chunk_count",
     "left_input_memory_usage_bytes", "left_input_chunk_size", "right_input_row_count", "right_input_chunk_count",
     "right_input_memory_usage_bytes", "right_input_chunk_size", "output_row_count", "output_chunk_count",
     "output_memory_usage_bytes", "output_chunk_size", "output_selectivity", "operator_description",
     "execution_time_ns"});

inline const std::vector<AllTypeVariant> CalibrationFeatures::serialize(const CalibrationFeatures& features) {
  return {features.operator_type,
          features.input_table_size_ratio,
          static_cast<int32_t>(features.left_input_row_count),
          static_cast<int32_t>(features.left_input_chunk_count),
          static_cast<int32_t>(features.left_input_memory_usage_bytes),
          static_cast<int32_t>(features.left_input_chunk_size),
          static_cast<int32_t>(features.right_input_row_count),
          static_cast<int32_t>(features.right_input_chunk_count),
          static_cast<int32_t>(features.right_input_memory_usage_bytes),
          static_cast<int32_t>(features.right_input_chunk_size),
          static_cast<int32_t>(features.output_row_count),
          static_cast<int32_t>(features.output_chunk_count),
          static_cast<int32_t>(features.output_memory_usage_bytes),
          static_cast<int32_t>(features.output_chunk_size),
          features.output_selectivity,
          features.operator_description,
          static_cast<int64_t>(features.execution_time_ns)};
}

}  // namespace opossum
