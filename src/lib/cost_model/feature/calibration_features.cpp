#include "calibration_features.hpp"

#include "constant_mappings.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> CalibrationFeatures::serialize() const {
  // clang-format off
  std::map<std::string, AllTypeVariant> features = {
    {"operator_type", operator_type_to_string.at(operator_type)},
    {"execution_time_ns", static_cast<int64_t>(execution_time_ns)},
    {"input_table_size_ratio", input_table_size_ratio},
    {"left_input_row_count", static_cast<int64_t>(left_input_row_count)},
    {"left_input_chunk_count", static_cast<int64_t>(left_input_chunk_count)},
    {"left_input_memory_usage_bytes",static_cast<int64_t>(left_input_memory_usage_bytes)},
    {"left_input_chunk_size", static_cast<int64_t>(left_input_chunk_size)},
    {"left_input_is_small_table", left_input_is_small_table},
    {"right_input_row_count", static_cast<int64_t>(right_input_row_count)},
    {"right_input_chunk_count", static_cast<int64_t>(right_input_chunk_count)},
    {"right_input_memory_usage_bytes", static_cast<int64_t>(right_input_memory_usage_bytes)},
    {"right_input_chunk_size", static_cast<int64_t>(right_input_chunk_size)},
    {"right_input_is_small_table", right_input_is_small_table},
    {"output_row_count", static_cast<int64_t>(output_row_count)},
    {"output_chunk_count", static_cast<int64_t>(output_chunk_count)},
    {"output_memory_usage_bytes", static_cast<int64_t>(output_memory_usage_bytes)},
    {"output_chunk_size", static_cast<int64_t>(output_chunk_size)},
    {"output_is_small_table", output_is_small_table},
    {"selectivity", selectivity},
    {"is_selectivity_below_50_percent", is_selectivity_below_50_percent},
    {"selectivity_distance_to_50_percent", selectivity_distance_to_50_percent},
    {"operator_description", operator_description},
  };
  // clang-format on

  // std::map::merge() not supported yet by Clang
  const auto serialized_constant_hardware_features = constant_hardware_features.serialize();
  const auto serialized_runtime_hardware_features = runtime_hardware_features.serialize();
  const auto serialized_aggregate_features = aggregate_features.serialize();
  const auto serialized_join_features = join_features.serialize();
  const auto serialized_projection_features = projection_features.serialize();
  const auto serialized_table_scan_features = table_scan_features.serialize();

  features.insert(serialized_constant_hardware_features.begin(), serialized_constant_hardware_features.end());
  features.insert(serialized_runtime_hardware_features.begin(), serialized_runtime_hardware_features.end());
  features.insert(serialized_aggregate_features.begin(), serialized_aggregate_features.end());
  features.insert(serialized_join_features.begin(), serialized_join_features.end());
  features.insert(serialized_projection_features.begin(), serialized_projection_features.end());
  features.insert(serialized_table_scan_features.begin(), serialized_table_scan_features.end());

  return features;
}

}  // namespace cost_model
}  // namespace opossum
