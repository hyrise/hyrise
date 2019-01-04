#include "calibration_features.hpp"

#include "constant_mappings.hpp"

namespace opossum {
namespace cost_model {

const std::map<std::string, AllTypeVariant> CalibrationFeatures::serialize() const {
  // clang-format off
  std::map<std::string, AllTypeVariant> features = {
    {"operator_type", operator_type_to_string.at(operator_type)},
    {"execution_time_ns", execution_time_ns},
    {"input_table_size_ratio", input_table_size_ratio},
    {"left_input_row_count", left_input_row_count},
    {"left_input_chunk_count", left_input_chunk_count},
    {"left_input_memory_usage_bytes", left_input_memory_usage_bytes},
    {"left_input_chunk_size", left_input_chunk_size},
    {"left_input_is_small_table", left_input_is_small_table},
    {"right_input_row_count", right_input_row_count},
    {"right_input_chunk_count", right_input_chunk_count},
    {"right_input_memory_usage_bytes", right_input_memory_usage_bytes},
    {"right_input_chunk_size", right_input_chunk_size},
    {"right_input_is_small_table", right_input_is_small_table},
    {"output_row_count", output_row_count},
    {"output_chunk_count", output_chunk_count},
    {"output_memory_usage_bytes", output_memory_usage_bytes},
    {"output_chunk_size", output_chunk_size},
    {"output_is_small_table", output_is_small_table},
    {"selectivity", selectivity},
    {"is_selectivity_below_50_percent", is_selectivity_below_50_percent},
    {"selectivity_distance_to_50_percent", selectivity_distance_to_50_percent},
    {"operator_description", operator_description},
  };
  // clang-format on

  features.merge(hardware_features.serialize());
  features.merge(runtime_features.serialize());
  features.merge(aggregate_features.serialize());
  features.merge(join_features.serialize());
  features.merge(projection_features.serialize());
  features.merge(table_scan_features.serialize());

  return features;
}

}  // namespace cost_model
}  // namespace opossum
